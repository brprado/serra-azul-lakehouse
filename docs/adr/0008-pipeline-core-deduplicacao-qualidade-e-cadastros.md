# ADR-0008: Construir o pipeline core com bronze fiel, deduplicação por AUTO CDC na silver, cadastros recalculados da última extração e qualidade por expectations

- **Status:** aceito — emendado pelo [ADR-0009](0009-validacao-de-documento-digitos-repetidos-e-cnpj-alfanumerico.md) (validação do documento)
- **Data:** 2026-10-20
- **Fatia:** fatia 1 — Comercial
- **Decisor:** Engenheiro de Dados Sênior (Claude) · **Acompanhamento:** usuário (pode vetar)
- **Relação:** implementa o ADR-0002 (SDP), o ADR-0004 (nomes, PII) e o ADR-0006 (dimensões). Fecha as decisões pendentes da fatia 1 sobre deduplicação e qualidade.

## Contexto

A carga inicial do ERP está na landing ([dicionário](../dicionario-de-dados-erp.md)). Faltava decidir como ela chega até as dimensões. Estas eram as perguntas abertas no roadmap:
- deduplicar na bronze ou na silver;
- que regras de qualidade aplicar e com qual ação;
- como garantir que reprocessar não duplica;
- como tratar cadastros que chegam em extração full com histórico.

O SLA (D-1 até 8h) favorece não parar a carga por sujeira **conhecida**. A Controladoria, por outro lado, exige que nada errado seja publicado em silêncio.

## Decisão

Pipeline SDP `core` em SQL (`src/pipelines/core/`), com três camadas. O catálogo padrão vem do bundle; `${catalog_core}` só aparece no caminho do volume.

**1. Bronze: cópia fiel e append-only**
- Uma streaming table por dataset, com Auto Loader (`read_files`, Parquet), sem filtro nem regra.
- Acrescenta só metadados: `_source_file`, `_extraction_date` (tirada do caminho) e `_ingested_at`.
- A bronze aceita tudo, porque é a fonte da verdade para reprocessar (ADR-0007). O Auto Loader lê cada arquivo uma única vez.

**2. Silver dos fatos: deduplicação por `AUTO CDC` (SCD 1)**
- **A view de limpeza** (`erp_*_clean`, uma streaming live view):
  - renomeia as colunas para o contrato em inglês;
  - tipa os valores em `DECIMAL(18,2)`;
  - aplica as expectations;
  - deriva `invoice_type` do CFOP.
- **O `AUTO CDC INTO`** mantém uma linha por chave natural, a mais recente por `SEQUENCE BY STRUCT(_source_updated_at, …, _ingested_at)`.
- **O que isso resolve:** duplicatas exatas, quase-duplicatas (status antigo) e reenvio de arquivo, que vira upsert e nunca duplicata.

**3. Silver dos cadastros: recalculada da extração mais recente**
- Cliente, produto e hierarquia são materialized views sobre a **última** `_extraction_date` da bronze, que já contém o histórico inteiro (ADR-0006).
- **Cliente:**
  - valida o dígito verificador do CNPJ/CPF (módulo 11) e guarda só `is_document_valid`;
  - descarta a PII;
  - troca a cidade digitada pela cidade do CEP.
- **Vigência:** o intervalo fechado do ERP vira `[valid_from, valid_to)`.

**4. Gold: dimensões do ADR-0006**
- `dim_date` (tipo 0), `dim_product` (tipo 1) e `dim_customer` e `dim_sales_rep` (SCD 2 por atributo, fundindo versões consecutivas sem mudança rastreada).
- Chave `xxhash64` e membro `-1`.
- **Regra nova, `is_latest`:** marca a última versão de cada entidade, inclusive de vendedor desligado, que não tem versão vigente. A visão "como é hoje" usa `is_latest`, e não `is_current`, para que as vendas de desligados (R$ 6,6 mi em jan–jun/2026) não fiquem sem região.

**5. Regras de qualidade**

| Regra | Onde | Ação | Por quê | Violações na carga inicial |
|---|---|---|---|---|
| Chave natural nula | todas as tabelas da silver | `FAIL UPDATE` | Sem chave não há deduplicação nem junção: é quebra de contrato, e é melhor parar a carga do que publicar errado | 0 |
| Data do pedido nula | `erp_order_header` | `DROP ROW` | Sem data de negócio, o pedido não entra em "vendas de ontem" | 0 |
| Status desconhecido | `erp_order_header` | aviso | Medir sem bloquear; status novo no ERP é aviso de mudança de contrato | 0 |
| Quantidade ou valor negativo | `erp_order_item` | `DROP ROW` | Erro de integração (devolução vem por NF, não por pedido negativo) | 3.038 |
| Quantidade ≤ 0 no item de NF | `erp_invoice_item` | `DROP ROW` | Item fiscal com quantidade zero é inválido | 16.285 (ver achados) |
| CFOP desconhecido | NFs | aviso | Operação fiscal nova precisa de regra antes de entrar no fato | 0 |
| Devolução sem NF de origem | `erp_invoice_header` | aviso | Devolução deve referenciar a venda | 0 |
| Documento com dígito inválido | `erp_customer` | aviso | O cliente existe e compra; o flag vai para a dimensão | 1.336 versões (735 clientes) |
| CEP fora da referência | `erp_customer` | aviso | Cidade ficaria nula; referência incompleta | 0 |
| Vigência invertida | cadastros | aviso | Detectar erro de cadastro no ERP | 0 |

As contagens ficam no event log do pipeline, na aba *Data quality*. Nada some sem métrica.

**6. Retentativas:** 1 tentativa extra por update e por flow (`pipelines.numUpdateRetryAttempts = 1`). O padrão de 5 repetiria uma falha determinística e queimaria a cota.

## Achados da primeira execução (2026-10-20)

**Contagens:** tudo bate com os gabaritos (`data_gen/erp/manifest_*.json`):
- 805.153 pedidos únicos;
- 2.931.839 itens;
- 838.957 NFs, 11.158 delas de devolução;
- 72.650 versões de cliente → 47.166 na `dim_customer`;
- 7.264 grafias corrigidas;
- 6 vendedores desligados;
- nenhuma coluna de PII na silver ou na gold.

A consulta "quem era o supervisor do vendedor X na data D" devolve exatamente uma linha.

**Dois problemas reais apareceram e mudaram a implementação:**

1. **Empate no `SEQUENCE BY`.** Em 269 pedidos, a linha reenviada com status `ABERTO` e a linha final tinham o **mesmo** `dt_atualizacao` (faturados no mesmo instante da criação). O `AUTO CDC` escolheu arbitrariamente, e 269 pedidos ficaram `ABERTO`.
   - **Correção:** desempate pelo ciclo de vida do pedido, `SEQUENCE BY STRUCT(_source_updated_at, _status_rank, _ingested_at)`. Na mesma hora, o status mais avançado vence, porque um pedido não regride.
   - **Lição:** sequência com empate deixa o resultado não determinístico. Em produção, todo `SEQUENCE BY` precisa de desempate.
2. **Item de NF com quantidade 0.** Foi um defeito do gerador: no faturamento parcial de item com 2 unidades, a 2ª NF ficou com 0. Isso gerou 16.285 itens e 945 NFs "fantasmas", somando −R$ 0,61.
   - **Como apareceu:** pela expectation `positive_quantity`, que estava como aviso.
   - **Correção:** a regra virou `DROP ROW` e o gerador foi corrigido para as próximas execuções.
   - **Carga de 2026-10-19:** mantém o defeito, registrado no gabarito. Regerar exigiria apagar e reingerir a bronze inteira.

As duas correções só valem para dados novos. Para aplicá-las ao histórico, é preciso um ***full refresh* seletivo** de `silver.erp_order_header` e `silver.erp_invoice_item`. As tabelas são reconstruídas a partir da bronze, que guarda tudo; é o ADR-0007 em ação.

## Alternativas consideradas

| Alternativa | Por que não |
|---|---|
| Deduplicar na bronze | A bronze deixaria de ser fiel: um reenvio apagado não deixaria rastro, e uma regra de dedup errada não teria volta |
| `ROW_NUMBER()` / `dropDuplicates` na silver | Não resolve "a versão mais recente vence" nem atualizações tardias, e mantém estado sem limite. O `AUTO CDC` faz isso de forma declarativa |
| `AUTO CDC FROM SNAPSHOT` para cadastros | Dataria as versões pela extração, não pela vigência (ADR-0006) |
| `FAIL UPDATE` para toda sujeira conhecida | Itens negativos e documentos inválidos derrubariam a carga todo dia e furariam o SLA por um problema já conhecido |
| Tabela de quarentena com motivo | É a prática melhor para reprocessar depois que a fonte corrige. Fica para a fatia 3, cujo tema é contratos e quarentena. Aqui, `DROP ROW` com métrica já não é silencioso |
| Corrigir o sinal dos itens negativos (`abs`) | Supõe que o erro de integração é sempre de sinal. Sem confirmação do time do ERP, corrigir é inventar dado |
| Expectations em `CREATE TEMPORARY VIEW` (sintaxe moderna) | Não suportam `CONSTRAINT`. A `STREAMING LIVE VIEW` é a sintaxe legada mantida exatamente para isso; a alternativa seria materializar uma tabela intermediária, pagando armazenamento |

## Consequências

- **Positivas:**
  - reprocessar não duplica, por três mecanismos: arquivo lido uma vez, upsert por chave e cadastro recalculado;
  - toda regra de qualidade tem métrica;
  - a PII some na primeira transformação;
  - o pipeline inteiro roda em ~2,5 min.
- **Negativas / dívidas assumidas:**
  - o `fact_sales` vai somar ~0,1% a menos que o total do cabeçalho do pedido, por causa dos 3.038 itens descartados. A diferença é conciliável, mas a correção definitiva depende do ERP;
  - os cadastros são recalculados inteiros a cada execução (~73 mil linhas: barato hoje);
  - a `STREAMING LIVE VIEW` é sintaxe legada e precisa ser acompanhada nas notas de versão do SDP;
  - mudar a regra de uma streaming table exige *full refresh* para valer no histórico.
- **O que nos faria revisitar:**
  - o volume dos cadastros pesar na janela;
  - a fatia 3 trazer a quarentena;
  - o SDP oferecer expectations em views temporárias modernas.

## Em produção faríamos diferente?

- **Quarentena:** linhas rejeitadas iriam para quarentena com motivo e reprocesso automático quando a fonte corrigisse.
- **Alertas:** violações e falhas iriam para alerta (event log materializado e consultado por um alerta SQL).
- **Cadastros grandes:** dezenas de milhões de clientes seriam incrementais (CDC da fonte ou snapshot comparado).
- **Contrato:** o `SEQUENCE BY` e as chaves estariam num contrato de dados, com o desempate acordado com o time do ERP.

**Em 10x de volume:** a arquitetura se mantém. O *full refresh* seletivo fica mais caro, o que reforça testar regras novas antes, com *validate-only* e uma amostra.

**Em 100x:** liquid clustering nas silver grandes (fatia 4) e cadastros incrementais.

## Conceitos para estudar

- bronze imutável e reprocessamento;
- Auto Loader e checkpoint;
- `AUTO CDC` / SCD 1 e a importância do desempate no `SEQUENCE BY`;
- expectations: aviso × `DROP ROW` × `FAIL UPDATE`;
- quarentena;
- *full refresh* seletivo;
- módulo 11 (CNPJ/CPF);
- *gaps and islands* para fundir versões;
- defeito de dados encontrado por teste: por que medir antes de descartar.
