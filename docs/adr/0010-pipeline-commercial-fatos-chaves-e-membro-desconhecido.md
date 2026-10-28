# ADR-0010: Construir o pipeline commercial com fatos recalculados, chaves pela data do documento que carrega a FK, motivo do membro desconhecido exposto e regras de vigência nos cadastros

- **Status:** aceito — emendado pelo [ADR-0012](0012-job-diario-extracao-simulada-idempotencia-e-verificacao.md) (D13 entregue na tarefa `verify_uniqueness`, ampliada com a contagem de cada fato contra a silver e com as versões repetidas na silver de cadastros; idempotência provada, o que fecha a dívida "idempotência ainda sem prova"; "com o gerador corrigido, as próximas cargas devem coincidir" refinado: a regra corrigida do faturamento parcial vale só para pedidos cuja 1ª NF sai depois de 2026-10-18; os pipelines deixam de ter retentativa de tarefa no job)
- **Data:** 2026-10-21
- **Fatia:** fatia 1 — Comercial
- **Decisor:** Engenheiro de Dados Sênior (Claude) · **Acompanhamento:** usuário (pode vetar)
- **Relação:** implementa os fatos do ADR-0003 com as chaves do ADR-0006. Emendas:
  - [ADR-0003](0003-fatos-de-vendas-e-faturamento.md): D4 (bruto do `fact_invoice`), D5 (CFOP sem regra fora da soma) e D8 (série da NF no grão);
  - [ADR-0006](0006-dimensoes-conformadas-identidade-scd-e-chaves.md): D1 e D1b (data da versão no `fact_invoice`), D2 (motivo do `-1`) e D12 (`dim_product` pela última versão);
  - [ADR-0008](0008-pipeline-core-deduplicacao-qualidade-e-cadastros.md): D11 (regras de vigência sobreposta e buraco na tabela do §5) e D14 (regras à prova de nulo: as de descarte passam a descartar e contar o nulo). O D11 cumpre a regra prometida nas Consequências do ADR-0006.
  - [ADR-0005](0005-orquestracao-por-job-e-codigo-em-bundle.md): D13 (tarefa final de verificação de unicidade no job `serra_azul_daily`).

## Contexto

O pipeline `core` publica a silver e as dimensões conformadas (ADR-0008), e faltavam os fatos do ADR-0003 no catálogo `commercial`. Ao implementá-los, apareceram lacunas nos ADRs anteriores: a NF não tem vendedor, e o membro `-1` tem mais de um motivo, que pedem ações diferentes. Os centavos da NF não fecham bruto − desconto = líquido, e o ADR-0003 não diz em que camada aplicar o sinal das devoluções. A regra de vigências sobrepostas prometida no ADR-0006 não existia. A Controladoria exige bater com o fiscal, e o Diretor Comercial lê a região comercial do vendedor "como era" (ADR-0006 §7).

## Decisão

**Os fatos comerciais são materialized views recalculadas a cada execução, com a chave de cada dimensão resolvida na data do documento que carrega a FK e o motivo de cada `-1` exposto numa coluna técnica.**

O pipeline SDP `commercial`, em SQL ([`resources/commercial.pipeline.yml`](../../resources/commercial.pipeline.yml), `src/pipelines/commercial/gold/`), publica as duas materialized views em `commercial.gold`. Elas leem `${catalog_core}.silver.*` e `${catalog_core}.gold.dim_*` e não reconstroem nada do `core`.

### Contrato dos fatos

| | `fact_sales` | `fact_invoice` |
|---|---|---|
| Grão (chave natural) | `(order_id, order_item_number)` | `(invoice_id, invoice_series, invoice_item_number)` |
| Data de negócio | `order_date` | `issue_date` |
| Dimensões (`_key` + `_id`) | cliente, vendedor, produto | cliente, vendedor, produto |
| Degeneradas e atributos | `order_id`, `order_status`, `is_cancelled` | `invoice_id`, `order_id`, `order_item_number`, `invoice_type` |
| Medidas | `quantity`, `volume_hl`, `gross_amount`, `discount_amount`, `net_amount` | as mesmas, com sinal (D5) |
| Técnicas | `_is_customer_id_known`, `_is_sales_rep_id_known`, `_updated_at` | as mesmas e `_is_order_header_found` (D6) |

Cliente e vendedor usam a `_key` da versão vigente na data, pelo intervalo `[valid_from, valid_to)` (ADR-0006 §3). O produto usa a `_key` por `product_id` (SCD 1, D12).

### D1. O vendedor do item de NF é resolvido na `order_date` do pedido de origem

- **Critério:** a chave é resolvida na data do documento que carrega a FK. O vendedor não está na NF: ele está no pedido. Então vale a `order_date`.
- **Precisão no ADR-0006 §5:** a "data do evento" é a data do documento que carrega a FK. Para o cliente da NF, é a `issue_date` (D1b).
- **Problema que resolve:** resolvido na `issue_date`, o vendedor ficava em `-1` em 255 itens de NF (238 de venda e 17 de devolução; 80 NFs; ~R$ 71,7 mil sem sinal). Eram NFs de vendedores desligados: o pedido foi tirado com o vendedor ativo, e a NF saiu 1 a 19 dias depois do fim da vigência. Medido depois da mudança: 0.
- **Custo, medido em 2026-10-21:**
  - 14.520 itens de NF (13.198 de venda e 1.322 de devolução) mudam de versão do vendedor entre a data do pedido e a da emissão;
  - em 344 deles (307 e 37), muda a região comercial: R$ 102.550,60 sem sinal (R$ 95.619,66 + R$ 6.930,94);
  - **consequência:** no faturamento mensal por região, vale a estrutura comercial da data do pedido. Esses 344 itens cairiam em outra região se fossem resolvidos na emissão.
- **Ganho:** o mesmo item tem a mesma região comercial no `fact_sales` e no `fact_invoice`. O *drill-across* por região fecha item a item.

### D1b. O cliente do item de NF é resolvido na `issue_date`

- **Critério:** o mesmo do D1. A NF carrega o `customer_id` (o destinatário fiscal), então vale a `issue_date`.
- **Consequência concreta:** a versão na emissão é a que está impressa na NF.
- **Medido em 2026-10-21:** em 252 itens de NF (R$ 75.777,35 sem sinal), a versão do cliente na `order_date` difere da versão na `issue_date`. Um item pode mudar mais de um atributo:

| Atributo que muda | Itens |
|---|---|
| Endereço | 209 |
| Segmento (`trade_segment`) | 43 |
| UF | 6 |
| Canal (`sales_channel`) | 0 |
| Região geográfica | 0 |

- A UF de destino importa para a conciliação fiscal. Resolver pela `order_date` deixaria 6 itens com UF diferente da que consta na NF.
- **Custo:** o mesmo item tem segmento diferente nos dois fatos em 43 itens, e canal diferente em 0. Se o dashboard comparar segmento (ou canal) entre os fatos, a medição deve ser refeita.
- **Terminologia:** o canal do dashboard é o `sales_channel` (DIRETO, DISTRIBUIDOR, REDE), os três canais do [caso de negócio](../caso-de-negocio.md) §1. A "mudança de canal" do PDV que o dicionário descreve é mudança de `trade_segment`.

### D2. O motivo do `-1` fica em colunas técnicas

`_is_customer_id_known` e `_is_sales_rep_id_known`, em cada fato, separam os dois motivos:

| Caso | `_is_<entidade>_id_known` | `_key` | Significado | O que fazer |
|---|---|---|---|---|
| (a) | `false` | `-1` | O `_id` não está na dimensão | Esperar a carga seguinte. Se não se resolver, investigar FK nula na origem ou exclusão por LGPD |
| (b) | `true` | `-1` | O `_id` existe, mas a data do evento cai **fora da vigência total** da entidade: antes do primeiro `valid_from` (antes do cadastro) ou depois do fim da última versão fechada (encerramento ou desligamento) | Verificar se a data do evento é a certa (D1) ou se a vigência total no ERP está errada |

**Origens do (a):**
- **cadastro atrasado:** o caso esperado. A linha se corrige sozinha na próxima execução (ADR-0006 §6);
- **FK nula na origem:** hoje, 0 pedidos com `customer_id` ou `sales_rep_id` nulo;
- **cliente apagado por direito ao esquecimento (LGPD):** a bronze e a landing são apagadas, o cliente some da dimensão, e os fatos ficam em `-1` (a) para sempre. É a consequência desejada: o `customer_id` vira um código sem como ser ligado à pessoa, e os valores fiscais permanecem. Os fatos não carregam PII (ADR-0004);
- **item sem cabeçalho:** ver a contagem dupla, abaixo.

**Origem do (b).** Um buraco no meio do histórico **não** gera `-1`. As dimensões calculam `valid_to = coalesce(lead(valid_from), last_valid_to)`: a versão anterior se estica, em silêncio, por cima do buraco. O (b) só acontece fora da vigência total. Exemplo: os 255 itens do D1, de vendedores desligados resolvidos na emissão. Eram legítimos, sem erro do ERP.

**Expectations de aviso:**

| Expectation | `fact_sales` | `fact_invoice` | Conta |
|---|---|---|---|
| `order_header_found` | sim | sim | item sem o cabeçalho do pedido (D6) |
| `invoice_header_found` | — | sim | item de NF sem o cabeçalho da NF (D6) |
| `known_invoice_type` | — | sim | CFOP sem regra (D5) |
| `amounts_add_up` | sim | — | bruto − desconto ≠ líquido (D4) |
| `customer_id_in_dimension` | sim | sim | cliente (a) |
| `customer_version_valid_on_<order\|issue>_date` | `order_date` | `issue_date` | cliente (b) |
| `sales_rep_id_in_dimension` | sim | sim | vendedor (a) |
| `sales_rep_version_valid_on_order_date` | sim | sim | vendedor (b) |
| `product_id_in_dimension` | sim | sim | produto (a). O produto não tem (b): a junção é por `product_id`, sem data (D12) |

- **15 regras:** 7 no `fact_sales` e 8 no `fact_invoice`. Três entraram ou mudaram depois da primeira execução: `order_header_found` no `fact_invoice` (D6), `amounts_add_up` (hoje 0) e a versão à prova de nulo de `known_invoice_type` (D5, D14).

**Contagem dupla, de propósito.** Uma linha sem cabeçalho conta também como caso (a):
- no `fact_sales`, o item sem o pedido conta no (a) do cliente e no (a) do vendedor;
- no `fact_invoice`, o item sem o pedido conta no (a) do vendedor, e o item sem a NF, no (a) do cliente.

Para ler o (a) puro, subtraia a contagem do cabeçalho correspondente.

**Por que as regras ficam no próprio fato, e não numa view intermediária:**
- a documentação oficial diz: "*Because views are only calculated when queried, data quality metrics might not be available for a defined view*" (docs.databricks.com, *Expectations › Limitations*). No `core`, as expectations das streaming live views apareceram no event log (ADR-0008), mas isso é comportamento observado, não contrato;
- a regra no fato mede exatamente o que é publicado, inclusive colunas que só existem no resultado final: `_is_*` e as chaves.

É uma versão leve da *audit dimension* de Kimball.

### D3. O cliente do item de NF é o da própria NF

- É o destinatário fiscal.
- Hoje, 0 NFs têm `customer_id` diferente do pedido. A data da versão desse cliente é o assunto do D1b.

### D4. Emenda ao ADR-0003: `gross_amount` do `fact_invoice` = `net_amount + discount_amount`

- O fiscal manda: bruto − desconto = líquido em toda linha dos dois fatos.
- No `fact_sales`, o bruto continua `quantity × unit_price`, vindo da silver. Lá a conta já fecha: o `vl_total_item` do pedido é `quantidade × vl_unitario − vl_desconto` ([dicionário](../dicionario-de-dados-erp.md), §3.6). A regra `amounts_add_up` vigia essa igualdade no `fact_sales`.
- **Custo:** em ~3% dos itens de NF, bruto ÷ quantidade difere do preço unitário em centavos.

### D5. Emenda ao ADR-0003: o sinal das devoluções é aplicado só no fato, e CFOP sem regra fica fora da soma

- A silver guarda valores positivos, como no documento fiscal.
- O fato aplica o sinal uma única vez: `CASE invoice_type WHEN 'SALE' THEN 1 WHEN 'RETURN' THEN -1 END`, em `quantity`, `volume_hl` e nos três valores.
- CFOP sem regra (`invoice_type` nulo) fica com medidas nulas: fica fora da soma "já líquida" do ADR-0003 até ganhar regra.
- `known_invoice_type` conta o caso. A regra passou a ser à prova de nulo (`invoice_type IS NOT NULL AND invoice_type IN (...)`). Antes, o CFOP sem regra dava `NULL`, e não `false`, justamente no caso que a regra deveria contar (D14).

### D6. `LEFT JOIN` nos cabeçalhos

- Item sem cabeçalho fica no fato, com data nula.
- No `fact_sales`, `order_header_found` conta o item sem o pedido.
- No `fact_invoice`, `invoice_header_found` conta o item sem a NF, e `order_header_found` conta o item sem o pedido de origem. Sem o pedido, o vendedor fica nulo.
- O `order_header_found` do `fact_invoice` entrou depois da primeira execução. Antes, o caso só aparecia como vendedor (a), em `sales_rep_id_in_dimension`.
  - Ele lê a coluna técnica `_is_order_header_found` (`= order_date IS NOT NULL`). A medida é exata, porque a silver descarta pedido sem data (ADR-0008).
  - A `order_date` não é publicada no `fact_invoice`: a coluna guarda só o resultado.
  - Medido hoje: 0.
- A contagem do fato é igual à da silver **enquanto a `_key` das dimensões for única**. A tarefa de verificação do job confere isso (D13). Na primeira execução: 0 itens sem cabeçalho.

### D7. `_updated_at` = maior `_ingested_at` das linhas da silver que formam a linha do fato

- `fact_sales`: item e cabeçalho do pedido. `fact_invoice`: item da NF, cabeçalho da NF e cabeçalho do pedido.
- É determinístico: recalcular não muda o valor.
- **Custo:** uma órfã resolvida (troca de chave causada só pela dimensão) não muda o `_updated_at`. Um consumidor incremental guiado por ele perde essa correção.

### D8. Emenda ao ADR-0003: `invoice_series` no grão do `fact_invoice`

- Chave natural `(invoice_id, invoice_series, invoice_item_number)`, a mesma da fonte (`num_nf` + `serie` + `item_nf`).
- Hoje a série é sempre `1`. O grão não depende disso.

### D9. `volume_hl` em `DECIMAL(18,5)`

- `liters_per_sku` tem 3 casas; dividido por 100, dá no máximo 5.
- Produto `-1` fica com volume nulo: desconhecido não é zero.

### D10. Fora dos fatos, por YAGNI

| Coluna | Por que fica fora |
|---|---|
| `unit_price` | Não é aditivo. Preço médio = `sum(net_amount) / sum(quantity)` |
| `order_total_amount` | Medida de cabeçalho: repetida em cada item, somaria errado |
| `cfop` | O fato usa o `invoice_type`, derivado do CFOP na silver |
| `original_invoice_id` | A conciliação da devolução é item a item, por `order_id` + `order_item_number` |

Qualquer uma entra depois como mudança aditiva (convenções: o dono aprova e avisa os consumidores).

### D11. Emenda ao ADR-0008: regras de aviso para vigências sobrepostas e buracos nos cadastros

- **Escopo:** altera o pipeline `core` (dono: time técnico). Fica neste ADR porque nasceu da mesma revisão.
- A tabela de regras do ADR-0008 §5 ganha uma linha: vigência sobreposta / com buraco, nos cadastros, como aviso, com 0 violações na carga inicial.
- Cumpre a expectation prometida nas Consequências do ADR-0006 e acrescenta os buracos.

**Como funciona**
- Nas três silver de cadastro (`erp_customer`, `erp_sales_hierarchy` e `erp_product`), duas regras de aviso:

| Regra | Expressão | Conta |
|---|---|---|
| `no_overlap_with_next_version` | `_next_valid_from IS NULL OR _next_valid_from >= valid_to` | sobreposição: a versão seguinte começa antes de esta terminar. A dimensão **corta** a versão anterior em silêncio |
| `no_gap_to_next_version` | `_next_valid_from IS NULL OR _next_valid_from <= valid_to` | buraco: há dias entre o fim desta versão e o início da seguinte. A dimensão **estica** a versão anterior em silêncio |

- O motivo das duas regras é medir o que a dimensão faz sem avisar: o `coalesce(lead(valid_from), last_valid_to)` corta a sobreposição e estica a versão por cima do buraco. Nenhum dos dois casos gera `-1`.
- Elas leem uma coluna técnica nova, `_next_valid_from`: o `valid_from` da versão seguinte do mesmo id. Ela é nula na última versão, que portanto nunca viola.
- A janela usa `ORDER BY dt_inicio_vigencia, dt_fim_vigencia NULLS LAST`. A contagem é determinística; só num empate total de início e fim a linha marcada pode variar (ver Limitações). Duas versões abertas, ou com o mesmo início, contam como sobreposição.
- O cálculo das dimensões não muda: as regras só medem.

**Testes (2026-10-21)**
- Casos literais: sobrepõe, buraco, contíguo, duas abertas e mesmo início. Todos com o resultado esperado.
- Silver real: 0 sobreposições e 0 buracos em cliente (72.650 versões), vendedor (505) e produto (126).
- **Efetivação:** na primeira execução do job `serra_azul_daily`, quando as contagens passam a aparecer no event log.

**Limitações conhecidas**
- Num empate total de início e fim, a linha marcada pode variar entre execuções. A contagem não varia.
- Uma versão contida em outra mais longa conta como sobreposição e pode aparecer também como um "buraco" falso.

**Riscos**
- **Buraco legítimo na hierarquia.** Um vendedor recontratado tem buraco de verdade. A `dim_sales_rep`, porém, estende a versão anterior por cima dele. Decidir se a dimensão deve respeitar o buraco fica para o primeiro caso real (hoje 0).
- **Produto:** o D12 elimina o risco de duas versões abertas duplicarem linhas dos fatos. A regra continua avisando na silver.

### D12. Emenda ao ADR-0006: a `dim_product` usa a última versão de cada produto, e não `is_current`

- **Motivo:** a convenção `is_latest` do ADR-0008 diz que "como é hoje" usa a última versão.
- **Elimina dois riscos:**
  - um produto sem versão aberta sumiria da dimensão e iria para o (a);
  - duas versões abertas do mesmo produto duplicariam linhas dos fatos na junção.
- **Medido hoje:** 0 produtos sem versão aberta e 0 com duas versões abertas.
- **Conferência da `dim_product` nova:** as mesmas 92 linhas de produto, conteúdo idêntico ao da anterior (`EXCEPT` 0/0) e `_key` única.
- O ADR-0006 e o código descreviam a `dim_product` como "versão atual" (`WHERE is_current`).

### D13. Unicidade verificada por uma tarefa final do job

- O job `serra_azul_daily`, próximo passo da fatia, ainda não existe. Ele terá uma tarefa final de verificação.
- **O que ela confere:** unicidade do grão dos dois fatos e da `_key` das três dimensões (cliente, vendedor e produto).
- **Medido hoje:** 0 duplicatas nas três dimensões e nos dois fatos.
- **Status:** *a entregar* com o job.

### D14. Emenda ao ADR-0008: toda expectation dá `true` ou `false`, nunca `NULL`

- **Problema:** uma regra que dá `NULL` no caso que deveria contar mede 0 sem ter medido nada (lógica de três valores).
- **Formas adotadas:**
  - `x IS NOT NULL AND ...` quando a regra olha uma coluna;
  - `coalesce(..., false)` numa expressão composta;
  - `x IS DISTINCT FROM v OR ...` quando o nulo deve passar (`return_has_original_invoice`).
- A forma nova é correta qualquer que seja o tratamento de `NULL` pelo SDP. Esse tratamento não foi verificado na documentação.

**Regras corrigidas na varredura (2026-10-21)**

| Regra | Tabela | Antes |
|---|---|---|
| `known_invoice_type` | `commercial.gold.fact_invoice` | `NULL` no caso que devia contar |
| `known_status` | `erp_order_header` | `NULL` no caso que devia contar |
| `known_region` | `erp_sales_hierarchy` | `NULL` no caso que devia contar |
| `valid_interval` | `erp_customer`, `erp_product`, `erp_sales_hierarchy` | `NULL` no caso que devia contar; agora `valid_from IS NOT NULL AND ...` |
| `positive_volume` | `erp_product` | `NULL` no caso que devia contar |
| `positive_quantity_and_amount` (`DROP ROW`) | `erp_order_item` | `NULL` no caso que devia contar |
| `positive_quantity` (`DROP ROW`) | `erp_invoice_item` | `NULL` no caso que devia contar |
| `return_has_original_invoice` | `erp_invoice_header` | `NULL` num caso que **não** devia contar; agora `invoice_type IS DISTINCT FROM 'RETURN' OR ...`. O CFOP sem regra já é contado por `known_cfop` |

As tabelas sem catálogo ficam em `core.silver`.

**Efeito**
- Nas duas regras de descarte, o valor nulo é descartado **e contado** com certeza, qualquer que seja o tratamento de `NULL` pelo SDP (não verificado). Isso precisa a semântica das regras do ADR-0008 §5.
- Hoje há 0 nulos em todas as colunas envolvidas: nada muda no histórico.
- **Quando vale:** nas regras em streaming live view (`known_status`, as duas de descarte e `return_has_original_invoice`), para dados novos, sem *full refresh*. Nas regras em materialized view (`valid_interval`, `known_region` e `positive_volume` nas silver de cadastro; `known_invoice_type` no `fact_invoice`), para todo o histórico na próxima execução, como no ADR-0009.

### Execução

- **Ordem:** o `commercial` roda depois do `core`. O job `serra_azul_daily` vai garantir essa ordem (ADR-0005). O significado exato do limite de "1 pipeline ativo por tipo" do Free Edition segue sem teste.
- **Retentativas:** 1 por update e por flow, como no `core` (ADR-0008 §6).
- **Observação operacional (2026-10-21):** um *validate-only* do `commercial`, iniciado logo depois do fim do `core`, falhou uma vez com `RESOURCE_EXHAUSTED: You have hit your limit for serverless compute`. A nova tentativa, um minuto depois, passou.
  - Provavelmente é um limite de concorrência momentâneo do Free Edition, e não a cota diária.
  - No job, duas retentativas absorvem esse caso: a do pipeline (`numUpdateRetryAttempts` e `maxFlowRetryAttempts` = 1) e a de 1 por tarefa do job (ADR-0005).
  - A primeira rodada do job deve ser observada.

## Resultados da primeira execução (2026-10-21)

Update `0a632d`, 87 s (~1,5 min) com o arranque.

| Conferência | `fact_sales` | `fact_invoice` |
|---|---|---|
| Linhas (= silver) | 2.931.839 | 3.017.600 |
| Chaves nulas / duplicatas no grão | 0 / 0 | 0 / 0 |
| Soma de `net_amount` (= silver, com sinal na NF) | R$ 3.916.105.322,30 | R$ 3.825.572.385,53 |
| Cliente `-1` (a) / (b) | 1.070 / 0 | 1.084 / 0 |
| Vendedor `-1` (a) / (b) | 0 / 0 | 0 / 0 |
| Produto `-1` | 0 | 0 |

- **Outras conferências:** bruto − desconto ≠ líquido: 0 linhas. Devolução não negativa: 0 linhas.
- **Volume:** 4.200.325 hl no `fact_sales` (o dicionário registra 4,2 mi hl gerados).
- **Event log:** as métricas das 13 expectations da primeira execução repetem essas contagens. As regras que entraram ou mudaram depois (D2, D5, D6, D11, D14) foram medidas fora do pipeline, todas com 0. A confirmação delas no event log vem com a primeira execução do job.
- **SCD 2:** receita do NE em jan–jun/2026 = R$ 209,2 mi "como era" × R$ 197,4 mi "como é hoje" (`is_latest`), igual ao dicionário.
- **Cliente `-1` (a):** são os 227 clientes fora da carga (313 pedidos), o caso de cadastro atrasado.
- **Centavos da NF, 89.555 na silver × 90.031 no dicionário:**
  - o gerador marca a divergência de centavos sem olhar a quantidade. Por isso, 476 dos 16.285 itens de quantidade 0 (2,9%) também receberam o delta de ±1 a 3 centavos;
  - a silver descarta esses itens (`positive_quantity`, `DROP ROW`): 90.031 − 476 = 89.555;
  - a bronze tem 0 duplicatas e uma única extração;
  - os −R$ 0,61 das "NFs fantasmas" do ADR-0008 são exatamente esses 476 deltas. As outras 15.809 linhas de quantidade 0 têm líquido 0;
  - com o gerador corrigido, as próximas cargas devem coincidir.

**Premissas do ADR-0002 verificadas nesta entrega:**
- `${catalog_core}` funciona em nomes de tabela de outro catálogo, e não só no caminho do volume.
- Dois pipelines ETL coexistem no workspace. Se os dois podem rodar ao mesmo tempo continua sem teste.

## Alternativas consideradas

| Alternativa | Por que não |
|---|---|
| D1: vendedor na `issue_date`, mantendo o `-1` | 255 itens de vendedor conhecido ficariam sem região (R$ 71,7 mil), e outros 344 mudariam de região comercial entre os dois fatos |
| D1: versão `is_latest` como reserva quando não há versão na `issue_date` | Mistura "como é hoje" na visão "como era" |
| D1b: cliente na `order_date`, a mesma versão do `fact_sales` | A NF carrega o `customer_id`; a versão da emissão é a impressa nela. Pela `order_date`, 6 itens ficariam com UF diferente da que consta na NF |
| D2: view intermediária com as expectations | A documentação não garante métricas de view ("*might not be available*"). E a regra no fato mede o que é publicado, inclusive `_is_*` e as chaves, que só existem no resultado final |
| D2: *private* materialized view intermediária | Duplicaria ~3 mi de linhas. A disponibilidade no Free Edition não foi verificada |
| D2: membro `-2` para o caso (b) | Todo consumidor e dashboard precisaria conhecer um segundo membro especial, e o motivo ficaria no domínio da chave, não num atributo. O `_is_*` mantém um único membro especial e põe o motivo numa coluna |
| D2: tabela de qualidade separada | Ler o motivo de uma linha exigiria juntar com outra tabela, com refresh próprio. O booleano está na linha e é recalculado com ela |
| D2: motivos exclusivos (o item sem cabeçalho não conta no (a)) | As expressões codificariam uma precedência entre motivos (o cabeçalho antes da dimensão), acoplando as regras. Com 0 casos hoje, a regra de subtração basta |
| D3: cliente do pedido | Junção a mais, sem ganho: hoje 0 NFs divergem |
| D4: `gross_amount = quantity × unit_price` | 89.555 linhas em que bruto − desconto ≠ líquido, no dashboard da Controladoria |
| D5: `ELSE 1` no sinal | Uma remessa (CFOP sem regra) viraria faturamento |
| D5: `ELSE 0` no sinal | Esconderia a linha dentro das somas |
| D6: `INNER JOIN` nos cabeçalhos | Descartaria itens sem métrica |
| D6: publicar a `order_date` no `fact_invoice` para a regra ler | Seria uma segunda data de negócio no fato: decisão de negócio, sem pergunta que a peça hoje (YAGNI) |
| D6: `EXPECT (sales_rep_id IS NOT NULL)` no lugar de `_is_order_header_found` | Dependeria de uma garantia que a silver não dá: um pedido com vendedor nulo seria lido como pedido ausente |
| D7: `current_timestamp()` | Muda a cada execução: toda linha pareceria alterada |
| D7: `_source_updated_at` | Um consumidor incremental guiado por ele perderia as linhas reprocessadas no lakehouse com o mesmo carimbo da fonte (*backfill*, *full refresh*) |
| D9: `DECIMAL(18,2)` | Arredondaria 0,042 hl (caixa de 12 latas de 350 ml) para 0,04: ~5% de erro |
| D9: `DOUBLE` | Não soma de forma exata |
| D11: corrigir a vigência no cálculo da dimensão (cortar a sobreposição, preencher o buraco por regra) | Inventaria histórico. A vigência é do ERP (ADR-0006 §2): o certo é medir e levar o caso ao time do ERP |
| D11: `DROP ROW` na versão problemática | A versão anterior se esticaria em silêncio por cima da descartada, publicando atributos errados sem métrica |
| D11: `FAIL UPDATE` | Pararia a carga diária e furaria o SLA das 8h por um problema que não invalida o resto |
| D11: manter sem medir | A dimensão esconderia o problema: o `lead(valid_from)` estica ou corta versões em silêncio |
| D12: manter `is_current` na `dim_product` | Contraria a convenção `is_latest` do ADR-0008 e deixa dois riscos abertos: produto sem versão aberta no (a) e duplicação nos fatos |
| D14: manter as regras como estavam | Uma regra que dá `NULL` no caso que deveria contar mede 0 sem ter medido nada |
| D14: confiar no tratamento de `NULL` pelo SDP | Não foi verificado na documentação. A forma nova é correta qualquer que seja esse tratamento |

## Consequências

- **Positivas:**
  - a região comercial do item é a mesma nos dois fatos, e o *drill-across* por região fecha item a item;
  - o cliente do `fact_invoice` é o da NF impressa, inclusive a UF de destino;
  - nenhuma linha some sem métrica: a contagem do fato é a da silver, e cada `-1` tem motivo;
  - bruto − desconto = líquido em toda linha, e o faturamento bate com o fiscal;
  - recalcular dá o mesmo resultado: chaves `xxhash64` e `_updated_at` pela ingestão;
  - vigência com sobreposição ou buraco no ERP passa a ter métrica (D11);
  - nenhuma regra mede 0 por causa de um `NULL`, e as regras de descarte também descartam e contam o nulo (D14).
- **Negativas / dívidas assumidas:**
  - colunas técnicas entram no contrato: duas `_is_*` no `fact_sales`, três no `fact_invoice`, e `_next_valid_from` nas silver de cadastro;
  - uma linha sem cabeçalho conta em mais de uma regra; o (a) puro sai por subtração (D2);
  - cliente apagado por LGPD fica em `-1` (a) para sempre. É desejado, mas o (a) deixa de ser só "cadastro atrasado";
  - **buraco legítimo na hierarquia** (vendedor recontratado) fica coberto pela versão anterior na `dim_sales_rep`, até a decisão no primeiro caso real (D11);
  - 344 itens de NF ficam numa região comercial diferente da que teriam pela data de emissão (D1);
  - em 43 itens, o segmento do cliente difere entre os fatos (D1b);
  - em ~3% dos itens de NF, bruto ÷ quantidade difere do preço unitário em centavos;
  - **event log no schema de consumo:** aparece como tabela `event_log_*` em `commercial.gold` (o `core` faz o mesmo em `core.bronze`). Com o `GRANT` no catálogo, todo consumidor do `commercial` lê o event log. Destino e configuração ficam *a verificar* na fatia 2 (observabilidade), quando o event log ganha um schema técnico;
  - **sem refresh incremental:** o SDP recalcula os fatos inteiros. Os motivos `INPUT_NOT_IN_DELTA` e `EXCESSIVE_OPERATOR_NESTING` foram observados no `planning_information` do event log. É barato hoje (~6 mi de linhas);
  - **em série com o `core`:** o job vai garantir a ordem;
  - **idempotência ainda sem prova:** a segunda execução com números idênticos fica para o teste do job;
  - **unicidade sem verificação automática** até o job existir (D13);
  - **`RESOURCE_EXHAUSTED` logo depois do `core`:** observado uma vez num *validate-only*. No job, depende das retentativas do pipeline e da tarefa (Execução);
  - uma órfã resolvida não muda o `_updated_at`: um consumidor incremental guiado por ele perde a correção (D7).
- **O que nos faria revisitar:**
  - o (b) subir: evento fora da vigência total da entidade (data do evento errada ou vigência total errada no ERP);
  - o (a) não se resolver na carga seguinte: FK nula na origem ou exclusão por LGPD;
  - `known_invoice_type` contar linhas: CFOP novo precisa de regra antes de entrar no faturamento;
  - NFs com cliente diferente do pedido (hoje 0): o *drill-across* por cliente deixaria de fechar item a item;
  - o dashboard comparar segmento ou canal entre os dois fatos: refazer a medição do D1b;
  - as regras do D11 contarem sobreposição ou buraco; no primeiro buraco da hierarquia, decidir se a `dim_sales_rep` o respeita;
  - um caso sem cabeçalho aparecer e a contagem dupla atrapalhar a leitura: aí vale tornar os motivos exclusivos;
  - o volume chegar a 10x (ver abaixo);
  - o `RESOURCE_EXHAUSTED` derrubar a primeira rodada do job mesmo com a retentativa;
  - uma pergunta de negócio que precise de `unit_price`, `cfop` ou `original_invoice_id`.

## Perguntas difíceis

- **"A regra não era a data do evento? Por que o vendedor da NF usa a data do pedido?"** A data é a do documento que carrega a FK. O vendedor está no pedido, e não na NF. O cliente está na NF, então vale a emissão (D1b). Medido: 344 itens mudariam de região comercial se o vendedor fosse resolvido na emissão, e 6 ficariam com UF diferente da NF se o cliente fosse resolvido no pedido.
- **"Um buraco na vigência do vendedor manda vendas para o `-1`?"** Não. A dimensão estica a versão anterior por cima do buraco, em silêncio, e a venda fica com os atributos da versão anterior. Por isso existe o `no_gap_to_next_version` (D11). O `-1` (b) só aparece fora da vigência total: antes do cadastro ou depois do desligamento.
- **"E se o cadastro do cliente chegar 2 dias atrasado?"** As linhas entram com `customer_key = -1` e `_is_customer_id_known = false`. Na primeira execução depois da chegada, o fato é recalculado e elas se corrigem, sem reprocesso manual (ADR-0006 §6).
- **"E se um cliente pedir para ser esquecido (LGPD)?"** Bronze e landing são apagadas, e o cliente some da dimensão. Os fatos ficam em `-1` (a) para sempre, com os valores fiscais intactos e sem PII (ADR-0004).
- **"E se o extrato de pedidos ou de NFs chegar 2 dias atrasado?"** O Auto Loader lê o arquivo quando ele chega, a silver faz o upsert, e o fato recalculado põe as linhas na data de negócio certa. Até lá, o D-1 fica incompleto **sem aviso**: detectar fonte que não chegou é o tema da fatia 2. Um dia já reportado muda depois, o mesmo trade-off aceito no ADR-0006.
- **"E se a NF chegar antes do pedido?"** O item da NF entra com vendedor `-1` e `_is_order_header_found = false`. `order_header_found` conta o caso, e `sales_rep_id_in_dimension` o conta também como vendedor (a). Para o (a) puro, subtraia o primeiro do segundo. A linha se corrige quando o pedido chegar.
- **"E se o ERP mandar duas versões abertas do mesmo produto?"** `no_overlap_with_next_version` avisa na silver. A `dim_product` usa a última versão (D12), então os fatos não duplicam. A tarefa de verificação do job confere a unicidade (D13). A correção é na fonte, não na dimensão.
- **"Qual o custo em 10x?"** Extrapolação, não medição. Dos 87 s da primeira execução, 65 s foram arranque (fila e inicialização), que não crescem com o volume; o processamento levou 22 s. Multiplicado por 10, daria ~4 min; como o *shuffle* das junções cresce mais que linearmente, a estimativa honesta é de 5 a 10 min de processamento, mais ~1 min de arranque, para ~60 mi de linhas. Cabe na janela das 5h às 8h. O limite real é a cota de 1 execução diária.
- **"Por que não refresh incremental já?"** O SDP recusou o incremental da materialized view, pelos dois motivos observados no `planning_information`. Com ~6 mi de linhas, a recomputação é barata (YAGNI). Os caminhos para o incremental estão em "Em produção".

## Em produção faríamos diferente?

Sim:
- **Event log** num schema técnico, fora do schema de consumo do domínio e fora do alcance do `GRANT` dos consumidores.
- **Refresh incremental.** Dois caminhos, ambos hipóteses a verificar:
  1. os fatos como streaming tables com `AUTO CDC` a partir da silver, com reprocessamento periódico só das linhas `-1`. É o plano do ADR-0006, revisitado depois que o SDP recusou o incremental da materialized view;
  2. dividir a consulta em materialized views intermediárias, para reduzir o aninhamento (`EXCESSIVE_OPERATOR_NESTING`).
- **CFOP:** as regras de `invoice_type` viram contrato de dados com o fiscal.
- **Layout:** liquid clustering por data nos dois fatos. O suporte em materialized view fica *a verificar* na fatia 4.
- **Paralelismo:** sem o limite de 1 pipeline ativo, os domínios rodariam em paralelo depois do `core` (ADR-0005).

**Em 10x de volume** (~60 mi de linhas nos dois fatos): pela extrapolação acima, de 5 a 10 min de processamento, dentro da janela das 5h às 8h. O limite real é a cota de 1 execução diária. O incremental passa a valer a pena.

**Em 100x** (~600 mi): o incremental é obrigatório. Aparece também o *skew*: as 2 redes concentram 26,2% do volume em 2 `customer_id`, o que desbalanceia a junção com o cliente (tema da fatia 4).

## Conceitos para estudar

- *drill-across* e fatos conformados;
- qual data resolve a versão de uma SCD 2: a do documento que carrega a FK;
- membro desconhecido: *late-arriving dimension*, FK nula, exclusão por LGPD e evento fora da vigência total;
- como `lead(valid_from)` esconde buracos e sobreposições, e por que medir em vez de corrigir;
- validação de vigência com `lead()` e desempate na janela (contagem determinística, linha marcada não);
- *audit dimension* (Kimball) e colunas de auditoria;
- lógica de três valores no SQL: por que uma regra com `IN` precisa ser à prova de nulo;
- medidas aditivas, semiaditivas e não aditivas (`unit_price`, total de cabeçalho);
- valores com sinal e devoluções;
- `DECIMAL` × `DOUBLE`: precisão, escala e soma exata;
- materialized view: refresh completo × incremental (`planning_information`);
- *watermark* de carga incremental: por que o carimbo da fonte não basta;
- determinismo e idempotência;
- `LEFT JOIN` × `INNER JOIN`, unicidade de chave e conservação de contagens entre camadas;
- *skew* em junção com dimensão.
