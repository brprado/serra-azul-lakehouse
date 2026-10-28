# ADR-0012: Rodar a carga diária no job serra_azul_daily, com extração simulada determinística e verificação de unicidade

- **Status:** aceito (2026-10-28, depois de duas revisões do `adr-reviewer` e da checagem do `free-edition-checker`)
- **Data:** 2026-10-28
- **Fatia:** fatia 1 — Comercial
- **Decisor:** Engenheiro de Dados Sênior (Claude) · **Acompanhamento:** usuário (pode vetar)
- **Relação:** implementa o job do [ADR-0005](0005-orquestracao-por-job-e-codigo-em-bundle.md) e entrega o D13 do [ADR-0010](0010-pipeline-commercial-fatos-chaves-e-membro-desconhecido.md). Emendas (as linhas de status dos ADRs emendados são aceitas junto com este):
  - [ADR-0005](0005-orquestracao-por-job-e-codigo-em-bundle.md) §3: o simulador é um script Python numa `spark_python_task`, e não um notebook (D1). As retentativas passam a ser definidas tarefa a tarefa (1 no simulador e na verificação, 0 nos pipelines), no lugar de 1 em toda tarefa (D8). O agendamento pausado continua;
  - [ADR-0006](0006-dimensoes-conformadas-identidade-scd-e-chaves.md) §4 e [ADR-0011](0011-dim-customer-tipo-1-pela-ultima-versao.md): uma linha por id e início antes do cálculo de versões na `dim_customer` e na `dim_sales_rep` (D6). Resolve a pendência "prevenir ou só detectar" do ADR-0011;
  - [ADR-0008](0008-pipeline-core-deduplicacao-qualidade-e-cadastros.md) §3: a silver de cadastros junta as linhas idênticas da mesma extração (D5). A frase "o gerador foi corrigido para as próximas execuções" é refinada pelo D10;
  - [ADR-0010](0010-pipeline-commercial-fatos-chaves-e-membro-desconhecido.md):
    - D13 entregue e ampliado (D7);
    - os pipelines deixam de ter retentativa de tarefa no job (D8);
    - a dívida "idempotência ainda sem prova" fica fechada (Resultados);
    - a frase "com o gerador corrigido, as próximas cargas devem coincidir" é refinada pelo D10.

## Contexto

Os pipelines `core` e `commercial` estavam publicados e rodavam à mão sobre uma única extração, a carga inicial de 2026-10-19 (ADR-0008, ADR-0010). Faltavam três coisas para a fatia 1 fechar:
- **dado novo todo dia:** sem uma extração diária do ERP, não existe D-1 para o dashboard mostrar;
- **idempotência provada:** "recarga do mesmo dia não duplica" é critério de pronto da fatia, e o ADR-0010 deixou a prova para o job;
- **unicidade verificada:** o ADR-0010 D13 prometeu uma tarefa de verificação, e o ADR-0011 deixou para este ADR o caso de duas versões com o mesmo início.

O ERP é simulado: alguém precisa fazer o papel dele todo dia. As restrições são as de sempre: SLA D-1 até 8h, cota do Free Edition (1 execução por dia, agendamento pausado, ADR-0005) e bronze *append-only* com *full refresh* bloqueado (ADR-0007). Um arquivo errado na landing não se desfaz com um *reset* da bronze.

## Decisão

**Um job diário encadeia o simulador do ERP, os pipelines `core` e `commercial` e uma verificação final:**
- **o simulador** grava as extrações que faltam, sem sobrescrever nada. O conteúdo é determinístico, com o Faker em versão fixa, e continua a história da landing, inclusive a regra antiga do faturamento parcial para os pedidos da carga;
- **a silver de cadastros** junta as cópias de um reenvio;
- **as dimensões** ficam com uma versão por início;
- **a verificação**, que roda depois da publicação, falha o job se algo duplicou.

| # | Decisão |
|---|---|
| D1 | Simulador como script Python numa `spark_python_task` |
| D2 | O simulador continua o "mundo" da carga inicial |
| D3 | Cada extração é determinística; o simulador completa o que falta |
| D4 | Nome de arquivo fixo, e nunca sobrescrever |
| D5 | Silver de cadastros com `GROUP BY ALL`, para absorver o reenvio |
| D6 | Mesmo início: prevenir na dimensão e detectar na silver e no job |
| D7 | Verificação de unicidade com 10 contagens, depois da publicação |
| D8 | Job, retentativas, timeouts e parâmetros |
| D9 | Faker com versão exata |
| D10 | A regra do faturamento parcial depende da data da 1ª NF |

Código: [`resources/serra_azul_daily.job.yml`](../../resources/serra_azul_daily.job.yml), [`data_gen/erp/erp_daily_extraction.py`](../../data_gen/erp/erp_daily_extraction.py), [`src/jobs/verify_uniqueness.py`](../../src/jobs/verify_uniqueness.py).

### D1. O simulador do ERP é um script Python numa `spark_python_task`

- **O quê:** `data_gen/erp/erp_daily_extraction.py`, tarefa `simulate_erp_extraction`, serverless.
- **Por que script, e não notebook (emenda ao ADR-0005 §3):**
  - o simulador importa o gerador da carga inicial, um módulo irmão (D2);
  - os parâmetros entram por `argparse`, o mesmo padrão do job `erp_initial_load`, que já era script.
- **Testes:** houve testes locais com Spark, fora do repositório: a reprodução da carga e um oráculo com 36/36 casos. **Não há testes versionados.**
- **Import do módulo irmão:** o diretório de trabalho de uma `spark_python_task` serverless não é garantido. O job passa `--lib-dir ${workspace.file_path}/data_gen/erp`, e o script faz `sys.path.insert`. A UDF do Faker roda nos *workers*, que não têm essa pasta: o módulo vai serializado por valor junto com a UDF (`cloudpickle.register_pickle_by_value`).

**O que cada extração traz.** A extração da data E traz o que mudou no dia D = E − 1:

| Dataset | Extração | Conteúdo da extração E |
|---|---|---|
| `pedidos_cab` | incremental | Pedidos criados em D e cabeçalhos cujo status mudou em D (faturamento, cancelamento), com o status vigente em D. Mesmas sujeiras da carga: duplicata exata e quase-duplicata |
| `pedidos_itens` | incremental | Itens dos pedidos criados em D (itens não mudam depois). Mesma sujeira: ~0,1% negativos |
| `notas_fiscais` | incremental | NFs emitidas em D: venda, 2ª NF do faturamento parcial e devolução |
| `notas_fiscais_itens` | incremental | Itens dessas NFs. Mesma sujeira: divergência de centavos |
| `clientes_pdv` | full | Todas as versões conhecidas em E, com o histórico inteiro |
| `produtos` | full | Todas as versões, com o histórico inteiro |
| `hierarquia_comercial` | full | Todas as versões, com o histórico inteiro |
| `cep_municipio` | — | Não é reenviado: carga única |

- **Clientes fora da carga (a FK órfã):** os 227 clientes que ficaram fora da carga inicial chegam no cadastro na extração `go_live + 1 + floor(u("chegada_cadastro") × 10)`, entre 2026-10-20 e 2026-10-29. É o que prova que o `-1` se corrige na recomputação (ADR-0006 §6).
- **Simplificação (YAGNI):** depois do go-live, nenhuma versão nova de cadastro, nenhum cliente novo e nenhuma reorganização trimestral (a hierarquia fica na estrutura de 2026-10-01; a de 2027 não acontece). O único cadastro que chega é o dos órfãos.
  - **Custo:** as extrações diárias não exercitam SCD 2 novo. O SCD 2 continua provado pelos 18 meses da carga inicial.

### D2. O simulador continua o "mundo" da carga inicial

- **Uma fonte de verdade para as fórmulas:** o simulador importa `erp_initial_load.py`. A carga inicial foi refatorada para isso:
  - `build_orders` ganhou `first_day`, `last_day`, `as_of` e `id_offset`; `build_invoices` ganhou `first_issue_date` e `nf_offset`;
  - com os valores padrão, a carga inicial gera exatamente o que gerou em 2026-10-19.
- **O horizonte vem dos manifests:** período, data do go-live, escala e as contagens que a numeração continua (`pedidos_distintos`, `linhas` de NF).
- **Janela:** o simulador gera só os pedidos que ainda podem gerar evento.
  - NFs a partir de `end − MAX_EVENT_LAG` (2026-09-26);
  - pedidos a partir de `end − 2 × MAX_EVENT_LAG` = `end − 44` (2026-09-04), para cobrir essas NFs;
  - `MAX_EVENT_LAG = 22`: faturamento até D+2 e devolução de 3 a 20 dias depois dele. O simulador confere o maior atraso nos dados que gera e falha se passar do limite.
- **A numeração continua sem buraco:** pedidos a partir de `PV00805154`, NFs a partir de `100838958`. Ela segue a data, então basta somar o que veio antes da janela. Essa contagem só é conhecida no fim do horizonte da carga (os manifests), e por isso a janela começa antes dele, e não antes de D (comentário em `erp_daily_extraction.py`).
- **Calendário:** feriados, Natal e Carnaval foram estendidos até o fim de 2027, sem tocar nas datas do horizonte. Depois de 2027, nenhum feriado. **Gatilho:** revisar antes de 2027-10.

**Custo: a janela cresce um dia por dia de calendário, rodando ou não.**

| Data | Tamanho da janela | Em relação à carga inicial (548 dias) |
|---|---|---|
| 2026-10-27 (medido) | 53 dias (2026-09-04 a 2026-10-26) | ~10% |
| ~2027-03-03 | 180 dias | ~33% |
| ~2027-10-27 | ~420 dias | ~77% |
| ~2028 | mais de 548 dias | mais que a carga inteira |

- O "~10%" que descarta a alternativa "regerar a história inteira todo dia" é uma foto de hoje. **Com o tempo, a escolha converge para essa alternativa.**
- **Gatilho para reancorar:** a janela passar de 180 dias (por volta de 2027-03-03) ou o simulador passar de 10 minutos. Reancorar = gravar um manifesto de "checkpoint" com as contagens de pedidos e NFs até uma data, como os da carga inicial, e começar a janela a partir dele.
- **Pausa longa no job:** a execução com 8 extrações levou 212 s, e o custo marginal de cada extração é pequeno perto do cálculo da janela. N dias numa execução somam N × ~73 mil linhas de cliente num update (30 dias ≈ 2,2 mi). Estimativa, não medida: deve caber, mas o timeout de 1.800 s do simulador (D8) limita o tamanho do *catch-up*. O *catch-up* é um reprocessamento justificado no sentido do [caso de negócio](../caso-de-negocio.md) §6 ("reprocessos são exceção justificada").
  - **Trade-off:** avançar aos poucos, com `extraction_date` (como sugere o README), recalcula a janela a cada execução. Custa mais no total e menos por execução, e fica longe do timeout.

### D3. Cada extração é determinística, e o simulador completa o que falta

- **Determinismo:** o conteúdo de cada extração é função só da sua data. Os sorteios vêm de hash da chave com semente fixa, como na carga inicial. Depende também da versão do Faker e do ambiente serverless (D9).
- **Data alvo:** `extraction_date` vazio = hoje em `America/Sao_Paulo`. Data futura é erro. Data anterior ao go-live (2026-10-19) é erro.
- **Catch-up:** a tarefa grava todas as extrações que faltam entre a última existente e E.
  - "Última existente" é a **menor** entre as últimas de cada dataset. Assim, uma execução que caiu entre datasets da mesma data é completada.
  - Numa raiz sem extração nenhuma (ensaio), começa no go-live.
- **Extração existente nunca é regravada** (D4). "Existente" = a pasta da data tem algum `.parquet` (`has_extraction`).
- **Buraco no meio da série não é preenchido (YAGNI):** o catch-up anda só a partir da última. Um buraco no meio só existe se alguém apagar uma pasta.
- **Recarga de um dia já gravado:** passe `extraction_date` explícito. Em 2026-10-28, com `extraction_date=2026-10-27`, o simulador não tinha nada a gravar, e os pipelines rodaram de novo sobre a mesma landing. Rodar de novo sem dado novo é uma **recarga**, e não um reprocessamento.
- **Requisito de reprocessamento** (caso de negócio §6: "qualquer dia pode ser reprocessado sem gerar duplicata"). Três mecanismos:
  - **recarga:** rodar de novo não muda nada (provado, Resultados);
  - **reenvio da extração pela fonte:** absorvido (D5). No simulador, o reenvio sai uma vez por data, porque o sufixo `_reenvio` é fixo. O próximo teste de reenvio precisa ser de outra data, e não de 2026-10-27;
  - **reprocessar com regra nova:** *full refresh* seletivo da silver a partir da bronze (ADR-0008).
- **Retentativa do simulador:** repetir completa o que faltou e não toca no que existe. **Ressalva:** um arquivo truncado conta como existente (ver Perguntas difíceis). Se a interrupção for por timeout, não há retentativa, e o job falha de forma visível (D8): antes de rodar de novo, apague a extração incompleta. Se for outra falha, a retentativa, ou a execução seguinte, termina com SUCCESS e empurra a falha para o `core`.

### D4. Nome de arquivo fixo, e nunca sobrescrever

- **Caminho:** `/Volumes/core/landing/erp/<dataset>/<AAAA-MM-DD>/<dataset>.parquet`.
- **Nunca sobrescrever:** com `cloudFiles.allowOverwrites = false` (o padrão), "*files are processed exactly once*", e o estado do Auto Loader é por caminho ([opções](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/options) e [FAQ](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/faq) do Auto Loader). Então cada extração ocupa um caminho, gravado uma vez.
  - **Sobrescrito antes de ser processado:** não há garantia de qual versão foi lida.
  - **Sobrescrito depois de processado:** nunca é reingerido. O conteúdo novo não chega à bronze.
- **De onde vem a idempotência do gravador:** da checagem de existência por pasta (D3), e não do nome.
- **O que o nome fixo compra:**
  - um arquivo por dataset × data, então "existe" ≈ "completo" (com a ressalva do arquivo truncado);
  - um caminho determinístico, para comparar ensaios;
  - o reenvio num caminho que se distingue do original.
- **Escrita:**
  1. o Spark grava com `coalesce(1)` em `{raiz}/erp/_work`, fora das pastas que a bronze lê (o Spark só grava pastas com `part-<uuid>`). Além disso, o Auto Loader ignora por padrão os caminhos com segmento que começa com `_` ou `.` (`ignoredPathSegmentRegex = ^[._]`, documentado para o Runtime 19+);
  2. o arquivo único é copiado com `shutil.copyfile` para o nome final. A [documentação de volumes](https://docs.databricks.com/aws/en/volumes/volume-files) não fala de visibilidade de escrita parcial nem da atomicidade de `os.rename`; diz só que *append* direto e escrita aleatória não são suportados. **Risco aceito** (ver o arquivo truncado em Perguntas difíceis);
  3. o simulador lê o rodapé do arquivo final para contar as linhas.
- **Reenvio:** `resend=true` grava `<dataset>_reenvio.parquet` na mesma pasta: o mesmo conteúdo, outro caminho. Simula o reenvio de integração do ERP. Só vale depois do go-live e com a extração original presente.
- **Exceções documentadas:** o direito ao esquecimento, que exige regravar ou apagar arquivos da landing (ver PII e LGPD), e o reparo de um arquivo truncado no meio da série (ver Perguntas difíceis). Apagar a extração incompleta depois de um timeout não é exceção: o `core` nunca leu aqueles arquivos.
- **Carga inicial:** não rodar de novo na landing real. Os dados sairiam iguais, mas em arquivos `part-<uuid>` com nomes novos, e a bronze leria a carga inteira outra vez. **A proteção é só um comentário** em [`resources/erp_initial_load.job.yml`](../../resources/erp_initial_load.job.yml) (ver Consequências). Para testar o gerador, use `--output-root` numa pasta que nenhuma bronze lê.

### D5. A silver de cadastros junta as linhas idênticas da mesma extração

- **Problema:** a silver de cadastros lê a extração mais recente (`_extraction_date = max`, ADR-0008 §3). Num reenvio, a bronze tem duas cópias da mesma extração, com a mesma `_extraction_date`. Sem tratamento, a silver dobra.
- **Escolha:** nas três silver (`erp_customer`, `erp_product`, `erp_sales_hierarchy`), a extração mais recente é lida com `SELECT * EXCEPT (_source_file, _ingested_at) ... GROUP BY ALL`.
  - Linhas idênticas nas colunas de negócio viram uma.
  - As colunas técnicas que mudam no reenvio (arquivo e instante da ingestão) ficam de fora.
- **Silver de fatos:** não muda. O `AUTO CDC` já absorve o reenvio como *upsert* por chave (ADR-0008 §2), e a prova confirmou.

**O achado da prova de reenvio (2026-10-27, update `3e9fdd9c` do `core`).** A primeira versão usava `SELECT DISTINCT * EXCEPT (...)`.

- **Observado:**
  - a silver dobrou nos três cadastros: cliente 72.976 → 145.952; produto 126 → 252; hierarquia 505 → 1.010;
  - a mesma consulta, no SQL warehouse e sobre a mesma tabela de materialização da bronze, deu o resultado certo (126 em produto);
  - as cópias diferiam só em `_next_valid_from`;
  - o event log registrou recomputação completa (`COMPLETE_RECOMPUTE`). Ele só traz o operador do topo, e não o plano;
  - as regras de aviso contaram as cópias nos três cadastros: `no_overlap_with_next_version` deu 72.976 em cliente, 505 em hierarquia e 126 em produto.
- **Inferência (não verificada):** a janela do `lead()` viu as duas cópias.
- **Evidência que estreita a hipótese:** os `SELECT DISTINCT customer_id` e `SELECT DISTINCT sales_rep_id`, de uma coluna só, nas materialized views `fact_sales.sql` e `fact_invoice.sql`, funcionam: a contagem do fato bate com a da silver em todas as execuções. O defeito é **compatível** com a combinação `DISTINCT` + `* EXCEPT (...)`, mas não a isola: entre os dois casos mudam várias variáveis (uma coluna × muitas, o pipeline, a fonte materialized view × streaming table, e o `lead()` depois).
- **Hipóteses (não testadas):**
  - **H1:** no pipeline, a deduplicação foi aplicada antes da remoção das colunas do `EXCEPT`;
  - **H2:** o planejador da materialized view injetou uma coluna abaixo do `DISTINCT`.
- **Testes possíveis:**
  - **o perfil de consulta do reenvio, sem custo de compute, ainda não aberto.** Cada fluxo gera um `REFRESH` no Query History (filtro Compute = "Pipeline compute", ou aba Performance do pipeline), com o *query profile* completo ([histórico de consultas de pipeline](https://docs.databricks.com/aws/en/ldp/query-history)). O histórico lista o `REFRESH MATERIALIZED VIEW core.silver.erp_product` do update `3e9fdd9c`: `query_id` `dd42dcbc-a415-4154-ae41-c58412b2bf98`, `plans_state: EXISTS`. O plano não vem pela API pública, só pela interface. É o teste que decide entre H1 e H2. O registro confirma que o histórico de consultas do pipeline existe neste workspace do Free Edition;
  - uma reprodução mínima num pipeline de teste, com duas linhas que diferem só numa coluna excluída. Não executada: custa uma execução.
- **Por que `GROUP BY ALL` resolve:** foi validado **empiricamente**, numa execução, sobre as cópias que já estavam na bronze (recarga de 2026-10-28). A chave de agrupamento sai da lista do `SELECT`, já sem as colunas excluídas. A explicação é coerente com o resultado, mas a causa do `DISTINCT` continua não confirmada.
- **Risco residual:**
  - os pipelines usam `channel: CURRENT`: uma atualização do runtime pode mudar o comportamento sem mudança de código;
  - a lista de exclusão é frágil: uma coluna técnica nova na bronze que difira entre as cópias reabre o defeito;
  - **mitigação:** a contagem nova da verificação, versões repetidas `(id, valid_from)` na silver (D7), pega as duas situações.

**Reenvio corrigido da mesma data: não tratado.**
- Se a correção mantiver o `valid_from`, as duas versões ficam na silver, a dimensão fica com uma (D6), e `no_overlap_with_next_version` e a verificação (D7) acusam.
- Se a correção mudar o início, ou acrescentar ou remover versões, a silver e a dimensão publicam **a união** das duas cópias até a extração do dia seguinte, que passa a ser a mais recente.

### D6. Versões com o mesmo início: prevenir na dimensão e detectar na silver e no job

- **O caso (ADR-0011):** duas versões da silver com o mesmo id e o mesmo `valid_from` abrem duas ilhas com a mesma `_key = xxhash64(id, valid_from)`. A junção do fato pela `_key` duplica linhas.
- **Prevenir:** CTE `one_per_start` na `dim_customer` e na `dim_sales_rep`, antes do cálculo de versões:

```sql
QUALIFY row_number() OVER (
  PARTITION BY customer_id, valid_from
  ORDER BY valid_to DESC, _source_updated_at DESC NULLS LAST, xxhash64(<atributos>) DESC) = 1
```

- **Ordem do desempate:**
  1. **a vigência que termina mais tarde** (a aberta vence). Vem antes de `_source_updated_at` de propósito: prefere a versão que continua aberta, e o cliente mantém uma versão vigente, coerente com a fonte. **Custo:** uma correção mais nova que fechou a versão perde. A incoerência fica medida (abaixo) e vai para o time do ERP;
  2. **a mais atualizada no ERP;**
  3. **o hash dos atributos.** Ele só empata linhas de conteúdo igual, salvo colisão de 64 bits. **Exceção:** o `xxhash64` ignora os argumentos nulos. Então duas linhas com os mesmos valores deslocados em relação a um nulo (`a, NULL` × `NULL, a`) empatam sempre. Nesse caso, improvável, a ordem não é total.
- **`xxhash64` e `NULL`** (não documentado; testado no warehouse em 2026-10-28):
  - `xxhash64(NULL, 'a') = xxhash64('a', NULL) = xxhash64('a')`: o argumento nulo é ignorado. O hash só de `NULL` é a semente (42);
  - `xxhash64(1) <> xxhash64(CAST(1 AS BIGINT))`: o tipo muda o hash. As chaves usam tipos estáveis (`STRING` e `DATE`), o que evita a armadilha do `INT` × `BIGINT`;
  - **consequência para as chaves:** `xxhash64(id, valid_from)` depende de `valid_from` não nulo. O `id` tem `FAIL UPDATE`, mas `valid_interval` é só aviso. Duas versões do mesmo `id` com `valid_from` nulo caem na mesma partição do `one_per_start` (o `PARTITION BY` junta os nulos) e viram **uma** na dimensão. É uma **prevenção silenciosa**: uma versão some, e a `_key` não se repete. Quem detecta são as contagens 8 a 10 da `verify_uniqueness` (o `GROUP BY` também agrupa os nulos), e não a `_key` repetida (D7). Hoje há 0 nulos nos três cadastros.
- **Tipo 1 da mesma base:** os atributos tipo 1 da `dim_customer` e o nome da `dim_sales_rep` saem de `one_per_start`, pela linha de `valid_from` mais recente. O `max_by(sales_rep_name, valid_from)` saiu.
- **Quem detecta o quê:**

| Mecanismo | O que faz | Alerta? |
|---|---|---|
| `no_overlap_with_next_version` (aviso, silver) | Mede o caso no event log | Não: só mede |
| `verify_uniqueness`, versões repetidas `(id, valid_from)` na silver (D7) | Falha o job quando a silver tem o caso | Job vermelho, sem notificação até a fatia 2 (D8) |
| `verify_uniqueness`, `_key` única nas dimensões (D7) | Pega caminhos de duplicação não previstos, que a prevenção não cobre | Idem |

- **Por que prevenir e detectar:**
  - **a verificação roda depois que o `commercial` já atualizou e publicou os fatos** (D7). Com "só detectar", o dashboard mostraria vendas infladas, e o job terminaria FAILED, sem notificação. O SLA não cairia: cairia a correção do número, sem ninguém saber;
  - prevenir na dimensão evita que o número errado chegue ao consumidor;
  - detectar é o alarme para o caminho de duplicação não previsto. É defesa em profundidade.
- **Resolve no ADR-0011:**
  - a pendência "prevenir ou só detectar": os dois;
  - a divergência residual entre `is_latest` e o tipo 1: com um início por linha, a ordem por `valid_from` é total, e os dois saem da mesma linha;
  - a ordenação não total do tipo 1: o hash é o último critério (com a exceção do nulo, acima);
  - a diferença de desempate do nome na `dim_sales_rep`.
- **Prova só de leitura (2026-10-23), antes do refresh:**

| Conferência | `dim_customer` | `dim_sales_rep` |
|---|---|---|
| Linhas / chaves distintas | 47.167 / 47.167 | 506 / 506 |
| Checksum da versão nova = o da publicada | sim | — |
| `EXCEPT ALL` nos dois sentidos | 0 / 0 | 0 / 0 |

- **Na prova de reenvio:** com a silver de cadastros dobrada pelo `DISTINCT` (D5), as dimensões ficaram idênticas. A prevenção funcionou. A detecção ficou incompleta:
  - `no_overlap_with_next_version` contou 72.976 em cliente, 505 em hierarquia e 126 em produto, mas só mede;
  - a verificação da época, com 7 contagens, passou: com o `one_per_start`, a `_key` não se repete mais pelo mesmo início, e ela não via o caso. **Daí as três contagens novas do D7**, que teriam feito o job falhar ali.
- **Fora do D6: `dim_product`.** Ela já tem uma linha por `product_id` (ADR-0010 D12), mas a ordenação (`valid_from`, `valid_to`, `_source_updated_at`) não é total. Não recebeu o hash como último critério porque não há caso de mesmo início medido em produto. Fica como dívida (Consequências), com o gatilho "primeiro caso de produto com versões de mesmo início".

### D7. Verificação de unicidade: dez contagens que devem dar 0

[`src/jobs/verify_uniqueness.py`](../../src/jobs/verify_uniqueness.py) é a última tarefa do job. Entrega o ADR-0010 D13 e o amplia com a conservação de contagens (ADR-0010 D6) e com as versões repetidas na silver.

| # | Contagem | Tabela |
|---|---|---|
| 1 | Linhas a mais no grão `(order_id, order_item_number)` | `commercial.gold.fact_sales` |
| 2 | Linhas a mais no grão `(invoice_id, invoice_series, invoice_item_number)` | `commercial.gold.fact_invoice` |
| 3 | `customer_key` repetida | `core.gold.dim_customer` |
| 4 | `sales_rep_key` repetida | `core.gold.dim_sales_rep` |
| 5 | `product_key` repetida | `core.gold.dim_product` |
| 6 | Linhas do `fact_sales` − linhas de `silver.erp_order_item` | — |
| 7 | Linhas do `fact_invoice` − linhas de `silver.erp_invoice_item` | — |
| 8 | Versões repetidas `(customer_id, valid_from)` | `core.silver.erp_customer` |
| 9 | Versões repetidas `(product_id, valid_from)` | `core.silver.erp_product` |
| 10 | Versões repetidas `(sales_rep_id, valid_from)` | `core.silver.erp_sales_hierarchy` |

- **Por que as contagens 8 a 10:** com o `one_per_start` (D6), a dimensão nunca repete a `_key` pelo mesmo início, então as contagens 3 e 4 não veem mais esse caso. Antes, ele só era medido pela regra de aviso, sem alerta. Elas também pegam o reenvio que a silver não absorveu (D5).
  - **Medido em 2026-10-28, no warehouse:** 0 nas três.
  - **Ainda não rodaram no job:** o deploy está feito, e elas rodam na próxima execução.
- **Ação:** qualquer contagem diferente de 0 falha a tarefa e o job, com a lista das violações. A tarefa não corrige nada: a correção é na fonte ou no pipeline.
- **É um alarme depois da publicação, e não um portão.** A tarefa roda depois que os pipelines publicaram silver, dimensões e fatos. Qualquer duplicação que ela detecta já chegou ao consumidor quando o job falha.
- **Por que uma tarefa, e não expectations:** a expectation mede linha a linha. Duplicata é um problema de conjunto.
- **Por que Python, e não uma `sql_task`** (as convenções dizem "Python só onde SQL não resolver"):
  - a `sql_task` rodaria no único SQL warehouse 2X-Small, disputado com o dashboard;
  - a tarefa Python roda no compute serverless de jobs e imprime todas as violações antes de falhar.
- **Custo:** uma consulta só (`UNION ALL`). Na execução real, com 7 contagens, 35 s com o arranque.
- **Catálogos por parâmetro**, validados como identificador, nunca literais (convenções).

### D8. O job `serra_azul_daily`

| Tarefa | Tipo | Depende de | Retentativas | Timeout | Tempo medido (2026-10-27) |
|---|---|---|---|---|---|
| `simulate_erp_extraction` | `spark_python_task` | — | 1, com 60 s de intervalo | 1.800 s | 212 s |
| `core` | `pipeline_task` | simulador | 0 | 1.800 s | 183 s |
| `commercial` | `pipeline_task` | `core` | 0 | 1.200 s | 102 s |
| `verify_uniqueness` | `spark_python_task` | `commercial` | 1, com 60 s de intervalo | 600 s | 35 s |

- **Agendamento:** cron `0 0 5 * * ?` em `America/Sao_Paulo` (o `timezone_id` é obrigatório), **pausado** (ADR-0005). Execução à mão com `databricks bundle run serra_azul_daily`.
- **`max_concurrent_runs: 1`:** duas execuções juntas gravariam a mesma extração e disputariam os pipelines.
- **Timeouts:** de ~8 a ~17 vezes o tempo medido, com folga para um *catch-up* de várias extrações.
- **Retentativas (emenda ao ADR-0005, que previa 1 por tarefa):**
  - **simulador, 1:** ele é idempotente (D3, com a ressalva do arquivo truncado), e uma falha de arranque do serverless é transitória;
  - **pipelines, 0:** os pipelines já têm `pipelines.numUpdateRetryAttempts: "1"` e `pipelines.maxFlowRetryAttempts: "1"` no bundle. A tarefa fica com 0: retentar aqui também multiplicaria as tentativas do update pelas da tarefa (detalhes abaixo);
  - **verificação, 1:** uma violação é determinística e falha de novo em ~35 s, o que custa pouco. Uma falha passageira na partida da tarefa, sem a retentativa, deixaria o job FAILED sem achado nenhum. A retentativa separa violação de falha de infraestrutura;
  - `min_retry_interval_millis` (60 s) conta a partir do início da execução que falhou ([Jobs API](https://docs.databricks.com/api/workspace/jobs/create)).
- **Timeout não é retentado, de propósito:** `retry_on_timeout` vem `false` por padrão, então o `max_retries` **não** vale quando a tarefa estoura `timeout_seconds` ([Jobs API](https://docs.databricks.com/api/workspace/jobs/create)).
  - **Decisão: manter assim no simulador.** Se ele estourar o tempo no meio de uma cópia, uma retentativa veria o arquivo truncado como "extração existente" e terminaria com SUCCESS, empurrando a falha para o `core`. Falhar o job por timeout deixa o problema visível.
- **Retentativa interna do pipeline** ([propriedades](https://docs.databricks.com/aws/en/ldp/properties) e [updates](https://docs.databricks.com/aws/en/ldp/updates) de pipeline):
  - `pipelines.numUpdateRetryAttempts` repete o update em falha "*retryable*". O padrão é 5 para pipelines *triggered*. A retentativa roda como *full update*, atualizando todas as tabelas. Não é *full refresh* (*reset*): a bronze continua protegida por `pipelines.reset.allowed = false`;
  - `pipelines.maxFlowRetryAttempts` repete o fluxo dentro do update. O padrão é 2; o valor "1" do projeto dá 2 execuções do fluxo;
  - **vale para updates disparados por Jobs ou pela API**, o caso do `pipeline_task`. Não vale para updates *ad-hoc* do editor ou da interface nem para o *validate*. O que decide é como o update foi disparado, e não o modo produção. Os pipelines do bundle, sem `mode`, ficam em `development: false`, o padrão da API ([modos de deployment](https://docs.databricks.com/aws/en/dev-tools/bundles/deployment-modes));
  - **não documentado:** quais falhas são "*retryable*" (o único exemplo é "*failure to start a cluster*"); se o `RESOURCE_EXHAUSTED` por cota entra; o intervalo e o *backoff*; e se os dois contadores se multiplicam;
  - **custo de uma falha:** até (1 + 1) × (1 + 1) execuções de um fluxo, pela leitura da documentação, que não define a interação. Uma retentativa de tarefa multiplicaria as tentativas do update pelas da tarefa: esse número dobraria.
- **Observação pedida pelo ADR-0010 (`RESOURCE_EXHAUSTED`):** nos 7 updates de 2026-10-27 e 2026-10-28 houve 0 `RESOURCE_EXHAUSTED`, 0 retentativas e 0 eventos de erro:
  - `core`: `0ebd1bd3`, `3e9fdd9c`, `2ed71db9` (*validate-only*) e `20126340`;
  - `commercial`: `55aace92`, `8455a170` e `7b10a440`.
- **Parâmetros do job:**

| Parâmetro | Padrão | Uso |
|---|---|---|
| `extraction_date` | vazio = hoje em `America/Sao_Paulo` | Data alvo. Explícito para recarregar um dia já gravado (D3) |
| `output_root` | vazio = `/Volumes/<catalog_core>/landing` | Outra raiz para ensaio, numa pasta que nenhuma bronze lê |
| `resend` | `false` | `true` grava o reenvio da extração (D4) |

- Cada parâmetro vai como opção e valor num token só (`--extraction-date={{job.parameters.extraction_date}}`): com o valor vazio, o argumento chega como `""` e não some da lista.
- **Sem e-mail de falha:** **não confirmado** no Free Edition. A página de limitações não cita notificações (pendência "a verificar" do ADR-0005). Um job vermelho fica sem aviso até a fatia 2.
  - **Nuance documentada** ([notificações de job](https://docs.databricks.com/aws/en/jobs/notifications)): "*Job-level notifications aren't sent when failed tasks are retried*". Com retentativa na tarefa, a notificação de job só sai na falha final.
  - **Testes que ficam para a fatia 2 (observabilidade):** um teste sem custo (abrir a seção de notificações do job na interface e salvar) e um teste real (forçar uma falha trivial).

### D9. Faker com versão exata: `faker==37.12.0`

- Nos dois jobs: `erp_initial_load` e `serra_azul_daily`.
- **Por quê:** versões diferentes do Faker geram valores diferentes para a mesma semente. Como a extração full regera os cadastros inteiros todo dia (nomes, endereços e documentos), outra versão mudaria o cadastro a cada extração e quebraria o determinismo do D3.
- **A versão:** a dependência da carga inicial era `faker>=33,<38`. Pelas datas do PyPI, essa faixa resolveu para a 37.12.0, a última abaixo da 38. O ensaio 2 confirmou a reprodução: a extração de 2026-10-19 saiu idêntica ao recorte do dia da carga nos 7 datasets, inclusive nos cadastros, comparados inteiros, com dados do Faker.
- **O determinismo depende também do ambiente serverless** (`client: "4"` nos dois jobs).

### D10. A regra do faturamento parcial depende da data da 1ª NF

- **Contexto:** o ADR-0008 achou 16.285 itens de NF com quantidade 0 na carga inicial. No faturamento parcial de um item de 2 unidades, a 1ª NF levava `ceil(0,6 × 2) = 2`, e a 2ª ficava com 0. O gerador foi corrigido para `least(q − 1, ceil(0,6 q))`.
- **Problema achado no ensaio 1:** aplicada a todos os pedidos, a regra corrigida mudava a história. Os pedidos parciais com a 1ª NF até 2026-10-18 já estão na landing, com as 2 unidades na 1ª NF. A 2ª NF deles, pela regra nova, levaria mais 1: **3 unidades faturadas num item de 2**.
- **Escolha:** constante `QTY1_ORIGINAL_RULE_UNTIL = date(2026, 10, 18)` em `build_invoices`.
  - 1ª NF depois de 2026-10-18: regra corrigida;
  - 1ª NF até 2026-10-18: regra original. A landing é a verdade do ERP, e a extração diária continua a história dela.
- **Efeito esperado e medido:** 71 itens de quantidade 0 (54 pedidos, todos de venda) nas extrações de 2026-10-20 a 2026-10-22. São a 2ª NF dos pedidos parciais que já estavam na carga. A silver os descarta por `positive_quantity` (`DROP ROW`); o event log de 2026-10-27 confirma os 71.
- **Depois de 2026-10-22, nenhum:** a 2ª NF sai de 1 a 3 dias depois da 1ª, e a última 1ª NF com a regra original é de 2026-10-18.
- **Refina:**
  - o ADR-0008 ("o gerador foi corrigido para as próximas execuções");
  - o ADR-0010 ("com o gerador corrigido, as próximas cargas devem coincidir").
  - Nos dois, a correção vale para os pedidos cuja 1ª NF sai depois de 2026-10-18, e não para toda extração depois da carga.

## PII e LGPD

- **Direito ao esquecimento:** exige regravar ou apagar arquivos da landing que contêm o titular. É uma **exceção documentada** ao "nunca regravar" do D4.
  - Com `allowOverwrites = false`, o arquivo já processado e regravado no mesmo caminho não é reingerido (D4). Aqui isso é bom: o arquivo regravado sem o titular não volta à bronze.
  - Também é preciso apagar na bronze e rodar `VACUUM` ([caso de negócio](../caso-de-negocio.md) §6, ADR-0004).
  - **A fonte precisa parar de enviar o titular.** A extração full diária de `clientes_pdv` traz o cadastro inteiro: o titular apagado volta na extração seguinte, para a landing, a bronze, a silver e a dimensão.
    - **Em produção:** apagamento ou anonimização no ERP, ou cláusula no contrato da integração.
    - **No simulador:** não implementado. Gatilho: o primeiro pedido de esquecimento, ou a fatia 5 (governança).
  - **Ressalva ao ADR-0010 D2** (aceito, por isso registrada só aqui): "o cliente esquecido fica em `-1` (a) para sempre" só vale **desde que a fonte pare de reenviar o titular**.
- **Cópias de PII:** a extração full diária cria **uma cópia da PII de cada cliente por dia**, na landing e na bronze. É o contrário da minimização do ADR-0004.
  - Só a retenção do ADR-0007 limita essas cópias, e ela não está implementada (Consequências). Isso aumenta a prioridade da dívida de retenção.
- **Pasta `_work`:** se a tarefa do simulador falhar, os intermediários com PII ficam em `{raiz}/erp/_work` até a próxima execução que **grava alguma extração** e apaga a pasta no fim. Uma execução sem nada a gravar retorna antes da limpeza. **Risco aceito.**

## Resultados

### Método da prova

- **Antes e depois de cada execução:** contagem de linhas e checksum `sum(xxhash64(*) % 1000000007)` das colunas de negócio de cada tabela.
- **Fora do checksum:**
  - `_source_file` e `_ingested_at`, na silver de fatos: mudam a cada reenvio;
  - `_updated_at`, nos fatos. Num reenvio, o `AUTO CDC` troca o `_ingested_at` das linhas reenviadas e muda o `_updated_at` delas, mesmo sem mudança de conteúdo. **Consequência:** um consumidor incremental guiado pelo `_updated_at` relê essas linhas.
  - **Prova mais forte possível, não medida:** numa recarga sem dado novo, o `_updated_at` não deveria mudar. Incluí-lo no checksum provaria o determinismo também nessa coluna.
- **Comparação do simulador com a carga inicial (ensaios):** a extração de 2026-10-19 gerada pelo ensaio é comparada linha a linha (`EXCEPT ALL` nos dois sentidos) com o **recorte do dia 2026-10-18** da carga, e não com a carga inteira. O recorte é o que uma extração diária de 2026-10-19 traria:

| Dataset | Recorte |
|---|---|
| `pedidos_cab` | Pedidos cuja última `dt_atualizacao` é de 2026-10-18 |
| `pedidos_itens` | Itens dos pedidos **criados** (`dt_pedido`) em 2026-10-18, coerente com o D1 |
| `notas_fiscais`, `notas_fiscais_itens` | NFs com `dt_emissao` = 2026-10-18, e os itens dessas NFs |
| cadastros | A extração inteira |

### Ensaio 1 (2026-10-23, run `120935294350724`)

- Pasta de rascunho `_dryrun`, extração 2026-10-23: 5 extrações (2026-10-19 a 2026-10-23) em 242 s.
- Numeração: 0 buracos.
- Extração de 2026-10-19 × recorte do dia 2026-10-18 da carga:
  - **idênticos:** `pedidos_cab`, `pedidos_itens`, `clientes_pdv`, `produtos` e `hierarquia_comercial`;
  - **`notas_fiscais`:** 15 diferenças, todas de pedidos `PARCIALMENTE FATURADO` com item de 2 unidades. Deram origem ao D10;
  - **`notas_fiscais_itens`:** sem resultado registrado. A primeira versão da comparação deu erro interno do otimizador e teve de ser reescrita.

### Ensaio 2 (2026-10-27, run `319883143278742`)

- Pasta `_dryrun`, extração 2026-10-24: 6 extrações, 42 arquivos, ~4,5 min.

| Conferência | Resultado |
|---|---|
| Extração de 2026-10-19 × recorte do dia 2026-10-18 da carga | Idêntica nos 7 datasets (0 diferenças) |
| Quantidade faturada acima da pedida | 0 entre 2.913.332 itens de pedido com NF de venda (carga + ensaio) |
| Numeração | Sem buraco: pedidos 805154 a 817326; NFs 100838958 a 100849644 |
| Sujeiras, nas taxas da carga | Duplicata exata 1,55% a 2,04%; quase-duplicata ~0,3%; item negativo ~0,1%; centavos ~3% |
| Clientes órfãos | 113 chegaram até 2026-10-24; nenhum cliente da carga some |

- A pasta de rascunho foi apagada depois.

### Execução real (2026-10-27, run `531578600911479`)

- **SUCCESS em ~9 min:** simulador 212 s, `core` 183 s, `commercial` 102 s, verificação 35 s.
- **8 extrações** (2026-10-20 a 2026-10-27, 56 arquivos). Era a primeira execução depois da carga: o catch-up gravou os 8 dias de uma vez (D3). Contagem por extração no [dicionário](../dicionario-de-dados-erp.md) §5.

**Bronze: cresceu exatamente o que foi gravado.**

| Tabela da bronze | Linhas novas |
|---|---|
| `erp_order_header` | +24.469 |
| `erp_order_item` | +56.003 |
| `erp_invoice_header` | +14.667 |
| `erp_invoice_item` | +52.704 |
| `erp_customer` | +582.638 |
| `erp_product` | +1.008 |
| `erp_sales_hierarchy` | +4.040 |

**Silver e gold:**

| Conferência | Antes | Depois |
|---|---|---|
| `silver.erp_invoice_item` | — | +52.633 (71 a menos que a bronze: `positive_quantity`, D10) |
| `fact_sales` (linhas) | 2.931.839 | 2.987.790 |
| `fact_invoice` (linhas) | 3.017.600 | 3.070.233 |
| Data de negócio mais recente | 2026-10-18 | 2026-10-26 (D-1) |
| Cliente `-1` no `fact_sales` | 1.070 | 254 |
| Cliente `-1` no `fact_invoice` | 1.084 | 256 |
| `dim_customer` (linhas, com o `-1`) | 47.167 | 47.387 |
| `dim_sales_rep` e `dim_product` | — | checksum inalterado |
| Verificação de unicidade (7 contagens na época) | — | 7 zeros |

- **Cliente `-1`:** todos os que sobraram são de cliente ainda não cadastrado (caso (a) do ADR-0010 D2). Na extração de 2026-10-27 havia 39.962 clientes, contra 39.773 na carga: **189 dos 227 órfãos** já tinham chegado. Os 38 restantes chegam até 2026-10-29.
- **`fact_sales`:** cresceu 55.951 linhas, 52 a menos que a bronze de itens. São os itens negativos descartados por `positive_quantity_and_amount`, confirmados no event log (abaixo).

**Expectations no event log** (update `0ebd1bd3` do `core` e `55aace92` do `commercial`; soma de todos os lotes de cada fluxo):

| Pipeline | Fluxo / tabela | Regra | Resultado |
|---|---|---|---|
| `core` | `erp_invoice_item_cdc` | `positive_quantity` (`DROP ROW`) | 71 descartadas, 52.633 aprovadas (D10) |
| `core` | `erp_order_item_cdc` | `positive_quantity_and_amount` (`DROP ROW`) | 52 descartadas, 55.951 aprovadas |
| `core` | `erp_customer` | `document_valid` (aviso) | 1.338 avisos em 72.976 versões |
| `core` | `erp_customer`, `erp_product`, `erp_sales_hierarchy` | `no_overlap_with_next_version`, `no_gap_to_next_version` (aviso) | 0 nos três cadastros |
| `commercial` | `fact_sales` / `fact_invoice` | `customer_id_in_dimension` (aviso) | 254 / 256 |
| `core` e `commercial` | todas as outras | — | 0 |

- É a primeira medição no event log das regras que entraram ou mudaram em 2026-10-21 (ADR-0010 D11 e D14). As de aviso deram 0. As duas de descarte, `positive_quantity` (71) e `positive_quantity_and_amount` (52), contaram a sujeira esperada.
- As contagens de `no_overlap_with_next_version` do D5 e do D6 (72.976, 505 e 126) são do **reenvio** (update `3e9fdd9c`), e não desta execução.

### Reenvio (2026-10-27, run `920924158637651`): o D5 falhou

- Comando: `databricks bundle run serra_azul_daily --params extraction_date=2026-10-27,resend=true`. O simulador registrou "extração 2026-10-27 (dados de 2026-10-26, reenvio)".
- **Bronze:** +90.913 linhas. Em cada um dos 7 datasets, as linhas do reenvio = as linhas do original.
- **Silver de fatos, dimensões e fatos:** linhas e checksum idênticos aos de antes do reenvio.
- **Silver de cadastros: dobrou.** É o achado do D5. `no_overlap_with_next_version` contou 72.976 em cliente, 505 em hierarquia e 126 em produto.
- **Verificação (7 contagens na época): 7 zeros.** Ela não viu a silver dobrada, o que motivou as contagens 8 a 10 do D7.

### Reparo e recarga do mesmo dia (2026-10-28, run `678916590987444`)

- Depois da troca de `DISTINCT` por `GROUP BY ALL` (D5). Comando: `databricks bundle run serra_azul_daily --params extraction_date=2026-10-27`.
- **Simulador:** "nada a gravar: as extrações até 2026-10-27 já existem".
- **Bronze:** idêntica, nenhuma linha nova. **A recarga do mesmo dia não duplica.**
- **As 14 tabelas de silver e gold:** linhas e checksum idênticos aos de antes do reenvio. A silver de cadastros foi recalculada sobre as duas cópias que já estavam na bronze: o reenvio passou a ser absorvido.
- **Verificação:** 7 zeros.
- **O que esta sequência prova:** a recarga não muda nada, e o reenvio é absorvido **depois da correção do D5**, sobre as mesmas cópias. Um reenvio novo, de ponta a ponta, com o `GROUP BY ALL` e as 10 contagens, ainda não foi executado. Ele precisa ser de outra data (D3).

## Alternativas consideradas

| Alternativa | Por que não |
|---|---|
| D1: notebook, como previa o ADR-0005 | O simulador importa o gerador da carga (módulo irmão) e recebe parâmetros por `argparse`, o mesmo padrão do job de carga inicial. Diff legível não é o motivo: um notebook em formato *source* também o teria |
| D2: gerador diário independente | Quebraria a continuidade: os pedidos da carga não mudariam de status (faturamento, cancelamento, devolução), e a numeração recomeçaria ou teria buraco |
| D2: regerar a história inteira todo dia | 18 meses de fatos por execução, na cota. Hoje a janela é ~10% disso. **Com o tempo, a janela converge para essa alternativa** (D2, gatilho para reancorar) |
| D2: gerar o futuro de uma vez | Arquivos "do futuro" na landing: a bronze leria vendas de dias que ainda não aconteceram |
| D2: janela ancorada em D, levando a numeração da extração anterior | O simulador passaria a depender do estado das extrações anteriores (a contagem de pedidos e NFs já numerados) e deixaria de ser "função só da data" (D3). Um erro numa extração contaminaria as seguintes. Hoje a contagem anterior à janela vem dos manifests da carga, e por isso a janela começa no fim da carga |
| D3: preencher buraco no meio da série | YAGNI. O caso real é o job que não rodou, e o catch-up a partir da última extração o cobre |
| D4: sobrescrever o arquivo da data | Com `allowOverwrites = false`, o arquivo já processado e regravado no mesmo caminho não é reingerido: a correção nunca chegaria à bronze |
| D4: `cloudFiles.allowOverwrites = true` | O arquivo sobrescrito seria ingerido de novo (comportamento exato com `true` a confirmar), e a bronze *append-only* guardaria as duas versões. O que se perde é o arquivo original na landing e a distinção por `_source_file`: as duas cópias teriam o mesmo caminho |
| D4: nomes `part-<uuid>` do Spark, como na carga inicial | O nome muda a cada gravação: regravar a mesma extração vira arquivos novos, e a bronze lê tudo de novo. É o risco da carga inicial rodada de novo |
| D5: manter `SELECT DISTINCT * EXCEPT (...)` | Dobrou a silver de cadastros no pipeline (achado da prova), embora desse certo no warehouse |
| D5: "última chegada" por `_ingested_at` | Trataria o reenvio corrigido, mas duplica se o original e o reenvio chegarem na mesma atualização do pipeline: os dois têm o mesmo `_ingested_at` |
| D5: "último arquivo da data" (variante da "última chegada") | A carga inicial tem vários `part-*.parquet` por extração. E, se o original e o reenvio chegarem na mesma atualização, as duas cópias entram. O `GROUP BY ALL` cobre os dois casos |
| D5: `MERGE` ou `AUTO CDC` por `(id, valid_from)` na silver de cadastros | A extração full é uma foto: uma versão que some da fonte precisa sumir da silver, e um *upsert* a partir de um fluxo *append-only* não apaga. O ADR-0008 já rejeitou `AUTO CDC FROM SNAPSHOT`, que dataria as versões pela extração |
| D5: lista explícita de colunas no `GROUP BY` | Verbosa, e quebra com evolução de schema da fonte |
| D5: deduplicar na bronze | A bronze deixaria de ser fiel (ADR-0008) |
| D6: só detectar (a verificação falha o job) | A verificação roda depois da publicação: o dashboard mostraria vendas infladas, e o job terminaria FAILED sem notificação. O SLA não cairia; cairia a correção do número, sem ninguém saber |
| D6: só prevenir | A dimensão esconderia o erro da fonte. Sem a verificação, um caminho de duplicação não previsto passaria em silêncio |
| D6: `FAIL UPDATE` na silver | Pararia a carga diária por um problema de cadastro de uma entidade (ADR-0011) |
| D6/D7: verificar antes de publicar (*write-audit-publish*, WAP) | No SDP, cada materialized view publica ao terminar o refresh. Para ter WAP, seria preciso gravar os fatos numa área de *staging*, verificar e promover (views do consumidor apontando para a última versão auditada, ou *clone*/troca). Isso duplica armazenamento e compute da gold, cria mais uma etapa e pesa na cota do Free Edition: YAGNI num projeto de estudo. **Em produção, é o caminho recomendado** (ver "Em produção") |
| D7: expectations nas dimensões e nos fatos | Medem linha a linha; duplicata é problema de conjunto |
| D7: materialized view de validação com expectation sobre um agregado (`count(*)` por chave com `EXPECT (num_entries = 1)`) dentro do pipeline, padrão documentado em [expectation patterns](https://docs.databricks.com/aws/en/ldp/expectation-patterns) | A própria página diz: "*A validation table doesn't gate its downstream tables.*" Também não é portão: um `FAIL UPDATE` desfaz só a transação da tabela que falhou, e as outras ficam publicadas (inferência a partir de [expectations](https://docs.databricks.com/aws/en/ldp/expectations)). Recalcula uma materialized view a mais por execução. E acopla invariantes entre pipelines (fato do `commercial` × silver e dimensões do `core`) a um dos pipelines. A tarefa separada é uma checagem independente e aparece como uma tarefa vermelha própria |
| D7: `sql_task` em vez de Python | Rodaria no único SQL warehouse 2X-Small, disputado com o dashboard |
| D7: a verificação corrige o que achar | Esconderia o defeito. A correção é na fonte ou no pipeline |
| D8: 1 retentativa por tarefa (ADR-0005) | Nos pipelines, multiplicaria as tentativas do update pelas da tarefa numa falha determinística, na cota |
| D8: 0 retentativa na verificação | Uma falha passageira na partida deixaria o job FAILED sem achado nenhum |
| D8: e-mail de falha | Entrega não confirmada no Free Edition |
| D8: agendamento ativo | Consumiria a cota nos dias sem estudo (ADR-0005) |
| D9: Faker sem versão fixa | Outra versão gera outros nomes e endereços para a mesma semente: o cadastro full mudaria a cada extração |
| D10: regra corrigida para todos os pedidos | 3 unidades faturadas num item de 2, nos pedidos parciais que já estavam na carga |
| D10: regra original para todos | Manteria o item de quantidade 0 em toda extração futura |

## Consequências

- **Positivas:**
  - o D-1 sai com um comando, e a ordem `core` → `commercial` é garantida pelo job;
  - a recarga do mesmo dia não muda nada, provado por checksum. O reenvio é absorvido depois da correção do D5, sobre as mesmas cópias;
  - a unicidade e as versões repetidas na silver são verificadas a cada execução, com o job vermelho quando falham;
  - o `-1` de cadastro atrasado se corrige sozinho na recomputação (1.070 → 254 no `fact_sales`);
  - qualquer extração pode ser regerada igual, numa pasta de ensaio, para investigar um problema;
  - mesmo início na `dim_customer` e na `dim_sales_rep`: prevenido, medido e verificado.
- **Negativas / dívidas assumidas:**
  - **a verificação é um alarme depois da publicação, e não um portão:** qualquer duplicação que ela detecta já chegou ao consumidor quando o job falha, e o job vermelho fica sem notificação até a fatia 2;
  - **a janela do simulador cresce um dia por dia de calendário** (53 dias em 2026-10-27). Gatilho para reancorar: 180 dias ou 10 min (D2);
  - **as sujeiras de cadastro acabam:** os órfãos chegam até 2026-10-29. Depois disso, as extrações diárias não trazem cadastro novo nem versão nova;
  - **arquivo truncado** conta como extração existente. O reparo é manual, com procedimento para o timeout e para o meio da série (ver Perguntas difíceis). Risco aceito. Mitigação proposta, não implementada: conferir o rodapé `PAR1` antes de considerar a extração existente, com o gatilho "antes de ativar o agendamento, ou na primeira ocorrência";
  - **as contagens 8 a 10 e o job vermelho:**
    - um caso real de mesmo início vindo do ERP fica em toda extração full seguinte. Ele deixaria o job vermelho **todo dia** até o ERP corrigir, embora a dimensão já o contenha (D6);
    - o vermelho deixaria de distinguir "número publicado errado" (contagens 1 a 7) de "fonte incoerente, contida" (8 a 10), e mascararia a próxima violação real;
    - **hoje é impossível no projeto:** 0 casos, e o simulador não cria versões novas;
    - **saída, não implementada (YAGNI):** separar a severidade. Cópias idênticas, salvo `_next_valid_from` (o sintoma do D5, defeito da silver), falham o job. Mesmo início com atributos diferentes (defeito da fonte) vira aviso impresso;
    - **gatilho:** quando o simulador passar a gerar versões de cadastro, ou na fatia 2, junto com a notificação;
  - **`dim_product`:** a ordenação de desempate (`valid_from`, `valid_to`, `_source_updated_at`) não é total. Num empate dos três, a linha escolhida pode variar entre recálculos. Dívida, com o gatilho "primeiro caso de produto com versões de mesmo início";
  - **`xxhash64` com nulo** (testado, D6): o argumento nulo é ignorado. O último critério do D6 não garante ordem total num caso improvável. E duas versões do mesmo `id` com `valid_from` nulo viram uma na dimensão, em silêncio; só as contagens 8 a 10 da verificação as pegam (hoje 0);
  - **esquecimento (LGPD) depende da fonte:** a extração full traz o titular de volta todo dia até o ERP parar de enviá-lo. No simulador, não implementado (ver PII e LGPD);
  - **causa do `DISTINCT` não confirmada** (D5). O padrão `SELECT DISTINCT * EXCEPT (...)` em materialized view fica sob suspeita no projeto até a causa aparecer. O `GROUP BY ALL` é uma correção empírica, exposta a `channel: CURRENT` e a colunas técnicas novas na bronze;
  - **reenvio corrigido da mesma data não tratado** (D5);
  - **sem e-mail de falha** (D8);
  - **pipelines sem retentativa de tarefa:** um `RESOURCE_EXHAUSTED` depende só da retentativa interna do pipeline, e não é documentado se ela cobre esse erro (0 casos em 7 updates);
  - **timeout do simulador não é retentado** (D8): uma demora passageira falha o job inteiro. É o preço de não esconder um arquivo truncado;
  - **retenção do ADR-0007 sem rotina no código:**
    - a bronze de cadastros cresce ~73 mil linhas de cliente por extração (+582.638 em 8), cada uma com uma cópia da PII (ver PII e LGPD);
    - a landing também não tem rotina e recebe 7 arquivos por dia, um deles com PII;
    - a retenção de 90 extrações limitaria a bronze de clientes a ~6,6 mi de linhas;
    - a dívida está no [roadmap](../roadmap.md) (fatia 2): implementar antes da 90ª extração diária, por volta de 2027-01-17. **O gatilho é só uma data, sem proteção observável,** e o *catch-up* faz a data chegar pelo calendário, mesmo sem execuções;
  - **carga inicial rodada de novo:** a proteção é só um comentário no job. Além da bronze lida de novo, `silver.ref_postal_code` usa `max(_extraction_date)` sem juntar linhas iguais. A referência duplicada multiplicaria a `silver.erp_customer` no `LEFT JOIN`. As contagens 8 a 10 do D7 pegariam isso, depois da publicação;
  - **calendário do simulador até 2027.** Gatilho: revisar antes de 2027-10.
- **O que nos faria revisitar:**
  - a janela passar de 180 dias ou o simulador passar de 10 min (reancorar, D2);
  - a verificação acusar uma violação;
  - `no_overlap_with_next_version` contar casos fora de um reenvio;
  - um reenvio corrigido aparecer (é o tema da fatia 3, com o reenvio de mês inteiro do sell-out);
  - a confirmação de notificação por e-mail no Free Edition;
  - a causa do `DISTINCT` aparecer (documentação, nota de versão ou perfil de consulta);
  - o primeiro caso de produto com versões de mesmo início;
  - a fatia 2 precisar de cadastro que muda depois do go-live.

## Perguntas difíceis

- **"E se o job não rodar por 3 dias?"** Na próxima execução, o catch-up grava as 3 extrações que faltam, em ordem (D3). A bronze lê as três, a silver de fatos faz o *upsert* e os cadastros usam a mais recente. Aconteceu: a primeira execução real gravou 8 dias de uma vez (2026-10-20 a 2026-10-27). Nesses dias o D-1 ficou sem atualizar, **sem aviso**: detectar fonte que não chegou é o tema da fatia 2.
- **"E se o ERP reenviar a extração?"** Em duas etapas:
  - **reenvio de 2026-10-27:** a bronze guardou as duas cópias (+90.913 linhas). Silver de fatos, dimensões e fatos ficaram idênticos. A silver de cadastros **dobrou** (o D5 falhou com o `DISTINCT`), e a verificação da época não viu;
  - **reparo de 2026-10-28:** com o `GROUP BY ALL`, a silver de cadastros foi recalculada sobre as mesmas cópias, e as 14 tabelas voltaram a linhas e checksum idênticos aos de antes do reenvio. Hoje, as contagens 8 a 10 do D7 fariam o job falhar num caso como o primeiro.
- **"E se o arquivo chegar truncado?"** Risco aceito, sem arquivo-marcador (YAGNI no simulador). Análise do código e da documentação, não testada:
  - **causa mais provável:** interromper o `shutil.copyfile`, por exemplo pelo timeout de 1.800 s num *catch-up* longo. A documentação de volumes não fala de escrita parcial (D4);
  - **por timeout:** não há retentativa (`retry_on_timeout = false`, D8). O job falha, e o problema fica visível. O simulador grava em ordem de data e, dentro dela, de dataset, e só imprime "extração E (...)" depois de gravar todos os datasets de E. Então os arquivos incompletos estão todos na **extração seguinte à última impressa no log**, e o `core` nunca os leu, porque o job parou no simulador;
  - **procedimento depois de um timeout:** antes de rodar de novo, apagar os arquivos dessa extração incompleta, em todos os datasets. É seguro e não contraria o D4, porque esses arquivos nunca foram processados;
  - **por outra falha, ou se o operador rodar de novo sem apagar:** `has_extraction` trata qualquer `.parquet` na pasta como extração existente. A retentativa, ou a execução seguinte, termina com SUCCESS, grava as extrações seguintes, e o arquivo truncado passa a ficar **no meio da série**;
  - **no `core`:** `ignoreCorruptFiles` vem `false` por padrão. Pela leitura da documentação, o Parquet truncado faz o update falhar (inferência: a documentação só descreve o `true`);
  - **procedimento para o meio da série:** apagar o arquivo abriria um buraco que o D3 não preenche de propósito. Em vez disso, regerar aquela extração numa pasta de ensaio (`output_root` + `extraction_date`), que sai igual pelo D3, e copiar o arquivo por cima do truncado. Como o lote que leu o truncado falhou, pelo checkpoint o Auto Loader provavelmente tenta o arquivo de novo (inferência; o caso do lote que falhou não é documentado). É a única regravação de arquivo da landing fora do esquecimento (D4);
  - **mitigação proposta, não implementada:** antes de considerar uma extração existente, conferir o rodapé do Parquet (os 4 bytes finais `PAR1`). Isso transforma o arquivo truncado numa falha visível no simulador, antes do `core`. **Gatilho:** antes de ativar o agendamento, ou na primeira ocorrência;
  - **em produção:** manifesto ou `_SUCCESS` por extração.
- **"E se rodarem a carga inicial de novo?"** Na landing real, os nomes `part-<uuid>` seriam novos, e a bronze leria a carga inteira outra vez, sem *full refresh* para desfazer (ADR-0007). A referência de CEP duplicada multiplicaria a silver de clientes (Consequências). A proteção é só o comentário no job `erp_initial_load` (D4). Para testar o gerador, `--output-root` numa pasta de ensaio.
- **"E se o simulador cair no meio de uma data?"** A retentativa, ou a execução seguinte, recomeça da menor entre as últimas extrações de cada dataset e completa só os datasets que faltam (D3). Se a queda deixou um arquivo truncado, vale a resposta anterior.
- **"E se duas execuções rodarem juntas?"** `max_concurrent_runs: 1` impede (D8).
- **"E se um cliente pedir para ser esquecido?"** Apagar na landing, na bronze (com `VACUUM`) não basta: a extração full do dia seguinte traz o titular de volta. A fonte precisa parar de enviá-lo (apagamento ou anonimização no ERP, ou cláusula da integração). No simulador, não está implementado (ver PII e LGPD).
- **"Por que não confiar só na verificação, sem o `one_per_start`?"** Porque a verificação roda depois da publicação. Com "só detectar", o dashboard mostraria vendas infladas, e o job terminaria vermelho, sem notificação. O SLA não cairia, mas o número estaria errado sem ninguém saber (D6).
- **"Qual o custo em 10x?"** Extrapolação, não medição.
  - **Simulador:** a janela cresce com o tempo e com o volume. Em produção, ele não existe: o ERP entrega os arquivos.
  - **Pipelines:** o ADR-0010 estima de 5 a 10 min de processamento para ~60 mi de linhas nos fatos. Dos ~9 min da execução real, boa parte é arranque, que não cresce com o volume.
  - **Cadastros full:** ~730 mil versões de cliente por extração. Com 90 extrações retidas (ADR-0007, rotina ainda a implementar), ~66 mi de linhas na bronze de cliente. Ainda cabe, mas a extração full começa a pesar.

## Em produção faríamos diferente?

Sim:
- **O simulador não existe.** O ERP, ou uma ferramenta de integração, entrega os arquivos. O contrato de entrega define nome, formato, horário e marcador.
- **Verificação como portão (*write-audit-publish*):** é o caminho recomendado para os fatos comerciais do SLA das 8h. Os fatos são gravados numa área de *staging*, verificados e só então promovidos. O dashboard lê views sobre a última versão auditada.
- **Arquivo-marcador por extração:** `_SUCCESS` ou manifesto com contagem e checksum. A bronze só lê extração completa, e um arquivo truncado vira alerta, não falha no meio do pipeline. (Escrita em nome temporário seguida de renomeação também serve, onde a renomeação for atômica; em volumes, a atomicidade de `os.rename` não é documentada.) O `_SUCCESS` não seria ingerido como dado: o Auto Loader ignora por padrão nomes que começam com `_` (Runtime 19+).
- **Retentativas:** sem cota e com agendamento ativo, retentativa com *backoff* em todas as tarefas, para falha transitória, e a retentativa interna do pipeline no padrão.
- **Notificação de falha** (e-mail ou chat) e **alerta de *freshness*** quando a extração não chegar até um horário.
- **Quarentena** para extração inválida, com reprocesso quando a fonte corrigir (fatia 3).
- **Reenvio corrigido** tratado por contrato: cada reenvio com versão ou carimbo, e a silver fica com a última versão da extração.
- **Retenção e minimização:** rotina de retenção desde o primeiro dia, e cadastro por CDC em vez de extração full com PII diária. O esquecimento começa na fonte: apagamento ou anonimização no ERP, com cláusula no contrato da integração.
- **Verificação de unicidade também no CI**, antes do deploy (ADR-0011), além do job.
- **Agendamento ativo**, com *service principal* como identidade do job (ADR-0005).

**Em 10x** (~60 mi de linhas nos fatos, ~730 mil versões de cliente): a arquitetura do job se mantém. O que pesa é a extração full dos cadastros, que cresce na bronze a cada dia.

**Em 100x** (~600 mi nos fatos, ~7 mi de versões de cliente):
- a extração full diária de cadastro deixa de ser viável. Os cadastros passam a vir por CDC da fonte, e os fatos precisam de refresh incremental (ADR-0008, ADR-0010);
- a verificação de unicidade também fica cara. Olhar só as partições tocadas não basta: não pega uma chave repetida entre uma partição antiga e uma nova. As chaves tocadas na execução precisam ser conferidas contra a tabela inteira.

## Conceitos para estudar

- idempotência × determinismo: a mesma entrada dá a mesma saída, e repetir não duplica;
- recarga × reprocessamento × *backfill* × *catch-up*;
- Auto Loader: rastreamento por caminho, `cloudFiles.allowOverwrites`, checkpoint;
- arquivo-marcador (`_SUCCESS`), escrita em nome temporário e renomeação;
- *write-audit-publish* (WAP): verificação como portão × alarme depois da publicação;
- materialized view e recomputação completa (`COMPLETE_RECOMPUTE`); `GROUP BY ALL` × `DISTINCT`; observado × inferido × hipótese;
- defesa em profundidade: prevenir na dimensão e detectar na silver e no job;
- `row_number()` com ordenação total, desempate por hash e o que o nulo faz com ele;
- verificação de conjunto × expectation de linha (os *data tests* do dbt fazem o mesmo papel);
- retentativas: erro transitório × determinístico; idempotência como pré-requisito de *retry*;
- reprodutibilidade: versão exata de dependência e ambiente de execução;
- prova por checksum de tabela (`xxhash64`) e `EXCEPT ALL`;
- minimização de PII e retenção: o custo escondido da extração full;
- *late-arriving dimension* resolvida pela recomputação.
