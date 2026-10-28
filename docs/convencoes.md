# Convenções do lakehouse — Serra Azul

Resumo de uma página das regras em vigor: ADR-0001 com a emenda do [ADR-0004](adr/0004-ajustes-de-organizacao-nomes-landing-e-pii.md), [ADR-0005](adr/0005-orquestracao-por-job-e-codigo-em-bundle.md), [ADR-0006](adr/0006-dimensoes-conformadas-identidade-scd-e-chaves.md), [ADR-0007](adr/0007-retencao-da-bronze-por-tipo-de-extracao.md), [ADR-0008](adr/0008-pipeline-core-deduplicacao-qualidade-e-cadastros.md), [ADR-0010](adr/0010-pipeline-commercial-fatos-chaves-e-membro-desconhecido.md), [ADR-0011](adr/0011-dim-customer-tipo-1-pela-ultima-versao.md) e [ADR-0012](adr/0012-job-diario-extracao-simulada-idempotencia-e-verificacao.md). Em caso de dúvida, os ADRs mandam. Revisado em 2026-10-28.

## Organização

**Até a silver, por fonte; da gold em diante, por domínio.**

| Catálogo | Schema | Conteúdo | Dono |
|---|---|---|---|
| `core` | `landing` | Volumes de chegada, um por fonte (`erp`, `wms`, `tms`, `sellout`...) | Time técnico |
| `core` | `bronze` | Dado cru por fonte, append-only. **Única camada com PII: acesso restrito** | Time técnico |
| `core` | `silver` | Entidades limpas, deduplicadas e sem PII, ainda alinhadas à fonte | Time técnico |
| `core` | `gold` | **Somente** dimensões conformadas (`dim_*`) | Time técnico |
| `<domínio>` | `gold` | Fatos e agregados do domínio | Domínio |

Domínios: `commercial` (já criado); `supply`, `trade_marketing`, `logistics` e `data_science` são criados quando a fatia chegar.

## Nomes de tabelas

- Inglês, `snake_case`, entidade no singular. Sem prefixo de camada (o schema já diz) nem de domínio (o catálogo já diz).
- **Bronze e silver** levam o prefixo do sistema de origem (`erp_`, `wms_`, `tms_`, `sellout_`, `ref_`).
- **Gold**: fatos `fact_<processo>` e dimensões `dim_<entidade que forma o grão>`.

| Dataset da fonte (landing) | Bronze / silver | Extração | Gold |
|---|---|---|---|
| `pedidos_cab` | `erp_order_header` | incremental | `commercial.gold.fact_sales` |
| `pedidos_itens` | `erp_order_item` | incremental | `commercial.gold.fact_sales` |
| `notas_fiscais` | `erp_invoice_header` | incremental | `commercial.gold.fact_invoice` |
| `notas_fiscais_itens` | `erp_invoice_item` | incremental | `commercial.gold.fact_invoice` |
| `clientes_pdv` | `erp_customer` | full, com versões | `core.gold.dim_customer` |
| `produtos` | `erp_product` | full, com versões | `core.gold.dim_product` |
| `hierarquia_comercial` | `erp_sales_hierarchy` | full, com vigência | `core.gold.dim_sales_rep` |
| `cep_municipio` (referência) | `ref_postal_code` | carga única | usada na silver |
| — (gerada) | — | — | `core.gold.dim_date` |

## Nomes de colunas

| O quê | Padrão | Exemplo |
|---|---|---|
| Chave natural/durável (da fonte) | `<entidade>_id` | `customer_id`, `product_id` |
| Chave substituta | `<entidade>_key`, `BIGINT`, `xxhash64` determinístico; `-1` = desconhecido | `customer_key` |
| Data de negócio | `<evento>_date`, `DATE`, horário de Brasília | `order_date`, `issue_date` |
| Instante | `<evento>_at`, `TIMESTAMP` | `created_at` |
| Valor monetário | `<medida>_amount`, `DECIMAL(18,2)`, BRL | `net_amount` |
| Volume | `quantity` (unidades de SKU); `volume_hl` (hectolitros), `DECIMAL(18,5)`, nulo se o produto é desconhecido | |
| Booleano | `is_*` / `has_*` | `is_current`, `is_cancelled` |
| Vigência (SCD 2) | `valid_from`, `valid_to` (intervalo semiaberto `[from, to)`), `is_current` | |
| Técnicas | prefixo `_` | `_source_file`, `_ingested_at`, `_extraction_date`, `_updated_at` |
| Técnicas lidas por expectations | prefixo `_`; existem para a regra medir no resultado final (ADR-0010) | `_next_valid_from` (silver de cadastros), `_is_order_header_found` (`fact_invoice`) |
| Motivo do `-1` nos fatos | `_is_<entidade>_id_known`, `BOOLEAN`. `false`: o `_id` não está na dimensão (cadastro atrasado, FK nula ou exclusão por LGPD, desde que a fonte pare de reenviar o titular, ADR-0012). `true` com `_key = -1`: a data do evento cai fora da vigência total da entidade | `_is_customer_id_known` |

## Landing

- Caminho: `/Volumes/core/landing/<fonte>/<dataset>/<AAAA-MM-DD>/<arquivo>`.
- O nome do dataset é o que a fonte usa (a landing guarda o dado como chega).
- Extratos do ERP chegam em Parquet. O layout do sell-out será decidido na fatia 3.
- **Extração diária do ERP:** um arquivo por dataset, com nome fixo `<dataset>.parquet`. O reenvio da mesma extração vai na mesma pasta, como `<dataset>_reenvio.parquet` (ADR-0012).
- **Nunca sobrescrever nem regravar um arquivo da landing.** O Auto Loader (`allowOverwrites = false`) guarda o estado por caminho (ADR-0012 D4):
  - arquivo sobrescrito **antes** de ser processado: não há garantia de qual versão foi lida;
  - arquivo sobrescrito **depois** de processado: nunca é reingerido, e o conteúdo novo não chega à bronze;
  - caminho novo com o mesmo conteúdo: é lido de novo, e a bronze (*append-only*) guarda as duas cópias. Por isso a carga inicial (`part-<uuid>`) não é regerada na landing real; ensaios vão para outra raiz (`output_root`).
  - **Exceções documentadas:** o direito ao esquecimento (LGPD), que exige regravar ou apagar arquivos com o titular, e o reparo de um arquivo truncado no meio da série (regerado numa pasta de ensaio e copiado por cima).
- **Se o simulador estourar o tempo, apague a extração incompleta antes de rodar de novo:** são os arquivos da extração seguinte à última impressa no log, em todos os datasets. O `core` nunca os leu (ADR-0012).
- Os arquivos ficam retidos por 90 dias.

## Modelagem

- **Visão consolidada** das dimensões, dos fatos e da silver que os sustenta, com o porquê de cada decisão: [modelagem-do-lakehouse.md](modelagem-do-lakehouse.md). Os ADRs continuam mandando.
- **Fato** pertence ao domínio que produz o evento. Os outros domínios **leem, nunca reconstroem**. Uma métrica que cruza processos pertence ao domínio que precisa dela.
- **Fatos** guardam, para cada dimensão, a `_key` da versão vigente na data do evento ("como era") e o `_id` durável ("como é hoje"). As chaves são resolvidas no pipeline, nunca no dashboard.
- **Data da versão:** a chave é resolvida na data do documento que carrega a FK. No `fact_invoice`, o vendedor não está na NF, e sim no pedido: vale a `order_date`. O cliente está na NF (destinatário fiscal): vale a `issue_date`, a versão impressa na nota (ADR-0010).
- **SCD** é escolhido por atributo. O histórico vem da vigência informada pela fonte; correções de qualidade, como grafia, são feitas na silver, antes do cálculo de versões.
- **"Como é hoje"** usa `is_latest` (última versão, mesmo sem vigência aberta, como no vendedor desligado), não `is_current` (ADR-0008). Pelo mesmo motivo, o valor atual dos atributos tipo 1 vem da última versão: a `dim_product` (SCD 1) é montada pela última versão de cada produto (ADR-0010), e os atributos tipo 1 da `dim_customer` vêm da última versão de cada cliente (ADR-0011).

## Camadas nos pipelines (ADR-0008, ADR-0010, ADR-0012)

- **Bronze:** streaming table por dataset, Auto Loader, sem regra; só acrescenta `_source_file`, `_extraction_date`, `_ingested_at`. Toda tabela da bronze leva `pipelines.reset.allowed = false`: como a landing guarda só 90 dias (ADR-0007), um *full refresh* da bronze perderia histórico.
- **Silver de fatos:** streaming live view `erp_*_clean` (renomeia, tipa, aplica expectations) → `AUTO CDC ... STORED AS SCD TYPE 1`. Todo `SEQUENCE BY` tem desempate.
- **Silver de cadastros:** materialized view sobre a última `_extraction_date`, com as linhas idênticas da mesma extração juntadas por `SELECT * EXCEPT (_source_file, _ingested_at) ... GROUP BY ALL`, para absorver o reenvio. Não use `DISTINCT` aqui: no pipeline, ele manteve as duas cópias (ADR-0012). PII validada e descartada aqui. Vigências sobrepostas e buracos entre versões são contados por regras de aviso separadas (`no_overlap_with_next_version`, `no_gap_to_next_version`), sem mudar o cálculo das dimensões (ADR-0010).
- **Gold de fatos:** materialized view recalculada a cada execução. `LEFT JOIN` em cabeçalhos e dimensões: a contagem do fato é a da silver, enquanto a `_key` das dimensões for única (a tarefa `verify_uniqueness` do job confere, ADR-0012). O sinal da devolução é aplicado só no fato; bruto = líquido + desconto em toda linha; `_updated_at` = maior `_ingested_at` das linhas de origem.
- **Expectations:** `FAIL UPDATE` só para quebra de contrato (chave nula); `DROP ROW` para linha inválida conhecida; aviso para medir. Toda regra tem nome e aparece no event log.
  - **Toda expectation dá `true` ou `false`, nunca `NULL`.** Use `x IS NOT NULL AND ...` numa coluna, `coalesce(..., false)` numa expressão composta e `x IS DISTINCT FROM v OR ...` quando o nulo deve passar. Uma regra que dá `NULL` no caso que deveria contar mede 0 sem ter medido nada (lógica de três valores, ADR-0010).
- **Dimensão SCD 2:** antes do cálculo de versões, uma linha por `(id, valid_from)`, com desempate total, salvo empate do hash com nulo deslocado (vigência mais longa, depois a mais atualizada no ERP, depois o hash dos atributos). Duas versões com o mesmo início não repetem a `_key`; a silver mede o caso, e o job falha quando a silver tem versões repetidas `(id, valid_from)` (ADR-0012).
- **Dimensão conformada**: uma por entidade.
  - Mudança aditiva: o dono aprova e avisa os consumidores.
  - Quebra de contrato (renomear/remover coluna, mudar grão ou regra de SCD): o dono e os consumidores aprovam, com versão e prazo de depreciação.

## Código e execução

- **Motor:** Lakeflow SDP, SQL por padrão; Python só onde SQL não resolver (por exemplo, a simulação de dados).
- **Pipelines:** um `core` e um por domínio, encadeados pelo Job `serra_azul_daily`: `simulate_erp_extraction` → `core` → `commercial` → `verify_uniqueness` (ADR-0012). O agendamento (05:00 de Brasília) fica pausado; execução manual com `databricks bundle run serra_azul_daily`.
- **Parâmetros do job:** `extraction_date` (vazio = hoje em `America/Sao_Paulo`; data futura é erro), `output_root` (vazio = a landing real; outra raiz para ensaio) e `resend` (`true` grava o reenvio).
- **Recarga de um dia já gravado** = rodar o job com `extraction_date` explícito (`--params extraction_date=AAAA-MM-DD`): o simulador não grava nada que já existe, e os pipelines rodam de novo sobre a mesma landing, sem mudar nada. Sem `extraction_date`, o simulador completa as extrações que faltam até hoje.
- **Reprocessamento sem duplicata** (caso de negócio §6): o reenvio da fonte é absorvido (`AUTO CDC` nos fatos, `GROUP BY ALL` nos cadastros), e reprocessar com regra nova é *full refresh* seletivo da silver a partir da bronze (ADR-0008, ADR-0012).
- **Retentativas por tarefa:** 1 no simulador (é idempotente) e 1 na verificação (separa violação, que falha de novo, de falha passageira na partida); 0 nos pipelines (já tentam 1 vez por dentro).
- **A verificação é alarme, não portão:** roda depois que os pipelines publicaram. Ela falha o job e não corrige nada; a duplicação que ela acha já chegou ao consumidor (ADR-0012).
- **Simulação de fonte determinística:** o conteúdo de cada extração é função só da sua data. Dependência que afeta o dado gerado tem versão exata (`faker==37.12.0`).
- **Nomes de catálogo** nunca são escritos no SQL: usar `${catalog_core}` / `${catalog_<domínio>}`, preenchidos pelas variáveis do bundle.
- **Datasets:** `CREATE OR REFRESH STREAMING TABLE` / `MATERIALIZED VIEW`. Nunca `CREATE OR REPLACE` nem sintaxe legada (`LIVE.`).
- **Antes de executar:** `databricks bundle validate` e atualização *validate-only* dos pipelines.
- *Full refresh* de streaming table só com aprovação explícita.
- Catálogos e schemas são criados por `setup/*.sql` (no Free Edition, a API não cria catálogos).
- Tudo versionado em Git.

## Acesso, PII e retenção

- **Acesso por domínio:** `GRANT` no catálogo do domínio (herança de privilégios). Bronze do `core` restrita ao time técnico.
- **PII:** não sai da bronze (minimização). Colunas com PII levam a tag `pii = true`. Tags servem para descoberta; elas **não** concedem acesso.
- **Retenção da bronze:** tudo, nos datasets incrementais; as últimas 90 extrações, nos datasets full.
- **Toda decisão importante gera um ADR** em `docs/adr/`. ADR aceito não é reescrito: é emendado ou substituído por outro.
