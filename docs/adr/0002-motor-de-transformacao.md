# ADR-0002: Usar Lakeflow Spark Declarative Pipelines (SDP) como motor de transformação

- **Status:** aceito — emendado pelo [ADR-0005](0005-orquestracao-por-job-e-codigo-em-bundle.md) (Job, Git e bundle no lugar do disparo manual) e pelo [ADR-0006](0006-dimensoes-conformadas-identidade-scd-e-chaves.md) (SCD 2 das dimensões a partir da vigência do ERP, sem `AUTO CDC`)
- **Data:** 2026-10-16
- **Fatia:** fase 0
- **Decisor:** Engenheiro de Dados Sênior · **Revisor:** Staff Engineer

## Contexto

As transformações landing → bronze → silver → gold (ADR-0001) precisam de deduplicação por recência, SCD tipo 2 com histórico seletivo, regras de qualidade e reprocessamento sem duplicar. As alternativas eram SDP (declarativo) ou notebooks/scripts orquestrados por Jobs (imperativo, com `MERGE` escrito à mão). No Databricks Free Edition só há compute serverless e é permitido **um pipeline Lakeflow ativo por tipo** — a documentação não define "ativo".

## Decisão

Usar **Lakeflow SDP**, com SQL como linguagem padrão (Python só onde SQL não resolver).

- **Pipelines:** um pipeline `core` (landing → bronze → silver → dimensões em `core.gold`) e um pipeline por domínio (começando por `commercial`). Rodam **um por vez**, com **disparo manual**, o `core` primeiro.
- **Nomes de catálogo (parametrização):** o código SQL dos pipelines **nunca** escreve o nome do catálogo. Cada pipeline declara na sua `configuration` as chaves `catalog_core` e/ou `catalog_<domínio>` (ex.: `catalog_commercial`), e o SQL as referencia por interpolação: `${catalog_core}.bronze.erp_invoice`, `${catalog_commercial}.gold.fact_sales`. Trocar de catálogo é mudar configuração, não código. *Verificação no Free Edition pendente: confirmar ao criar o primeiro pipeline (fatia 1).* Exceção: o script de setup (`setup/00_fase0_catalogs_schemas.sql`) usa nomes literais, por ser DDL de execução única.
- **Recursos do SDP que atendem a fatia 1:** `AUTO CDC` com `SEQUENCE BY` (fica a versão mais recente por chave, inclusive com eventos fora de ordem), `STORED AS SCD TYPE 2` com `TRACK HISTORY ON` (nova versão só quando campos relevantes mudam, ex.: supervisor/gerente/região), expectations (`warn` / `drop` / `fail` por regra, com métricas por execução), streaming tables com Auto Loader (cada arquivo da landing processado uma única vez).
- **Premissa a verificar:** "um pipeline ativo" = um rodando por vez. Será confirmada ao criar o segundo pipeline; se só puder existir um pipeline ETL, juntamos tudo num único pipeline que publica nos dois catálogos (nome totalmente qualificado).

## Alternativas consideradas

| Alternativa | Por que não |
|---|---|
| Notebooks/scripts SQL ou Python orquestrados por Jobs, com `MERGE` manual | Dedup por recência, SCD2 (incluindo ordem dos eventos e histórico seletivo), regras de qualidade com métricas e ingestão incremental idempotente teriam de ser escritos e testados à mão; o SDP entrega esses comportamentos declarativamente |

## Consequências

- Positivas: regras de negócio mais fáceis de alterar e ler (SQL declarativo, acessível a analistas e analytics engineers); comportamentos difíceis (CDC, SCD2, qualidade, checkpoint de ingestão) vêm prontos; suporte nativo da Databricks.
- Negativas / dívidas assumidas: perde-se a flexibilidade de código imperativo (chamadas a APIs externas, lógica com efeitos colaterais) — ingestão de APIs fica fora do pipeline; *full refresh* de streaming table reprocessa do zero e pode perder estado, então só com aprovação explícita; disparo manual não garante o SLA das 8h; a configuração de cada pipeline precisa ser mantida à mão (sem bundle, não há um ponto único que preencha os valores por ambiente).
- O que nos faria revisitar: a premissa de "um rodando por vez" se mostrar falsa; uma transformação que exija lógica imperativa no meio do fluxo; a necessidade de cumprir o SLA de forma automática.

## Em produção faríamos diferente?

Sim:

- **Orquestração:** um Job agendado (madrugada) com `pipeline_task` em sequência (`core` → domínios), retries e alerta de falha, para cumprir o SLA de D-1 até 8h.
- **Parametrização:** a mesma regra (`${catalog_*}` na `configuration`), mas com os valores preenchidos por variáveis de bundle por target (dev/prod), versionadas e promovidas por CI/CD — em vez de configurados à mão em cada pipeline.
- **Pipelines:** sem o limite do Free Edition, os pipelines de domínio poderiam rodar em paralelo depois do `core`.

Em 10x de volume a escolha se mantém (o SDP escala com o serverless e processa incrementalmente); o ponto de atenção passa a ser o custo de *full refresh* e a duração da janela sequencial.

## Conceitos para estudar

Declarativo vs imperativo; streaming table vs materialized view; Auto Loader e checkpoint; CDC e `AUTO CDC` (`KEYS`, `SEQUENCE BY`, `APPLY AS DELETE WHEN`); SCD tipo 2 com histórico seletivo; expectations e ações por violação; idempotência; *full refresh* vs *selective refresh*.
