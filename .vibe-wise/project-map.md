# Project Map

## Purpose
Lakehouse de dados da fictícia Cervejaria Serra Azul (projeto de estudos): responder perguntas de negócio de comercial, supply, trade marketing e data science a partir de dados fake, em fatias verticais por domínio.

## Requirements
- Medallion (bronze/silver/gold); somente batch; dados gerados com a skill databricks-synthetic-data-gen.
- SLA comercial: dados D-1 até 8h; idempotência de reprocesso; PII não sai da bronze (ADR-0004).
- Ambiente: Databricks Free Edition, perfil CLI `aws2` — serverless only, 1 warehouse 2X-Small, 1 pipeline ativo por tipo, 5 tasks concorrentes, fair usage quota (estouro derruba compute pelo dia; em casos extremos, pelo mês).
- Detalhes: docs/caso-de-negocio.md, docs/roadmap.md, docs/convencoes.md. Decisões em docs/adr/ (0001–0007). Revisão geral em docs/revisao-2026-10-19.md.

## Components
- docs/ — caso de negócio, roadmap, convenções (v2, 2026-10-19), ADRs 0000–0007, revisão 2026-10-19. [existe]
- setup/00_fase0_catalogs_schemas.sql — DDL idempotente da fase 0. [IMPLEMENTADO e executado no aws2 em 2026-10-16]
- Workspace aws2: catálogos `core` (schemas landing/bronze/silver/gold; volume `core.landing.erp`) e `commercial` (schema gold). [IMPLEMENTADO, vazio]
- .claude/agents/ — 5 subagentes (adr-reviewer, done-checker, free-edition-checker, interviewer, synthetic-data) + hook `.claude/hooks/databricks_guard.py`. Prompts atualizados para os papéis novos. [existe]
- Mod `agent-board` (faixa de agentes acima do prompt) carregado na sessão via dev-mods. [existe, fora do repo]
- Git: repositório inicializado em 2026-10-19 com .gitignore; commit inicial bf20278 (fase 0 + revisão). [existe]
- notebooks/analise_nyctaxi.py — exercício anterior ao projeto, fora do lakehouse. [existe]
- Bundle `databricks.yml` (target prod/aws2, sync só do código) + job `erp_initial_load` (serverless, env client 4 + faker). [IMPLEMENTADO e implantado em 2026-10-19]
- data_gen/erp/erp_initial_load.py — gerador determinístico (hash xxhash64 + semente 20261019) da carga inicial do ERP; manifests de sujeiras em data_gen/erp/manifest_*.json. [IMPLEMENTADO; executado: 805 mil pedidos, 2,93 mi itens, 839 mil NFs, 3,03 mi itens de NF, 72.650 versões de cliente]
- Landing preenchida: /Volumes/core/landing/erp/<dataset>/2026-10-19/ (7 datasets) e /Volumes/core/landing/ref/cep_municipio/2026-10-19/ (volume core.landing.ref criado). Dicionário em docs/dicionario-de-dados-erp.md.
- Pipelines SDP, job diário, dashboard — [não iniciados; próximo passo]

## Main Flow
Decisões ACEITAS (Claude como Sênior desde 2026-10-19); da fatia 1, só a landing está implementada:

landing (Parquet, /Volumes/core/landing/erp/<dataset>/<data>/)
  --Auto Loader--> core.bronze.erp_* (append-only, PII só aqui)
  --dedup AUTO CDC SCD1 (fatos) / limpeza + versões (cadastros)--> core.silver.erp_* (sem PII)
  --MVs--> core.gold: dim_date (tipo 0), dim_product (tipo 1, product_id), dim_customer (SCD2 por atributo), dim_sales_rep (SCD2 pela vigência do ERP)
  --MVs--> commercial.gold: fact_sales (item do pedido), fact_invoice (item da NF); chaves _key (como era) + _id (como é hoje); volume_hl
  --> dashboard AI/BI (região comercial como padrão)

Orquestração: Job `serra_azul_daily` (simulate_erp_extraction → pipeline core → pipeline commercial), cron 05:00 BRT pausado; bundle com 1 target; validate-only antes de rodar.

## Data and Trust Boundaries
- Unity Catalog: 1 metastore (`metastore_aws_us_east_2`); catálogos só via SQL `CREATE CATALOG` (API falha). Storage só gerenciado.
- PII (CPF/CNPJ, contatos) só na bronze, acesso restrito, tag `pii=true`; silver/gold só com ids pseudônimos.
- Workspace: https://dbc-5822c68d-e092.cloud.databricks.com (conta de testes, perfil `aws2`).

## Build and Deployment
- CLI Databricks v1.21.0 (perfis: aws2 = ativo; aws = bloqueado por quota; empresa = NÃO usar). `.claude/settings.json` fixa DATABRICKS_CONFIG_PROFILE=aws2.
- Deploy previsto por Declarative Automation Bundles (ADR-0005); catálogos/schemas por setup SQL.

## Unknowns
- Premissa "1 pipeline ETL ativo = 1 rodando por vez" (irrelevante com Job sequencial, salvo se só puder existir 1 pipeline ETL).
- Notificação de falha por e-mail e update validate-only no Free Edition — verificar na implementação.
- Parametrização `${catalog_*}` via configuration do pipeline — testar no primeiro pipeline.
