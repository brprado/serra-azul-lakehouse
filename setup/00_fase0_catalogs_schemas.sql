-- Fase 0 — esqueleto do lakehouse (ADR-0001)
-- Idempotente: pode ser executado novamente sem efeito colateral.
-- Nomes literais de propósito: DDL de execução única (ver ADR-0002, parametrização vale para o código dos pipelines).
-- Free Edition: catálogos só podem ser criados via SQL (default storage); a API/CLI recusa.

-- Catálogo compartilhado: tudo que é alinhado à fonte + dimensões conformadas (dono: time técnico)
CREATE CATALOG IF NOT EXISTS core
  COMMENT 'Alinhado à fonte (landing, bronze, silver) + dimensões conformadas (gold). Dono: time técnico. ADR-0001';

CREATE SCHEMA IF NOT EXISTS core.landing
  COMMENT 'Volumes de chegada de arquivos, um por fonte';
CREATE SCHEMA IF NOT EXISTS core.bronze
  COMMENT 'Dados crus por fonte (prefixo do sistema: erp_, wms_, tms_, sellout_). Contém PII: acesso restrito';
CREATE SCHEMA IF NOT EXISTS core.silver
  COMMENT 'Entidades limpas e deduplicadas, ainda alinhadas à fonte';
CREATE SCHEMA IF NOT EXISTS core.gold
  COMMENT 'SOMENTE dimensões conformadas (dim_*). Qualquer outra gold precisa de um domínio dono';

CREATE VOLUME IF NOT EXISTS core.landing.erp
  COMMENT 'Landing dos extratos do ERP (fatia 1)';
CREATE VOLUME IF NOT EXISTS core.landing.ref
  COMMENT 'Dados de referência mantidos pela engenharia (ex.: CEP → município). Fatia 1, ADR-0006';

-- Catálogo do domínio comercial (dono: comercial)
CREATE CATALOG IF NOT EXISTS commercial
  COMMENT 'Gold do domínio comercial: fatos e agregados. ADR-0001';

CREATE SCHEMA IF NOT EXISTS commercial.gold
  COMMENT 'Fatos e agregados do comercial (fact_*)';
