-- Gold: vendedor com SCD tipo 2 (ADR-0006). Rastreados: supervisor, gerente e região comercial; nome é tipo 1.
-- is_latest marca a última versão de cada vendedor, inclusive de quem foi desligado (sem versão vigente):
-- é ela que a visão "como é hoje" usa, para que as vendas de desligados não fiquem sem região (ADR-0008).
-- Mesmo início (ADR-0012), como na dim_customer: antes de calcular as versões, fica uma linha por (sales_rep_id,
-- valid_from), com desempate determinístico e total (a vigência que termina mais tarde, depois a mais atualizada no
-- ERP, depois o hash dos atributos; total salvo os casos de empate do hash descritos na dim_customer). Sem isso, duas
-- versões com o mesmo início teriam a mesma sales_rep_key.
-- O nome (tipo 1) vem da última versão dessa mesma base, como os tipo 1 da dim_customer (ADR-0011).
CREATE OR REFRESH MATERIALIZED VIEW gold.dim_sales_rep
COMMENT 'Dimensão conformada de vendedor: uma linha por versão, vigência [valid_from, valid_to), chave determinística + membro -1.'
AS
WITH one_per_start AS (
  SELECT *
  FROM silver.erp_sales_hierarchy
  QUALIFY row_number() OVER (
    PARTITION BY sales_rep_id, valid_from
    ORDER BY valid_to DESC, _source_updated_at DESC NULLS LAST,
      xxhash64(sales_rep_name, supervisor_id, supervisor_name, manager_id, manager_name, commercial_region) DESC) = 1
),
tracked AS (
  SELECT
    sales_rep_id, valid_from, valid_to,
    named_struct(
      'supervisor_id', supervisor_id, 'supervisor_name', supervisor_name,
      'manager_id', manager_id, 'manager_name', manager_name, 'commercial_region', commercial_region) AS attrs
  FROM one_per_start
),
flagged AS (
  SELECT *,
    CASE WHEN lag(attrs) OVER (PARTITION BY sales_rep_id ORDER BY valid_from) IS DISTINCT FROM attrs THEN 1 ELSE 0 END AS opens_version
  FROM tracked
),
numbered AS (
  SELECT *,
    sum(opens_version) OVER (PARTITION BY sales_rep_id ORDER BY valid_from ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS version_number
  FROM flagged
),
versions AS (
  SELECT sales_rep_id, version_number, min(valid_from) AS valid_from, max(valid_to) AS last_valid_to, any_value(attrs) AS attrs
  FROM numbered
  GROUP BY sales_rep_id, version_number
),
dated AS (
  SELECT *,
    coalesce(lead(valid_from) OVER (PARTITION BY sales_rep_id ORDER BY valid_from), last_valid_to) AS valid_to,
    version_number = max(version_number) OVER (PARTITION BY sales_rep_id) AS is_latest
  FROM versions
),
names AS (
  -- Nome do vendedor é tipo 1: vale o da última versão, mesmo sem vigência aberta (vendedor desligado).
  SELECT sales_rep_id, sales_rep_name
  FROM one_per_start
  QUALIFY row_number() OVER (PARTITION BY sales_rep_id ORDER BY valid_from DESC) = 1
)
SELECT
  xxhash64(d.sales_rep_id, d.valid_from) AS sales_rep_key,
  d.sales_rep_id,
  n.sales_rep_name,
  d.version_number,
  d.attrs.supervisor_id, d.attrs.supervisor_name, d.attrs.manager_id, d.attrs.manager_name, d.attrs.commercial_region,
  d.valid_from, d.valid_to,
  d.valid_to = DATE'9999-12-31' AS is_current,
  d.is_latest
FROM dated d
JOIN names n ON n.sales_rep_id = d.sales_rep_id
UNION ALL
SELECT
  CAST(-1 AS BIGINT), 'N/A', 'VENDEDOR DESCONHECIDO', 1,
  'N/A', 'DESCONHECIDO', 'N/A', 'DESCONHECIDO', 'DESCONHECIDA',
  DATE'1900-01-01', DATE'9999-12-31', true, true;
