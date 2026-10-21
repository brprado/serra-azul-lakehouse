-- Gold: cliente com SCD tipo 2 por atributo (ADR-0006).
-- Rastreados (geram versão): tipo/canal, segmento, endereço, cidade, UF e região geográfica.
-- Tipo 1 (valor atual em todas as versões): razão social, nome fantasia, tipo de pessoa, validade do documento, status.
-- Versões consecutivas da silver que não mudam nada rastreado (troca de telefone, correção de grafia...) são fundidas.
CREATE OR REFRESH MATERIALIZED VIEW gold.dim_customer
COMMENT 'Dimensão conformada de cliente: uma linha por versão rastreada, vigência [valid_from, valid_to), chave determinística + membro -1.'
AS
WITH tracked AS (
  SELECT
    customer_id, valid_from, valid_to,
    named_struct(
      'customer_type', customer_type, 'sales_channel', sales_channel, 'trade_segment', trade_segment,
      'postal_code', postal_code, 'street', street, 'street_number', street_number, 'district', district,
      'city', city, 'state', state, 'geo_region', geo_region) AS attrs
  FROM silver.erp_customer
),
flagged AS (
  SELECT *,
    CASE WHEN lag(attrs) OVER (PARTITION BY customer_id ORDER BY valid_from) IS DISTINCT FROM attrs THEN 1 ELSE 0 END AS opens_version
  FROM tracked
),
numbered AS (
  SELECT *,
    sum(opens_version) OVER (PARTITION BY customer_id ORDER BY valid_from ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS version_number
  FROM flagged
),
versions AS (
  SELECT customer_id, version_number, min(valid_from) AS valid_from, max(valid_to) AS last_valid_to, any_value(attrs) AS attrs
  FROM numbered
  GROUP BY customer_id, version_number
),
dated AS (
  SELECT *,
    coalesce(lead(valid_from) OVER (PARTITION BY customer_id ORDER BY valid_from), last_valid_to) AS valid_to,
    version_number = max(version_number) OVER (PARTITION BY customer_id) AS is_latest
  FROM versions
),
current_values AS (
  SELECT customer_id, legal_name, trade_name, person_type, is_document_valid, status, registered_date
  FROM silver.erp_customer
  WHERE is_current
)
SELECT
  xxhash64(d.customer_id, d.valid_from) AS customer_key,
  d.customer_id,
  d.version_number,
  d.attrs.customer_type, d.attrs.sales_channel, d.attrs.trade_segment,
  d.attrs.postal_code, d.attrs.street, d.attrs.street_number, d.attrs.district,
  d.attrs.city, d.attrs.state, d.attrs.geo_region,
  c.legal_name, c.trade_name, c.person_type, c.is_document_valid, c.status, c.registered_date,
  d.valid_from, d.valid_to,
  d.valid_to = DATE'9999-12-31' AS is_current,
  d.is_latest
FROM dated d
JOIN current_values c ON c.customer_id = d.customer_id
UNION ALL
SELECT
  CAST(-1 AS BIGINT), 'N/A', 1,
  'DESCONHECIDO', 'DESCONHECIDO', 'DESCONHECIDO', CAST(NULL AS STRING), CAST(NULL AS STRING), CAST(NULL AS STRING),
  CAST(NULL AS STRING), 'DESCONHECIDA', 'DESCONHECIDA', 'DESCONHECIDA',
  CAST(NULL AS STRING), 'CLIENTE DESCONHECIDO', CAST(NULL AS STRING), CAST(NULL AS BOOLEAN), 'DESCONHECIDO', CAST(NULL AS DATE),
  DATE'1900-01-01', DATE'9999-12-31', true, true;
