-- Gold: produto (SCD tipo 1, ADR-0006). Resolvida a identidade pelo product_id, o produto não precisa de histórico:
-- descrição e EAN alterados são correções, e reclassificações são lidas "como é hoje".
CREATE OR REFRESH MATERIALIZED VIEW gold.dim_product
COMMENT 'Dimensão conformada de produto, uma linha por product_id (versão atual) + membro desconhecido -1.'
AS
SELECT
  xxhash64(product_id) AS product_key,
  product_id, sku, ean, description, brand, category, liquid, package_type,
  unit_volume_ml, units_per_pack, liters_per_sku, status, launch_date, discontinued_date
FROM silver.erp_product
WHERE is_current
UNION ALL
SELECT
  CAST(-1 AS BIGINT), 'N/A', 'N/A', 'N/A', 'PRODUTO DESCONHECIDO', 'DESCONHECIDA', 'DESCONHECIDA', 'DESCONHECIDO', 'DESCONHECIDA',
  CAST(NULL AS INT), CAST(NULL AS INT), CAST(NULL AS DECIMAL(10, 3)), 'DESCONHECIDO', CAST(NULL AS DATE), CAST(NULL AS DATE);
