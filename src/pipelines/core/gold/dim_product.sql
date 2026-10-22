-- Gold: produto (SCD tipo 1, ADR-0006). Resolvida a identidade pelo product_id, o produto não precisa de histórico:
-- descrição e EAN alterados são correções, e reclassificações são lidas "como é hoje".
-- "Como é hoje" é a última versão de cada produto, mesmo sem vigência aberta (a regra do is_latest, ADR-0008), e não
-- a versão is_current: com is_current, um produto cuja última versão foi fechada sumiria da dimensão (vendas no -1),
-- e duas versões abertas virariam duas linhas com o mesmo product_key, duplicando linhas dos fatos na junção.
-- Desempate: começo mais recente; no mesmo dia, a vigência que termina depois (a aberta vence); depois, a mais
-- atualizada no ERP. Duas versões com o mesmo início já são contadas pela regra de sobreposição da silver.
CREATE OR REFRESH MATERIALIZED VIEW gold.dim_product
COMMENT 'Dimensão conformada de produto, uma linha por product_id (última versão) + membro desconhecido -1.'
AS
SELECT
  xxhash64(product_id) AS product_key,
  product_id, sku, ean, description, brand, category, liquid, package_type,
  unit_volume_ml, units_per_pack, liters_per_sku, status, launch_date, discontinued_date
FROM silver.erp_product
QUALIFY row_number() OVER (PARTITION BY product_id ORDER BY valid_from DESC, valid_to DESC, _source_updated_at DESC) = 1
UNION ALL
SELECT
  CAST(-1 AS BIGINT), 'N/A', 'N/A', 'N/A', 'PRODUTO DESCONHECIDO', 'DESCONHECIDA', 'DESCONHECIDA', 'DESCONHECIDO', 'DESCONHECIDA',
  CAST(NULL AS INT), CAST(NULL AS INT), CAST(NULL AS DECIMAL(10, 3)), 'DESCONHECIDO', CAST(NULL AS DATE), CAST(NULL AS DATE);
