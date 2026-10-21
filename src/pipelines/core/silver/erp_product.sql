-- Silver: versões de produto (ADR-0006, ADR-0008). A identidade é o product_id do ERP, nunca reaproveitado;
-- o SKU é atributo e pode se repetir entre produtos diferentes.
CREATE OR REFRESH MATERIALIZED VIEW silver.erp_product (
  CONSTRAINT product_id_not_null EXPECT (product_id IS NOT NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT valid_interval EXPECT (valid_to > valid_from),
  CONSTRAINT positive_volume EXPECT (unit_volume_ml > 0 AND units_per_pack > 0)
)
COMMENT 'Produtos do ERP por versão, extração mais recente. liters_per_sku permite medir volume em hectolitros.'
AS SELECT
  cod_produto AS product_id,
  sku,
  ean,
  descricao AS description,
  marca AS brand,
  categoria AS category,
  liquido AS liquid,
  embalagem AS package_type,
  volume_ml AS unit_volume_ml,
  unidades_pack AS units_per_pack,
  CAST(volume_ml * unidades_pack / 1000.0 AS DECIMAL(10, 3)) AS liters_per_sku,
  status,
  dt_lancamento AS launch_date,
  dt_descontinuacao AS discontinued_date,
  dt_inicio_vigencia AS valid_from,
  coalesce(date_add(dt_fim_vigencia, 1), DATE'9999-12-31') AS valid_to,
  dt_fim_vigencia IS NULL AS is_current,
  dt_atualizacao AS _source_updated_at,
  _extraction_date
FROM bronze.erp_product
WHERE _extraction_date = (SELECT max(_extraction_date) FROM bronze.erp_product);
