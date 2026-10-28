-- Silver: versões de produto (ADR-0006, ADR-0008). A identidade é o product_id do ERP, nunca reaproveitado;
-- o SKU é atributo e pode se repetir entre produtos diferentes.
-- Vigência contígua (o valid_to de uma versão é o valid_from da seguinte) só é medida, por duas regras de aviso:
-- sobreposição e buraco. A dim_product fica com a última versão de cada produto (ADR-0010 D12), então duas versões
-- abertas não duplicam os fatos; a regra mostra que a fonte mandou um histórico incoerente.
-- Reenvio da mesma extração (o mesmo arquivo com outro nome): linhas idênticas em todas as colunas de negócio viram
-- uma (ADR-0012), como em silver.erp_customer.
CREATE OR REFRESH MATERIALIZED VIEW silver.erp_product (
  CONSTRAINT product_id_not_null EXPECT (product_id IS NOT NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT valid_interval EXPECT (valid_from IS NOT NULL AND valid_to > valid_from),
  CONSTRAINT no_overlap_with_next_version EXPECT (_next_valid_from IS NULL OR _next_valid_from >= valid_to),
  CONSTRAINT no_gap_to_next_version EXPECT (_next_valid_from IS NULL OR _next_valid_from <= valid_to),
  CONSTRAINT positive_volume EXPECT (coalesce(unit_volume_ml > 0 AND units_per_pack > 0, false))
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
  -- Início da versão seguinte do mesmo produto (nulo na última); desempate como em silver.erp_customer.
  lead(dt_inicio_vigencia) OVER (
    PARTITION BY cod_produto ORDER BY dt_inicio_vigencia, dt_fim_vigencia NULLS LAST) AS _next_valid_from,
  dt_atualizacao AS _source_updated_at,
  _extraction_date
FROM (
  -- GROUP BY ALL, e não DISTINCT: ver silver.erp_customer.
  SELECT * EXCEPT (_source_file, _ingested_at)
  FROM bronze.erp_product
  WHERE _extraction_date = (SELECT max(_extraction_date) FROM bronze.erp_product)
  GROUP BY ALL
);
