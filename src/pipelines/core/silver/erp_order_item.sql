-- Silver: itens de pedido deduplicados por (order_id, order_item_number) (ADR-0008).
-- Quantidade ou valor negativo é erro de integração (devolução não é pedido negativo: vem por NF):
-- a linha é descartada e a contagem fica nas métricas de qualidade do pipeline. Quantidade ou valor nulo também:
-- o coalesce faz a regra dar false, e não NULL, quando falta o valor.
CREATE STREAMING LIVE VIEW erp_order_item_clean (
  CONSTRAINT order_item_key_not_null EXPECT (order_id IS NOT NULL AND order_item_number IS NOT NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT positive_quantity_and_amount EXPECT (coalesce(quantity > 0 AND net_amount >= 0, false)) ON VIOLATION DROP ROW
)
AS SELECT
  num_pedido AS order_id,
  item_pedido AS order_item_number,
  cod_produto AS product_id,
  quantidade AS quantity,
  CAST(vl_unitario AS DECIMAL(18, 2)) AS unit_price,
  CAST(quantidade * vl_unitario AS DECIMAL(18, 2)) AS gross_amount,
  CAST(vl_desconto AS DECIMAL(18, 2)) AS discount_amount,
  CAST(vl_total_item AS DECIMAL(18, 2)) AS net_amount,
  dt_atualizacao AS _source_updated_at,
  _source_file,
  _extraction_date,
  _ingested_at
FROM STREAM(bronze.erp_order_item);

CREATE OR REFRESH STREAMING TABLE silver.erp_order_item
COMMENT 'Itens de pedido do ERP, uma linha por (order_id, order_item_number). Itens com quantidade/valor negativo descartados.';

CREATE FLOW erp_order_item_cdc AS AUTO CDC INTO silver.erp_order_item
FROM STREAM(erp_order_item_clean)
KEYS (order_id, order_item_number)
SEQUENCE BY STRUCT(_source_updated_at, _ingested_at)
STORED AS SCD TYPE 1;
