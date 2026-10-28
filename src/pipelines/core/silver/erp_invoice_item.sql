-- Silver: itens de NF deduplicados (ADR-0008). Cada item aponta para o item de pedido faturado
-- (order_id + order_item_number), o que permite conciliar faturamento parcial e devoluções item a item.
-- Item de NF com quantidade nula ou <= 0 é inválido pela regra fiscal: descartado, com contagem nas métricas.
-- (A carga inicial de 2026-10-19 tem 16.285 itens assim, por um defeito do gerador no faturamento parcial; ADR-0008.
-- As extrações diárias trazem os últimos: a 2ª NF dos pedidos parciais cuja 1ª NF saiu até 2026-10-18; ADR-0012.)
CREATE STREAMING LIVE VIEW erp_invoice_item_clean (
  CONSTRAINT invoice_item_key_not_null EXPECT (invoice_id IS NOT NULL AND invoice_series IS NOT NULL AND invoice_item_number IS NOT NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT known_cfop EXPECT (invoice_type IS NOT NULL),
  CONSTRAINT positive_quantity EXPECT (quantity IS NOT NULL AND quantity > 0) ON VIOLATION DROP ROW
)
AS SELECT
  num_nf AS invoice_id,
  serie AS invoice_series,
  item_nf AS invoice_item_number,
  num_pedido AS order_id,
  item_pedido AS order_item_number,
  cod_produto AS product_id,
  cfop,
  CASE WHEN cfop IN ('5102', '6102') THEN 'SALE' WHEN cfop IN ('1202', '2202') THEN 'RETURN' END AS invoice_type,
  quantidade AS quantity,
  CAST(vl_unitario AS DECIMAL(18, 2)) AS unit_price,
  CAST(vl_desconto AS DECIMAL(18, 2)) AS discount_amount,
  CAST(vl_total_item AS DECIMAL(18, 2)) AS net_amount,
  dt_atualizacao AS _source_updated_at,
  _source_file,
  _extraction_date,
  _ingested_at
FROM STREAM(bronze.erp_invoice_item);

CREATE OR REFRESH STREAMING TABLE silver.erp_invoice_item
COMMENT 'Itens de NF do ERP, uma linha por (invoice_id, invoice_series, invoice_item_number).';

CREATE FLOW erp_invoice_item_cdc AS AUTO CDC INTO silver.erp_invoice_item
FROM STREAM(erp_invoice_item_clean)
KEYS (invoice_id, invoice_series, invoice_item_number)
SEQUENCE BY STRUCT(_source_updated_at, _ingested_at)
STORED AS SCD TYPE 1;
