-- Silver: cabeçalho de NF deduplicado (ADR-0008). O tipo da nota vem do CFOP, como no documento fiscal:
-- 5102/6102 = venda, 1202/2202 = devolução de venda. Valores continuam positivos; o sinal é regra do fato (ADR-0003).
CREATE STREAMING LIVE VIEW erp_invoice_header_clean (
  CONSTRAINT invoice_key_not_null EXPECT (invoice_id IS NOT NULL AND invoice_series IS NOT NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT known_cfop EXPECT (invoice_type IS NOT NULL),
  CONSTRAINT return_has_original_invoice EXPECT (invoice_type IS DISTINCT FROM 'RETURN' OR original_invoice_id IS NOT NULL)
)
AS SELECT
  num_nf AS invoice_id,
  serie AS invoice_series,
  num_pedido AS order_id,
  num_nf_origem AS original_invoice_id,
  cod_cliente AS customer_id,
  cfop,
  CASE WHEN cfop IN ('5102', '6102') THEN 'SALE' WHEN cfop IN ('1202', '2202') THEN 'RETURN' END AS invoice_type,
  dt_emissao AS issue_date,
  CAST(vl_total_nf AS DECIMAL(18, 2)) AS invoice_total_amount,
  dt_atualizacao AS _source_updated_at,
  _source_file,
  _extraction_date,
  _ingested_at
FROM STREAM(bronze.erp_invoice_header);

CREATE OR REFRESH STREAMING TABLE silver.erp_invoice_header
COMMENT 'Notas fiscais do ERP, uma linha por (invoice_id, invoice_series). invoice_type derivado do CFOP.';

CREATE FLOW erp_invoice_header_cdc AS AUTO CDC INTO silver.erp_invoice_header
FROM STREAM(erp_invoice_header_clean)
KEYS (invoice_id, invoice_series)
SEQUENCE BY STRUCT(_source_updated_at, _ingested_at)
STORED AS SCD TYPE 1;
