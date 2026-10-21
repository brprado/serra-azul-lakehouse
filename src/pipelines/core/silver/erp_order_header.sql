-- Silver: cabeçalho de pedido deduplicado (ADR-0008).
-- Duplicatas exatas (reenvio da integração) e quase-duplicatas (mesmo pedido com status antigo) somem no AUTO CDC:
-- fica uma linha por order_id, a de maior dt_atualizacao. Reprocessar um arquivo vira upsert, nunca duplicata.
-- Empate de dt_atualizacao (status mudou no mesmo instante da criação) é desfeito pelo ciclo de vida do pedido:
-- o status mais avançado vence, porque um pedido nunca volta de FATURADO/CANCELADO para ABERTO.
CREATE STREAMING LIVE VIEW erp_order_header_clean (
  CONSTRAINT order_id_not_null EXPECT (order_id IS NOT NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT order_date_not_null EXPECT (order_date IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT known_status EXPECT (order_status IN ('ABERTO', 'PARCIALMENTE FATURADO', 'FATURADO', 'CANCELADO'))
)
AS SELECT
  num_pedido AS order_id,
  cod_cliente AS customer_id,
  cod_vendedor AS sales_rep_id,
  dt_pedido AS order_date,
  status AS order_status,
  CAST(vl_total_pedido AS DECIMAL(18, 2)) AS order_total_amount,
  dt_criacao AS created_at,
  dt_atualizacao AS _source_updated_at,
  CASE status WHEN 'ABERTO' THEN 1 WHEN 'PARCIALMENTE FATURADO' THEN 2 WHEN 'FATURADO' THEN 3 WHEN 'CANCELADO' THEN 4 ELSE 0 END
    AS _status_rank,
  _source_file,
  _extraction_date,
  _ingested_at
FROM STREAM(bronze.erp_order_header);

CREATE OR REFRESH STREAMING TABLE silver.erp_order_header
COMMENT 'Pedidos do ERP, uma linha por order_id (versão mais recente). Data de negócio: order_date.';

CREATE FLOW erp_order_header_cdc AS AUTO CDC INTO silver.erp_order_header
FROM STREAM(erp_order_header_clean)
KEYS (order_id)
SEQUENCE BY STRUCT(_source_updated_at, _status_rank, _ingested_at)
COLUMNS * EXCEPT (_status_rank)
STORED AS SCD TYPE 1;
