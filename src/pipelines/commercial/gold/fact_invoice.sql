-- Gold: fato de faturamento, uma linha por item de NF (ADR-0003, ADR-0006 §5 e §9). Mesmas regras de chave do fact_sales,
-- com a issue_date como data do evento. order_id + order_item_number ligam cada item ao item de pedido faturado,
-- o que concilia o faturamento parcial e as devoluções item a item.
-- De onde vem cada dimensão:
--   - cliente: o da própria NF (é o destinatário fiscal); hoje é sempre o mesmo do pedido. Item sem cabeçalho da NF
--     também conta no (a) de cliente: o (a) puro é customer_id_in_dimension menos invoice_header_found;
--   - vendedor: a NF não tem vendedor; vem do pedido de origem, resolvido na order_date desse pedido. O vínculo com o
--     vendedor nasce no pedido: assim o item tem a mesma região comercial no fact_sales e aqui, e o pedido de um vendedor
--     desligado, faturado depois do desligamento, não cai no -1. Se o pedido de origem não está na silver, o vendedor
--     fica nulo e cai no -1 (a); order_header_found conta o motivo real, e o (a) puro é sales_rep_id_in_dimension menos
--     order_header_found. A order_date não é publicada (não é medida nem dimensão deste fato): _is_order_header_found
--     guarda só o resultado;
--   - produto: o do item da NF.
-- Devolução fica no fato com sinal negativo em quantidade, volume e valores: a soma já é o faturamento líquido (ADR-0003).
-- A silver guarda os valores positivos, como no documento fiscal; o sinal é aplicado só aqui, uma única vez.
-- CFOP sem regra (invoice_type nulo) fica com medidas nulas: não entra no faturamento até ganhar regra, e a expectation o conta.
-- known_invoice_type testa o nulo antes do IN: com invoice_type nulo, o IN daria NULL, e não false, justamente no caso
-- que a regra existe para contar.
CREATE OR REFRESH MATERIALIZED VIEW gold.fact_invoice (
  CONSTRAINT invoice_header_found EXPECT (issue_date IS NOT NULL),
  CONSTRAINT order_header_found EXPECT (_is_order_header_found),
  CONSTRAINT known_invoice_type EXPECT (invoice_type IS NOT NULL AND invoice_type IN ('SALE', 'RETURN')),
  CONSTRAINT customer_id_in_dimension EXPECT (_is_customer_id_known),
  CONSTRAINT customer_version_valid_on_issue_date EXPECT (customer_key <> -1 OR NOT _is_customer_id_known),
  CONSTRAINT sales_rep_id_in_dimension EXPECT (_is_sales_rep_id_known),
  CONSTRAINT sales_rep_version_valid_on_order_date EXPECT (sales_rep_key <> -1 OR NOT _is_sales_rep_id_known),
  CONSTRAINT product_id_in_dimension EXPECT (product_key <> -1)
)
COMMENT 'Faturamento (NF) do ERP, uma linha por (invoice_id, invoice_series, invoice_item_number). Data de negócio: issue_date. Devolução com sinal negativo; -1 = desconhecido.'
AS
WITH customer_ids AS (
  SELECT DISTINCT customer_id FROM ${catalog_core}.gold.dim_customer WHERE customer_key <> -1
),
sales_rep_ids AS (
  SELECT DISTINCT sales_rep_id FROM ${catalog_core}.gold.dim_sales_rep WHERE sales_rep_key <> -1
),
items AS (
  SELECT
    ii.invoice_id, ii.invoice_series, ii.invoice_item_number, ii.order_id, ii.order_item_number,
    ii.invoice_type, ii.product_id, ii.quantity, ii.discount_amount, ii.net_amount,
    ih.issue_date,
    ih.customer_id,
    oh.sales_rep_id,
    oh.order_date,
    CASE ii.invoice_type WHEN 'SALE' THEN 1 WHEN 'RETURN' THEN -1 END AS sign,
    greatest(ii._ingested_at, ih._ingested_at, oh._ingested_at) AS _updated_at
  FROM ${catalog_core}.silver.erp_invoice_item ii
  -- LEFT JOIN nos cabeçalhos: item que chegue antes da NF ou do pedido fica no fato, e as expectations contam o caso.
  LEFT JOIN ${catalog_core}.silver.erp_invoice_header ih
    ON ih.invoice_id = ii.invoice_id AND ih.invoice_series = ii.invoice_series
  LEFT JOIN ${catalog_core}.silver.erp_order_header oh ON oh.order_id = ii.order_id
)
SELECT
  i.invoice_id,
  i.invoice_series,
  i.invoice_item_number,
  i.order_id,
  i.order_item_number,
  i.invoice_type,
  i.issue_date,
  coalesce(c.customer_key, -1) AS customer_key,
  i.customer_id,
  coalesce(r.sales_rep_key, -1) AS sales_rep_key,
  i.sales_rep_id,
  coalesce(p.product_key, -1) AS product_key,
  i.product_id,
  i.sign * i.quantity AS quantity,
  CAST(i.sign * i.quantity * p.liters_per_sku / 100 AS DECIMAL(18, 5)) AS volume_hl,
  -- Bruto = líquido + desconto, e não quantidade × preço: ~3% dos itens divergem do pedido em centavos no valor do item,
  -- e o faturamento vale o fiscal. Assim bruto − desconto = líquido em toda linha, como no fact_sales.
  CAST(i.sign * (i.net_amount + i.discount_amount) AS DECIMAL(18, 2)) AS gross_amount,
  CAST(i.sign * i.discount_amount AS DECIMAL(18, 2)) AS discount_amount,
  CAST(i.sign * i.net_amount AS DECIMAL(18, 2)) AS net_amount,
  -- A silver descarta pedido sem order_date (DROP ROW), então data nula aqui só acontece sem o cabeçalho do pedido.
  i.order_date IS NOT NULL AS _is_order_header_found,
  ci.customer_id IS NOT NULL AS _is_customer_id_known,
  ri.sales_rep_id IS NOT NULL AS _is_sales_rep_id_known,
  i._updated_at
FROM items i
LEFT JOIN ${catalog_core}.gold.dim_customer c
  ON c.customer_id = i.customer_id AND i.issue_date >= c.valid_from AND i.issue_date < c.valid_to
LEFT JOIN customer_ids ci ON ci.customer_id = i.customer_id
LEFT JOIN ${catalog_core}.gold.dim_sales_rep r
  ON r.sales_rep_id = i.sales_rep_id AND i.order_date >= r.valid_from AND i.order_date < r.valid_to
LEFT JOIN sales_rep_ids ri ON ri.sales_rep_id = i.sales_rep_id
LEFT JOIN ${catalog_core}.gold.dim_product p ON p.product_id = i.product_id;
