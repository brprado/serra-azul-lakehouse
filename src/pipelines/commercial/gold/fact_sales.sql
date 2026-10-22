-- Gold: fato de vendas, uma linha por item de pedido, com o cabeçalho desnormalizado (ADR-0003, ADR-0006 §5 e §9).
-- Materialized view recalculada a cada execução: um cliente que chega atrasado sai do -1 sozinho na próxima carga (ADR-0006 §6).
-- As chaves são resolvidas aqui, não no dashboard:
--   - cliente e vendedor: _key da versão vigente na order_date ("como era"), pelo intervalo semiaberto [valid_from, valid_to).
--     Com BETWEEN, a venda do dia da troca casaria com duas versões e seria duplicada;
--   - produto: _key por product_id (SCD 1, uma linha por produto);
--   - o _id durável vai junto, para a visão "como é hoje" (is_latest na dimensão).
-- Nenhuma linha é descartada: sem correspondência, a chave vai para o membro -1 e uma expectation de aviso conta o caso.
-- _is_*_id_known separa os dois motivos de -1, que pedem ações diferentes:
--   (a) o _id não existe na dimensão: cadastro que ainda não chegou (se corrige sozinho), FK nula na origem ou
--       cliente apagado por direito ao esquecimento (ADR-0010 D2);
--   (b) o _id existe, mas a data cai fora da vigência total da entidade: antes da primeira versão ou depois da última
--       versão fechada. Um buraco no meio do histórico não chega aqui: a dimensão estica a versão anterior por cima dele.
-- Item sem cabeçalho também conta no (a) de cliente e de vendedor, porque os dois _id vêm do cabeçalho: o (a) puro é
-- a contagem de customer_id_in_dimension (ou sales_rep_id_in_dimension) menos a de order_header_found.
-- Ficam na tabela, e não numa view intermediária, porque as métricas de expectation de view não são garantidas no event log.
-- Pedido cancelado continua no fato, com order_status e is_cancelled; quem filtra é o dashboard (ADR-0003).
-- Bruto − desconto = líquido (ADR-0010) aqui depende da fonte: o bruto é quantidade × preço e o líquido vem do ERP.
-- amounts_add_up vigia a igualdade; o coalesce faz um valor nulo contar como violação, e não passar como NULL.
CREATE OR REFRESH MATERIALIZED VIEW gold.fact_sales (
  CONSTRAINT order_header_found EXPECT (order_date IS NOT NULL),
  CONSTRAINT amounts_add_up EXPECT (coalesce(gross_amount - discount_amount = net_amount, false)),
  CONSTRAINT customer_id_in_dimension EXPECT (_is_customer_id_known),
  CONSTRAINT customer_version_valid_on_order_date EXPECT (customer_key <> -1 OR NOT _is_customer_id_known),
  CONSTRAINT sales_rep_id_in_dimension EXPECT (_is_sales_rep_id_known),
  CONSTRAINT sales_rep_version_valid_on_order_date EXPECT (sales_rep_key <> -1 OR NOT _is_sales_rep_id_known),
  CONSTRAINT product_id_in_dimension EXPECT (product_key <> -1)
)
COMMENT 'Vendas (pedidos) do ERP, uma linha por (order_id, order_item_number). Data de negócio: order_date. Chaves da versão vigente na data; -1 = desconhecido.'
AS
WITH customer_ids AS (
  SELECT DISTINCT customer_id FROM ${catalog_core}.gold.dim_customer WHERE customer_key <> -1
),
sales_rep_ids AS (
  SELECT DISTINCT sales_rep_id FROM ${catalog_core}.gold.dim_sales_rep WHERE sales_rep_key <> -1
)
SELECT
  i.order_id,
  i.order_item_number,
  h.order_date,
  coalesce(c.customer_key, -1) AS customer_key,
  h.customer_id,
  coalesce(r.sales_rep_key, -1) AS sales_rep_key,
  h.sales_rep_id,
  coalesce(p.product_key, -1) AS product_key,
  i.product_id,
  h.order_status,
  h.order_status = 'CANCELADO' AS is_cancelled,
  i.quantity,
  -- Hectolitro: litros do SKU com 3 casas / 100 dá no máximo 5 casas; DECIMAL(18,5) soma sem arredondar.
  CAST(i.quantity * p.liters_per_sku / 100 AS DECIMAL(18, 5)) AS volume_hl,
  i.gross_amount,
  i.discount_amount,
  i.net_amount,
  ci.customer_id IS NOT NULL AS _is_customer_id_known,
  ri.sales_rep_id IS NOT NULL AS _is_sales_rep_id_known,
  -- Quando a linha mudou no lakehouse: a ingestão mais recente entre as linhas da silver que a formam.
  -- Determinístico (recalcular não muda o valor), ao contrário de current_timestamp().
  greatest(i._ingested_at, h._ingested_at) AS _updated_at
FROM ${catalog_core}.silver.erp_order_item i
-- LEFT JOIN no cabeçalho: um item que chegue antes do cabeçalho fica no fato (data nula) e a expectation order_header_found o conta.
LEFT JOIN ${catalog_core}.silver.erp_order_header h ON h.order_id = i.order_id
LEFT JOIN ${catalog_core}.gold.dim_customer c
  ON c.customer_id = h.customer_id AND h.order_date >= c.valid_from AND h.order_date < c.valid_to
LEFT JOIN customer_ids ci ON ci.customer_id = h.customer_id
LEFT JOIN ${catalog_core}.gold.dim_sales_rep r
  ON r.sales_rep_id = h.sales_rep_id AND h.order_date >= r.valid_from AND h.order_date < r.valid_to
LEFT JOIN sales_rep_ids ri ON ri.sales_rep_id = h.sales_rep_id
LEFT JOIN ${catalog_core}.gold.dim_product p ON p.product_id = i.product_id;
