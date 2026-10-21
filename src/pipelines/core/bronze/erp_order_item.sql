-- Bronze: cópia fiel do que chegou na landing, append-only, com metadados de origem (ADR-0004, ADR-0007).
-- Auto Loader garante que cada arquivo é lido uma única vez, então rodar o pipeline de novo não duplica.
-- Full refresh bloqueado: a landing guarda só 90 dias, e resetar a bronze apagaria o histórico que só ela tem (ADR-0007).
CREATE OR REFRESH STREAMING TABLE bronze.erp_order_item
COMMENT 'Landing erp/pedidos_itens sem transformação. Extração: incremental.'
TBLPROPERTIES ('pipelines.reset.allowed' = 'false')
AS SELECT
  *,
  _metadata.file_path AS _source_file,
  to_date(regexp_extract(_metadata.file_path, '/([0-9]{4}-[0-9]{2}-[0-9]{2})/', 1)) AS _extraction_date,
  current_timestamp() AS _ingested_at
FROM STREAM read_files('/Volumes/${catalog_core}/landing/erp/pedidos_itens/', format => 'parquet');
