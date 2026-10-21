-- Silver: vigências da hierarquia comercial (ADR-0006, ADR-0008). O histórico vem da vigência informada
-- pelo ERP, não da data em que a mudança foi digitada: reorganizações cadastradas com atraso ficam com a data certa.
CREATE OR REFRESH MATERIALIZED VIEW silver.erp_sales_hierarchy (
  CONSTRAINT sales_rep_id_not_null EXPECT (sales_rep_id IS NOT NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT valid_interval EXPECT (valid_to > valid_from),
  CONSTRAINT known_region EXPECT (commercial_region IN ('SUDESTE', 'SUL', 'NORDESTE', 'CENTRO-OESTE', 'NORTE', 'CONTAS NACIONAIS'))
)
COMMENT 'Vendedor -> supervisor -> gerente -> região comercial, por vigência. is_retroactive marca mudanças digitadas depois do início da vigência.'
AS SELECT
  cod_vendedor AS sales_rep_id,
  nome_vendedor AS sales_rep_name,
  cod_supervisor AS supervisor_id,
  nome_supervisor AS supervisor_name,
  cod_gerente AS manager_id,
  nome_gerente AS manager_name,
  regiao_comercial AS commercial_region,
  dt_inicio_vigencia AS valid_from,
  coalesce(date_add(dt_fim_vigencia, 1), DATE'9999-12-31') AS valid_to,
  dt_fim_vigencia IS NULL AS is_current,
  dt_registro AS registered_date,
  dt_registro > dt_inicio_vigencia AS is_retroactive,
  dt_atualizacao AS _source_updated_at,
  _extraction_date
FROM bronze.erp_sales_hierarchy
WHERE _extraction_date = (SELECT max(_extraction_date) FROM bronze.erp_sales_hierarchy);
