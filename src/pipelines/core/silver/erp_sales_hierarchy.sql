-- Silver: vigências da hierarquia comercial (ADR-0006, ADR-0008). O histórico vem da vigência informada
-- pelo ERP, não da data em que a mudança foi digitada: reorganizações cadastradas com atraso ficam com a data certa.
-- Vigência contígua (o valid_to de uma versão é o valid_from da seguinte) só é medida, por duas regras de aviso:
-- sobreposição e buraco. A dim_sales_rep recalcula o valid_to pelo início da versão seguinte e esconderia os dois
-- casos (ADR-0006). Buraco pode ser legítimo (vendedor desligado e recontratado); por isso é aviso, não descarte.
-- Reenvio da mesma extração (o mesmo arquivo com outro nome): linhas idênticas em todas as colunas de negócio viram
-- uma (ADR-0012), como em silver.erp_customer.
CREATE OR REFRESH MATERIALIZED VIEW silver.erp_sales_hierarchy (
  CONSTRAINT sales_rep_id_not_null EXPECT (sales_rep_id IS NOT NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT valid_interval EXPECT (valid_from IS NOT NULL AND valid_to > valid_from),
  CONSTRAINT no_overlap_with_next_version EXPECT (_next_valid_from IS NULL OR _next_valid_from >= valid_to),
  CONSTRAINT no_gap_to_next_version EXPECT (_next_valid_from IS NULL OR _next_valid_from <= valid_to),
  CONSTRAINT known_region EXPECT (commercial_region IS NOT NULL AND commercial_region IN ('SUDESTE', 'SUL', 'NORDESTE', 'CENTRO-OESTE', 'NORTE', 'CONTAS NACIONAIS'))
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
  -- Início da versão seguinte do mesmo vendedor (nulo na última); desempate como em silver.erp_customer.
  lead(dt_inicio_vigencia) OVER (
    PARTITION BY cod_vendedor ORDER BY dt_inicio_vigencia, dt_fim_vigencia NULLS LAST) AS _next_valid_from,
  dt_atualizacao AS _source_updated_at,
  _extraction_date
FROM (
  -- GROUP BY ALL, e não DISTINCT: ver silver.erp_customer.
  SELECT * EXCEPT (_source_file, _ingested_at)
  FROM bronze.erp_sales_hierarchy
  WHERE _extraction_date = (SELECT max(_extraction_date) FROM bronze.erp_sales_hierarchy)
  GROUP BY ALL
);
