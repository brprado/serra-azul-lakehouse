-- Silver: referência CEP -> município (fonte ref, mantida pela engenharia). É a régua da grafia da cidade.
CREATE OR REFRESH MATERIALIZED VIEW silver.ref_postal_code (
  CONSTRAINT postal_code_not_null EXPECT (postal_code IS NOT NULL) ON VIOLATION FAIL UPDATE
)
COMMENT 'CEP -> município/UF, extração mais recente da referência.'
AS SELECT
  cep AS postal_code,
  municipio AS city,
  uf AS state,
  _extraction_date
FROM bronze.ref_postal_code
WHERE _extraction_date = (SELECT max(_extraction_date) FROM bronze.ref_postal_code);
