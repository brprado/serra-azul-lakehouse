-- Silver: versões de cadastro de cliente, sem PII (ADR-0004, ADR-0006, ADR-0008).
-- A extração full traz o histórico inteiro, então a silver é recalculada a partir da extração mais recente.
-- O que acontece aqui, nesta ordem:
--   0. reenvio da mesma extração (o mesmo arquivo com outro nome): a bronze guarda as duas cópias, e linhas idênticas
--      em todas as colunas de negócio viram uma (ADR-0012). Um reenvio corrigido da mesma data não é tratado: a
--      correção chega na extração do dia seguinte;
--   1. valida o dígito verificador do CNPJ/CPF e guarda só o resultado (o documento não sai da bronze);
--      dígitos todos iguais (000.000.000-00) passam no módulo 11, mas não são documento real (ADR-0009);
--   2. troca a cidade digitada pela cidade oficial do CEP (correção de grafia nunca vira versão nova);
--   3. converte a vigência do ERP (intervalo fechado) para [valid_from, valid_to);
--   4. mede se as versões são contíguas (o valid_to de uma é o valid_from da seguinte), sem corrigir nem descartar:
--      sobreposição (a seguinte começa antes) e buraco (começa depois) têm regras de aviso separadas. A dim_customer
--      recalcula o valid_to pelo início da versão seguinte e esconderia os dois casos (ADR-0006).
CREATE OR REFRESH MATERIALIZED VIEW silver.erp_customer (
  CONSTRAINT customer_id_not_null EXPECT (customer_id IS NOT NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT valid_interval EXPECT (valid_from IS NOT NULL AND valid_to > valid_from),
  CONSTRAINT no_overlap_with_next_version EXPECT (_next_valid_from IS NULL OR _next_valid_from >= valid_to),
  CONSTRAINT no_gap_to_next_version EXPECT (_next_valid_from IS NULL OR _next_valid_from <= valid_to),
  CONSTRAINT postal_code_known EXPECT (city IS NOT NULL),
  CONSTRAINT document_valid EXPECT (is_document_valid)
)
COMMENT 'Clientes do ERP por versão de cadastro. Sem PII; cidade padronizada pelo CEP; vigência semiaberta.'
AS
WITH latest AS (
  -- Uma linha por combinação de colunas, sem as técnicas que mudam no reenvio (arquivo e instante da ingestão).
  -- GROUP BY ALL, e não DISTINCT: na prova de idempotência (2026-10-27), SELECT DISTINCT * EXCEPT (...) manteve as
  -- duas cópias nesta materialized view, embora a mesma consulta no SQL warehouse desse uma (ADR-0012).
  SELECT * EXCEPT (_source_file, _ingested_at), regexp_replace(cnpj_cpf, '[^0-9]', '') AS doc
  FROM bronze.erp_customer
  WHERE _extraction_date = (SELECT max(_extraction_date) FROM bronze.erp_customer)
  GROUP BY ALL
),
check_digits AS (
  -- Módulo 11: CNPJ usa pesos 5..2,9..2 (1º dígito) e 6..2,9..2 (2º); CPF usa 10..2 e 11..2.
  SELECT *,
    CASE length(doc)
      WHEN 14 THEN aggregate(sequence(1, 12), 0, (acc, i) -> acc + CAST(substr(doc, i, 1) AS INT)
                             * element_at(array(5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2), i))
      WHEN 11 THEN aggregate(sequence(1, 9), 0, (acc, i) -> acc + CAST(substr(doc, i, 1) AS INT) * (11 - i))
    END AS sum1,
    CASE length(doc)
      WHEN 14 THEN aggregate(sequence(1, 13), 0, (acc, i) -> acc + CAST(substr(doc, i, 1) AS INT)
                             * element_at(array(6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2), i))
      WHEN 11 THEN aggregate(sequence(1, 10), 0, (acc, i) -> acc + CAST(substr(doc, i, 1) AS INT) * (12 - i))
    END AS sum2
  FROM latest
)
SELECT
  c.cod_cliente AS customer_id,
  c.tipo_cliente AS customer_type,
  CASE c.tipo_cliente WHEN 'PDV' THEN 'DIRETO' WHEN 'DISTRIBUIDOR' THEN 'DISTRIBUIDOR' WHEN 'REDE' THEN 'REDE' END AS sales_channel,
  c.segmento AS trade_segment,
  c.tipo_pessoa AS person_type,
  coalesce(
    length(c.doc) IN (11, 14)
    AND c.doc <> repeat(substr(c.doc, 1, 1), length(c.doc))
    AND CAST(substr(c.doc, length(c.doc) - 1, 1) AS INT) = CASE WHEN c.sum1 % 11 < 2 THEN 0 ELSE 11 - c.sum1 % 11 END
    AND CAST(substr(c.doc, length(c.doc), 1) AS INT) = CASE WHEN c.sum2 % 11 < 2 THEN 0 ELSE 11 - c.sum2 % 11 END,
    false) AS is_document_valid,
  CASE WHEN c.tipo_pessoa = 'J' THEN c.razao_social END AS legal_name,  -- para pessoa física, a razão social é o nome (PII)
  c.nome_fantasia AS trade_name,
  c.cep AS postal_code,
  c.logradouro AS street,
  c.numero AS street_number,
  c.bairro AS district,
  r.city,
  c.municipio AS source_city,
  coalesce(r.state, c.uf) AS state,
  CASE
    WHEN coalesce(r.state, c.uf) IN ('SP', 'RJ', 'MG', 'ES') THEN 'SUDESTE'
    WHEN coalesce(r.state, c.uf) IN ('PR', 'SC', 'RS') THEN 'SUL'
    WHEN coalesce(r.state, c.uf) IN ('BA', 'SE', 'AL', 'PE', 'PB', 'RN', 'CE', 'PI', 'MA') THEN 'NORDESTE'
    WHEN coalesce(r.state, c.uf) IN ('DF', 'GO', 'MT', 'MS') THEN 'CENTRO-OESTE'
    WHEN coalesce(r.state, c.uf) IN ('AM', 'PA', 'AC', 'RO', 'RR', 'AP', 'TO') THEN 'NORTE'
  END AS geo_region,
  c.status,
  c.dt_cadastro AS registered_date,
  c.dt_inicio_vigencia AS valid_from,
  coalesce(date_add(c.dt_fim_vigencia, 1), DATE'9999-12-31') AS valid_to,
  c.dt_fim_vigencia IS NULL AS is_current,
  -- Início da versão seguinte do mesmo cliente (nulo na última). O desempate por fim de vigência (aberta por último)
  -- faz de duas versões que começam no mesmo dia uma sobreposição. Num empate total de início e fim, a contagem não
  -- muda, mas a linha marcada pode variar entre execuções (ADR-0010 D11).
  lead(c.dt_inicio_vigencia) OVER (
    PARTITION BY c.cod_cliente ORDER BY c.dt_inicio_vigencia, c.dt_fim_vigencia NULLS LAST) AS _next_valid_from,
  c.dt_atualizacao AS _source_updated_at,
  c._extraction_date
FROM check_digits c
LEFT JOIN silver.ref_postal_code r ON r.postal_code = c.cep;
