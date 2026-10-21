-- Gold: calendário (SCD tipo 0, gerado). Intervalo largo de uma vez, para não depender de recarga (ADR-0006).
-- Feriados e calendário fiscal entram na fatia 5.
CREATE OR REFRESH MATERIALIZED VIEW gold.dim_date
COMMENT 'Um dia por linha, de 2024-01-01 a 2030-12-31. Chave: date (tipo DATE).'
AS SELECT
  d AS date,
  year(d) AS year,
  quarter(d) AS quarter,
  month(d) AS month,
  date_format(d, 'yyyy-MM') AS year_month,
  CASE month(d)
    WHEN 1 THEN 'janeiro' WHEN 2 THEN 'fevereiro' WHEN 3 THEN 'março' WHEN 4 THEN 'abril'
    WHEN 5 THEN 'maio' WHEN 6 THEN 'junho' WHEN 7 THEN 'julho' WHEN 8 THEN 'agosto'
    WHEN 9 THEN 'setembro' WHEN 10 THEN 'outubro' WHEN 11 THEN 'novembro' WHEN 12 THEN 'dezembro'
  END AS month_name,
  day(d) AS day_of_month,
  dayofweek(d) AS day_of_week,  -- 1 = domingo
  CASE dayofweek(d)
    WHEN 1 THEN 'domingo' WHEN 2 THEN 'segunda-feira' WHEN 3 THEN 'terça-feira' WHEN 4 THEN 'quarta-feira'
    WHEN 5 THEN 'quinta-feira' WHEN 6 THEN 'sexta-feira' WHEN 7 THEN 'sábado'
  END AS day_name,
  dayofweek(d) IN (1, 7) AS is_weekend,
  weekofyear(d) AS iso_week
FROM (SELECT explode(sequence(DATE'2024-01-01', DATE'2030-12-31')) AS d);
