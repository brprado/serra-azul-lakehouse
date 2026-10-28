"""Verificação de unicidade — última tarefa do job serra_azul_daily (ADR-0010 D13).

Os fatos usam LEFT JOIN nos cabeçalhos e nas dimensões: a contagem de cada fato é a da silver enquanto a _key das
dimensões for única (ADR-0010 D6). Uma _key duplicada (por exemplo, duas versões de cadastro com o mesmo início,
ADR-0011) multiplicaria as linhas do fato em silêncio e inflaria "vendas por canal". As expectations não pegam isso:
medem linha a linha, e o problema é de conjunto.

Confere, depois do pipeline commercial:
- grão dos dois fatos sem linha repetida;
- _key única nas três dimensões conformadas;
- contagem de cada fato igual à da silver que o forma;
- nenhuma versão repetida (id, valid_from) na silver dos três cadastros. A dimensão já fica com uma por início
  (ADR-0012 D6), então a _key única não mostra mais esse caso; e é o sintoma de um reenvio que a silver não absorveu
  (ADR-0012 D5).

Roda depois da publicação: avisa, não impede que o consumidor veja o problema (ADR-0012).
Falha a tarefa (e o job) listando as violações. Não corrige nada: a correção é na fonte ou no pipeline.
Catálogos por parâmetro, nunca literais (convenções).
"""

import argparse
import re

from pyspark.sql import SparkSession

IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def excess_rows(table, keys):
    """Linhas a mais no grão: 0 quando cada chave aparece uma vez. GROUP BY agrupa os nulos, então chave nula repetida
    também conta."""
    return (f"SELECT coalesce(sum(n - 1), 0) FROM (SELECT count(*) AS n FROM {table} GROUP BY {', '.join(keys)} "
            f"HAVING count(*) > 1)")


def count_difference(table, source):
    return f"SELECT (SELECT count(*) FROM {table}) - (SELECT count(*) FROM {source})"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog-core", required=True)
    parser.add_argument("--catalog-commercial", required=True)
    args = parser.parse_args()
    for name in (args.catalog_core, args.catalog_commercial):
        if not IDENTIFIER.match(name):
            raise SystemExit(f"nome de catálogo inválido: {name!r}")
    core, commercial = args.catalog_core, args.catalog_commercial

    checks = [
        ("fact_sales: linhas repetidas no grão (order_id, order_item_number)",
         excess_rows(f"{commercial}.gold.fact_sales", ["order_id", "order_item_number"])),
        ("fact_invoice: linhas repetidas no grão (invoice_id, invoice_series, invoice_item_number)",
         excess_rows(f"{commercial}.gold.fact_invoice", ["invoice_id", "invoice_series", "invoice_item_number"])),
        ("dim_customer: customer_key repetida", excess_rows(f"{core}.gold.dim_customer", ["customer_key"])),
        ("dim_sales_rep: sales_rep_key repetida", excess_rows(f"{core}.gold.dim_sales_rep", ["sales_rep_key"])),
        ("dim_product: product_key repetida", excess_rows(f"{core}.gold.dim_product", ["product_key"])),
        ("fact_sales - silver.erp_order_item (linhas)",
         count_difference(f"{commercial}.gold.fact_sales", f"{core}.silver.erp_order_item")),
        ("fact_invoice - silver.erp_invoice_item (linhas)",
         count_difference(f"{commercial}.gold.fact_invoice", f"{core}.silver.erp_invoice_item")),
        ("silver.erp_customer: versões repetidas (customer_id, valid_from)",
         excess_rows(f"{core}.silver.erp_customer", ["customer_id", "valid_from"])),
        ("silver.erp_product: versões repetidas (product_id, valid_from)",
         excess_rows(f"{core}.silver.erp_product", ["product_id", "valid_from"])),
        ("silver.erp_sales_hierarchy: versões repetidas (sales_rep_id, valid_from)",
         excess_rows(f"{core}.silver.erp_sales_hierarchy", ["sales_rep_id", "valid_from"])),
    ]
    # Uma consulta só, com as dez contagens.
    sql = "\nUNION ALL\n".join(f"SELECT '{name}' AS regra, ({query}) AS valor" for name, query in checks)
    spark = SparkSession.builder.getOrCreate()
    results = [(r["regra"], r["valor"]) for r in spark.sql(sql).collect()]

    width = max(len(name) for name, _ in results)
    for name, value in results:
        print(f"{name.ljust(width)}  {value}  {'ok' if value == 0 else 'VIOLAÇÃO'}")
    violations = [f"{name}: {value}" for name, value in results if value != 0]
    if violations:
        raise RuntimeError("verificação de unicidade falhou:\n- " + "\n- ".join(violations))
    print("verificação de unicidade: ok")


if __name__ == "__main__":
    main()
