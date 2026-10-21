"""Carga inicial sintética do ERP — Cervejaria Serra Azul (fatia 1).

Simula o dump histórico que o ERP entrega no go-live: 18 meses de pedidos e notas fiscais
e a extração full dos cadastros (com o histórico de versões), em Parquet, na landing:

    <raiz>/erp/<dataset>/<data da extração>/
    <raiz>/ref/cep_municipio/<data da extração>/

Especificação: docs/caso-de-negocio.md (seção 5.1), ADR-0004 e ADR-0006.

Determinístico: toda aleatoriedade vem de um hash (xxhash64) da chave do registro com uma
semente fixa, então rodar de novo produz exatamente os mesmos arquivos (carga idempotente).
Tabelas mestras pequenas (produtos, hierarquia, CEPs) são montadas no driver; as volumosas
(clientes, pedidos, notas) são geradas com Spark, sem loops em Python sobre os dados.
"""

import argparse
import json
import math
import os
import random
import shutil
import unicodedata
from datetime import date, datetime, time, timedelta

import pandas as pd
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

SEED = 20261019
PRIME = 2147483647

# Viradas de trimestre em que a hierarquia comercial é reorganizada.
REORGS = [date(2025, 7, 1), date(2025, 10, 1), date(2026, 1, 1), date(2026, 4, 1), date(2026, 7, 1), date(2026, 10, 1)]


# ============================================================================ aleatoriedade
def u(salt, *cols):
    """Uniforme em [0, 1), determinística: a mesma chave sempre gera o mesmo número."""
    h = F.xxhash64(F.lit(SEED), F.lit(salt), *cols)
    return F.pmod(h, F.lit(PRIME)).cast("double") / F.lit(float(PRIME))


def u_pos(salt, *cols):
    """Uniforme em (0, 1], para o logaritmo do Box-Muller."""
    h = F.xxhash64(F.lit(SEED), F.lit(salt), *cols)
    return (F.pmod(h, F.lit(PRIME)).cast("double") + F.lit(1.0)) / F.lit(float(PRIME) + 1.0)


def normal(salt, *cols):
    """Normal padrão (Box-Muller sobre duas uniformes determinísticas)."""
    return F.sqrt(F.lit(-2.0) * F.log(u_pos(salt + ":a", *cols))) * F.cos(
        F.lit(2.0 * math.pi) * u(salt + ":b", *cols)
    )


def lognormal(median, sigma, salt, *cols):
    """Valores positivos com cauda longa (quantidades, frequências de compra)."""
    return F.lit(float(median)) * F.exp(F.lit(float(sigma)) * normal(salt, *cols))


def pick(ucol, options):
    """Escolha ponderada a partir de uma uniforme: options = [(valor, peso), ...]."""
    total = float(sum(w for _, w in options))
    expr, acc = None, 0.0
    for value, weight in options[:-1]:
        acc += weight / total
        cond = ucol < F.lit(acc)
        expr = F.when(cond, F.lit(value)) if expr is None else expr.when(cond, F.lit(value))
    last = F.lit(options[-1][0])
    return last if expr is None else expr.otherwise(last)


def case_map(col, mapping, default):
    expr = None
    for key, value in mapping.items():
        cond = col == F.lit(key)
        expr = F.when(cond, F.lit(value)) if expr is None else expr.when(cond, F.lit(value))
    return expr.otherwise(F.lit(default))


def at_time(date_col, salt, *cols, first_hour=7, hours=12):
    """Timestamp no dia informado, num horário comercial determinístico."""
    secs = F.lit(first_hour * 3600) + (u(salt, *cols) * F.lit(hours * 3600)).cast("long")
    return F.timestamp_seconds(F.unix_timestamp(date_col.cast("timestamp")) + secs)


def money(col):
    return col.cast("decimal(15,2)")


# ============================================================================ referências
REGION_SHARE = [("SUDESTE", 0.45), ("SUL", 0.20), ("NORDESTE", 0.18), ("CENTRO-OESTE", 0.10), ("NORTE", 0.07)]

# municipio, uf, regiao, peso na região, grafia alternativa usada por parte dos cadastros, prefixos de CEP
CITIES = [
    ("São Paulo", "SP", "SUDESTE", 0.22, "Sao Paulo", ["01", "02", "03", "04", "05"]),
    ("Campinas", "SP", "SUDESTE", 0.06, None, ["13"]),
    ("Guarulhos", "SP", "SUDESTE", 0.04, None, ["07"]),
    ("Ribeirão Preto", "SP", "SUDESTE", 0.03, "Ribeirao Preto", ["14"]),
    ("Sorocaba", "SP", "SUDESTE", 0.03, None, ["18"]),
    ("Santos", "SP", "SUDESTE", 0.03, None, ["11"]),
    ("São José dos Campos", "SP", "SUDESTE", 0.03, "S. J. dos Campos", ["12"]),
    ("Rio de Janeiro", "RJ", "SUDESTE", 0.16, None, ["20", "21", "22", "23"]),
    ("Niterói", "RJ", "SUDESTE", 0.03, "Niteroi", ["24"]),
    ("Duque de Caxias", "RJ", "SUDESTE", 0.02, None, ["25"]),
    ("Belo Horizonte", "MG", "SUDESTE", 0.09, "B. Horizonte", ["30", "31"]),
    ("Contagem", "MG", "SUDESTE", 0.03, None, ["32"]),
    ("Santa Luzia", "MG", "SUDESTE", 0.02, "Sta Luzia", ["33"]),
    ("Uberlândia", "MG", "SUDESTE", 0.03, "Uberlandia", ["38"]),
    ("Juiz de Fora", "MG", "SUDESTE", 0.02, None, ["36"]),
    ("Vitória", "ES", "SUDESTE", 0.03, "Vitoria", ["29"]),
    ("Vila Velha", "ES", "SUDESTE", 0.02, None, ["29"]),
    ("Porto Alegre", "RS", "SUL", 0.22, None, ["90", "91"]),
    ("Caxias do Sul", "RS", "SUL", 0.08, None, ["95"]),
    ("Pelotas", "RS", "SUL", 0.05, None, ["96"]),
    ("Santa Maria", "RS", "SUL", 0.04, "Sta Maria", ["97"]),
    ("Curitiba", "PR", "SUL", 0.22, None, ["80", "81", "82"]),
    ("Londrina", "PR", "SUL", 0.08, None, ["86"]),
    ("Maringá", "PR", "SUL", 0.06, "Maringa", ["87"]),
    ("Florianópolis", "SC", "SUL", 0.10, "Florianopolis", ["88"]),
    ("Joinville", "SC", "SUL", 0.09, None, ["89"]),
    ("Blumenau", "SC", "SUL", 0.06, None, ["89"]),
    ("Salvador", "BA", "NORDESTE", 0.20, None, ["40", "41"]),
    ("Feira de Santana", "BA", "NORDESTE", 0.07, "Feira de Sant'Ana", ["44"]),
    ("Recife", "PE", "NORDESTE", 0.16, None, ["50", "51", "52"]),
    ("Fortaleza", "CE", "NORDESTE", 0.16, None, ["60", "61"]),
    ("São Luís", "MA", "NORDESTE", 0.07, "Sao Luis", ["65"]),
    ("Natal", "RN", "NORDESTE", 0.07, None, ["59"]),
    ("João Pessoa", "PB", "NORDESTE", 0.07, "Joao Pessoa", ["58"]),
    ("Maceió", "AL", "NORDESTE", 0.07, "Maceio", ["57"]),
    ("Teresina", "PI", "NORDESTE", 0.06, None, ["64"]),
    ("Aracaju", "SE", "NORDESTE", 0.07, None, ["49"]),
    ("Brasília", "DF", "CENTRO-OESTE", 0.35, "Brasilia", ["70", "71", "72"]),
    ("Goiânia", "GO", "CENTRO-OESTE", 0.30, "Goiania", ["74"]),
    ("Campo Grande", "MS", "CENTRO-OESTE", 0.15, None, ["79"]),
    ("Cuiabá", "MT", "CENTRO-OESTE", 0.15, "Cuiaba", ["78"]),
    ("Anápolis", "GO", "CENTRO-OESTE", 0.05, "Anapolis", ["75"]),
    ("Manaus", "AM", "NORTE", 0.35, None, ["69"]),
    ("Belém", "PA", "NORTE", 0.30, "Belem", ["66"]),
    ("Porto Velho", "RO", "NORTE", 0.10, None, ["76"]),
    ("Palmas", "TO", "NORTE", 0.08, None, ["77"]),
    ("Macapá", "AP", "NORTE", 0.07, "Macapa", ["68"]),
    ("Rio Branco", "AC", "NORTE", 0.05, None, ["69"]),
    ("Boa Vista", "RR", "NORTE", 0.05, None, ["69"]),
]
CITY_INDEX = {c[0]: i for i, c in enumerate(CITIES)}
N_CEPS = 5000

# Distribuidores e redes: poucos clientes, muito volume (ver seção 1 do caso de negócio).
# idx, tipo, municipio, razão social, nome fantasia, tamanho (multiplica a frequência de pedidos)
SPECIAL_CUSTOMERS = [
    (0, "DISTRIBUIDOR", "São Paulo", "DISTRIBUIDORA PAULISTA DE BEBIDAS LTDA", "DPB DISTRIBUIDORA", 1.6),
    (1, "DISTRIBUIDOR", "Rio de Janeiro", "GUANABARA DISTRIBUICAO DE BEBIDAS LTDA", "GUANABARA BEBIDAS", 1.4),
    (2, "DISTRIBUIDOR", "Belo Horizonte", "MONTANHAS DISTRIBUIDORA DE BEBIDAS LTDA", "MONTANHAS", 1.2),
    (3, "DISTRIBUIDOR", "Campinas", "INTERIOR PAULISTA DISTRIBUIDORA LTDA", "INTERIOR SP", 0.8),
    (4, "DISTRIBUIDOR", "Porto Alegre", "PAMPA DISTRIBUIDORA DE BEBIDAS LTDA", "PAMPA BEBIDAS", 1.1),
    (5, "DISTRIBUIDOR", "Curitiba", "ARAUCARIA DISTRIBUICAO DE BEBIDAS LTDA", "ARAUCARIA", 1.0),
    (6, "DISTRIBUIDOR", "Florianópolis", "ILHA DISTRIBUIDORA DE BEBIDAS LTDA", "ILHA BEBIDAS", 0.7),
    (7, "DISTRIBUIDOR", "Salvador", "BAHIA NORTE DISTRIBUIDORA LTDA", "BAHIA NORTE", 1.2),
    (8, "DISTRIBUIDOR", "Recife", "CAPIBARIBE DISTRIBUIDORA DE BEBIDAS LTDA", "CAPIBARIBE", 1.0),
    (9, "DISTRIBUIDOR", "Brasília", "PLANALTO CENTRAL DISTRIBUIDORA LTDA", "PLANALTO", 0.9),
    (10, "DISTRIBUIDOR", "Goiânia", "CERRADO DISTRIBUIDORA DE BEBIDAS LTDA", "CERRADO BEBIDAS", 0.7),
    (11, "DISTRIBUIDOR", "Manaus", "RIO NEGRO DISTRIBUIDORA LTDA", "RIO NEGRO", 0.8),
    (12, "REDE", "São Paulo", "SUPERMERCADOS ESTRELA DO SUL S.A.", "ESTRELA SUPERMERCADOS", 10.0),
    (13, "REDE", "São Paulo", "REDE NOVA ERA VAREJO S.A.", "NOVA ERA", 7.0),
]
N_PDV = 39986

PDV_SEGMENTS = [("BAR", 0.38), ("RESTAURANTE", 0.17), ("MERCADO", 0.25), ("CONVENIENCIA", 0.12), ("ADEGA", 0.08)]

# Produtos: marca, categoria, líquidos e preço de lista por litro (BRL, com impostos)
BRANDS = {
    "PIL": dict(marca="SERRA AZUL PILSEN", categoria="CERVEJA MAINSTREAM", tipo="CERVEJA", abrev="CERV SA PILSEN", liquidos=["PILSEN"], preco_litro=10.4, share=0.42),
    "PMT": dict(marca="SERRA AZUL PURO MALTE", categoria="CERVEJA PREMIUM", tipo="CERVEJA", abrev="CERV SA P MALTE", liquidos=["PURO MALTE"], preco_litro=13.9, share=0.22),
    "ALV": dict(marca="ALVORADA", categoria="CERVEJA ARTESANAL", tipo="CERVEJA", abrev="CERV ALVORADA", liquidos=["IPA", "WEISS", "APA"], preco_litro=29.0, share=0.06),
    "NZ": dict(marca="NEVOA ZERO", categoria="CERVEJA SEM ALCOOL", tipo="CERVEJA SEM ALCOOL", abrev="CERV S/ALC NEVOA ZERO", liquidos=["PILSEN ZERO"], preco_litro=13.5, share=0.06),
    "GUA": dict(marca="GUARA SERRA", categoria="REFRIGERANTE", tipo="REFRIGERANTE", abrev="REFRIG GUARA SERRA", liquidos=["GUARANA"], preco_litro=6.4, share=0.18),
}
# chave: (embalagem, volume em ml, fator de preço por litro, abreviação antiga)
PACKAGES = {
    "LATA 269": ("LATA", 269, 1.12, "LT269"),
    "LATA 350": ("LATA", 350, 1.00, "LT350"),
    "LATA 473": ("LATA", 473, 1.00, "LT473"),
    "LONG NECK 330": ("LONG NECK", 330, 1.18, "LN330"),
    "GARRAFA RETORNAVEL 600": ("GARRAFA RETORNAVEL", 600, 0.82, "GFA RET 600"),
    "BARRIL 30L": ("BARRIL", 30000, 0.62, "BARRIL 30L"),
    "BARRIL 50L": ("BARRIL", 50000, 0.60, "BARRIL 50L"),
    "PET 2L": ("PET", 2000, 0.80, "PET 2L"),
}
OFFER = {
    "PIL": {"LATA 269": [6, 8, 12, 15, 18], "LATA 350": [6, 8, 10, 12, 15, 18, 24], "LATA 473": [6, 8, 12, 18],
            "LONG NECK 330": [6, 12], "GARRAFA RETORNAVEL 600": [12, 24], "BARRIL 30L": [1], "BARRIL 50L": [1]},
    "PMT": {"LATA 269": [8, 12], "LATA 350": [6, 8, 12, 15, 18], "LATA 473": [6, 8, 12], "LONG NECK 330": [4, 6, 12, 24],
            "GARRAFA RETORNAVEL 600": [12, 24], "BARRIL 30L": [1], "BARRIL 50L": [1]},
    "ALV": {"LATA 473": [4, 6, 12], "LONG NECK 330": [4, 6, 12, 24], "BARRIL 30L": [1]},
    "NZ": {"LATA 269": [6, 8, 12], "LATA 350": [6, 8, 12, 18], "LATA 473": [6, 12], "LONG NECK 330": [6, 12, 24]},
    "GUA": {"LATA 269": [6, 8, 12], "LATA 350": [6, 8, 12, 18], "PET 2L": [1, 6]},
}
# Afinidade de embalagem por grupo de cliente: G1 bar/restaurante, G2 varejo de vizinhança, G3 distribuidor, G4 rede
PKG_AFFINITY = {
    "G1": {"GARRAFA RETORNAVEL 600": 0.42, "BARRIL 30L": 0.10, "BARRIL 50L": 0.06, "LONG NECK 330": 0.16, "LATA 350": 0.14, "LATA 269": 0.06, "LATA 473": 0.04, "PET 2L": 0.02},
    "G2": {"LATA 350": 0.34, "LATA 269": 0.16, "LATA 473": 0.15, "LONG NECK 330": 0.11, "GARRAFA RETORNAVEL 600": 0.12, "PET 2L": 0.12, "BARRIL 30L": 0.0, "BARRIL 50L": 0.0},
    "G3": {"LATA 350": 0.30, "GARRAFA RETORNAVEL 600": 0.24, "LATA 269": 0.10, "LATA 473": 0.10, "LONG NECK 330": 0.10, "BARRIL 30L": 0.04, "BARRIL 50L": 0.03, "PET 2L": 0.09},
    "G4": {"LATA 350": 0.35, "LATA 269": 0.15, "LATA 473": 0.15, "LONG NECK 330": 0.15, "PET 2L": 0.15, "GARRAFA RETORNAVEL 600": 0.05, "BARRIL 30L": 0.0, "BARRIL 50L": 0.0},
}
PACK_POPULARITY = {1: 1.0, 4: 0.5, 6: 1.0, 8: 0.7, 10: 0.4, 12: 1.4, 15: 0.8, 18: 0.6, 24: 0.8}

# Sazonalidade e calendário
MONTH_FACTOR = {1: 1.12, 2: 1.15, 3: 1.02, 4: 0.95, 5: 0.90, 6: 0.90, 7: 0.86, 8: 0.88, 9: 0.95, 10: 1.00, 11: 1.05, 12: 1.22}
DOW_FACTOR = {1: 0.02, 2: 1.18, 3: 1.12, 4: 1.10, 5: 1.12, 6: 1.21, 7: 0.35}  # dayofweek: 1 = domingo
HOLIDAYS = [date(2025, 4, 21), date(2025, 5, 1), date(2025, 6, 19), date(2025, 9, 7), date(2025, 10, 12), date(2025, 11, 2),
            date(2025, 11, 15), date(2025, 11, 20), date(2025, 12, 25), date(2026, 1, 1), date(2026, 4, 3), date(2026, 4, 21),
            date(2026, 5, 1), date(2026, 6, 4), date(2026, 9, 7), date(2026, 10, 12)]
# Crescimento por região (multiplicador no início → no fim da janela): a expansão no Nordeste
GROWTH = {"NORDESTE": (0.84, 1.36), "SUDESTE": (0.96, 1.08), "SUL": (0.94, 1.10), "CENTRO-OESTE": (0.93, 1.12), "NORTE": (0.92, 1.12)}


def strip_accents(text):
    return "".join(ch for ch in unicodedata.normalize("NFKD", text) if not unicodedata.combining(ch))


def ean13(item_number):
    body = f"7895821{item_number:05d}"
    total = sum(int(d) * (1 if i % 2 == 0 else 3) for i, d in enumerate(body))
    return body + str((10 - total % 10) % 10)


def month_start(m):
    return date(2025 + (3 + m) // 12, (3 + m) % 12 + 1, 1)


def month_end(m):
    return month_start(m + 1) - timedelta(days=1)


def month_index_col(dt_col):
    return ((F.year(dt_col) - F.lit(2025)) * F.lit(12) + F.month(dt_col) - F.lit(4)).cast("int")


# ============================================================================ driver: CEPs
def build_ceps(rng):
    region_share = dict(REGION_SHARE)
    region_weight = {}
    for _, _, regiao, peso, _, _ in CITIES:
        region_weight[regiao] = region_weight.get(regiao, 0.0) + peso
    seen, ceps, n_by_city = set(), [], {}
    for idx, (municipio, uf, regiao, peso, _, prefixes) in enumerate(CITIES):
        n = max(15, round(N_CEPS * region_share[regiao] * peso / region_weight[regiao]))
        n_by_city[idx] = n
        k = 0
        while k < n:
            cep = f"{rng.choice(prefixes)}{rng.randint(0, 999):03d}-{rng.randint(0, 999):03d}"
            if cep in seen:
                continue
            seen.add(cep)
            ceps.append((idx, k, cep, municipio, uf))
            k += 1
    return ceps, n_by_city


def build_city_slots():
    rows = []
    for regiao, _ in REGION_SHARE:
        cities = [(i, c[3]) for i, c in enumerate(CITIES) if c[2] == regiao]
        total = sum(w for _, w in cities)
        acc, lo = 0.0, 0
        for i, w in cities:
            acc += w / total
            hi = round(acc * 1000)
            rows.extend((regiao, s, i) for s in range(lo, hi))
            lo = hi
    return rows


# ============================================================================ driver: produtos
def build_products(start, end):
    rng = random.Random(SEED + 1)
    products, item, sku = [], 0, 100100

    def add(brand, liquido, pkg, pack, launch, disc=None, promo=False, sku_code=None):
        nonlocal item, sku
        item += 1
        if sku_code is None:
            sku += rng.randint(1, 9)
            sku_code = str(sku)
        products.append(dict(cod=f"P{item:05d}", sku=sku_code, ean=ean13(item), brand=brand, liquido=liquido, pkg=pkg,
                             pack=pack, launch=launch, disc=disc, promo=promo, desc_change=None))
        return products[-1]

    for brand, packages in OFFER.items():
        for liquido in BRANDS[brand]["liquidos"]:
            for pkg, packs in packages.items():
                for pack in packs:
                    if brand == "NZ":  # lançamento da linha sem álcool (história 2)
                        launch = date(2025, 9, 1)
                    else:
                        launch = month_start(-rng.randint(6, 120))
                    add(brand, liquido, pkg, pack, launch)

    regular_old = [p for p in products if p["brand"] != "NZ"]
    for p, d in zip(rng.sample(regular_old, 6), [date(2025, 8, 31), date(2025, 10, 31), date(2026, 1, 31),
                                                  date(2026, 3, 31), date(2026, 5, 31), date(2026, 8, 31)]):
        p["disc"] = d

    # SKUs promocionais de verão: lançados em dez/2025, descontinuados em fev/2026
    promos = [add("PIL", "PILSEN", "LATA 350", 18, date(2025, 12, 1), date(2026, 2, 28), promo=True),
              add("PIL", "PILSEN", "LATA 269", 15, date(2025, 12, 1), date(2026, 2, 28), promo=True),
              add("GUA", "GUARANA", "LATA 350", 12, date(2025, 12, 1), date(2026, 2, 28), promo=True),
              add("PMT", "PURO MALTE", "LATA 350", 12, date(2025, 12, 1), date(2026, 2, 28), promo=True)]
    # Sujeira: o SKU de 3 promoções é reaproveitado para produtos novos (novo cod_produto, mesmo SKU)
    reused = [add("ALV", "IPA", "LATA 350", 6, date(2026, 5, 1), sku_code=promos[0]["sku"]),
              add("GUA", "GUARANA", "LATA 473", 6, date(2026, 5, 1), sku_code=promos[2]["sku"]),
              add("NZ", "PILSEN ZERO", "LATA 350", 15, date(2026, 5, 1), sku_code=promos[3]["sku"])]

    # Sujeira: EAN duplicado (erro de cadastro nunca corrigido)
    for a, b in [(products[3], products[40]), (products[10], products[62])]:
        b["ean"] = a["ean"]

    # Projeto de padronização de descrições em 2025-11-01: parte dos produtos antigos ganha nova versão
    for p in rng.sample([p for p in products if p["launch"] < date(2025, 11, 1) and not p["promo"]], 25):
        if p["disc"] is None or p["disc"] > date(2025, 11, 1):
            p["desc_change"] = date(2025, 11, 1)

    def desc_old(p):
        b = BRANDS[p["brand"]]
        emb = PACKAGES[p["pkg"]]
        head = "CHOPP " + b["abrev"].split(" ", 1)[1] if emb[0] == "BARRIL" else b["abrev"]
        liq = f" {p['liquido']}" if len(b["liquidos"]) > 1 else ""
        qty = "" if p["pack"] == 1 else f" C/{p['pack']}"
        return f"{head}{liq} {emb[3]}{qty}"

    def desc_new(p):
        b = BRANDS[p["brand"]]
        emb = PACKAGES[p["pkg"]]
        head = "CHOPE" if emb[0] == "BARRIL" else b["tipo"]
        liq = f" {p['liquido']}" if len(b["liquidos"]) > 1 else ""
        vol = f"{emb[1] // 1000}L" if emb[1] >= 1000 else f"{emb[1]}ML"
        qty = " UNIDADE" if p["pack"] == 1 else f" PACK {p['pack']}"
        promo = " PROMO VERAO" if p["promo"] else ""
        return f"{head} {b['marca']}{liq} {emb[0]} {vol}{qty}{promo}"

    versions = []
    for p in products:
        b = BRANDS[p["brand"]]
        emb = PACKAGES[p["pkg"]]
        new_style = p["launch"] >= date(2025, 11, 1)
        points = [(p["launch"], "ATIVO", desc_new(p) if new_style else desc_old(p), None)]
        if p["desc_change"]:
            points.append((p["desc_change"], "ATIVO", desc_new(p), None))
        if p["disc"]:
            last_desc = points[-1][2]
            points.append((p["disc"] + timedelta(days=1), "DESCONTINUADO", last_desc, p["disc"]))
        for i, (vstart, status, descricao, dt_desc) in enumerate(points):
            vend = points[i + 1][0] - timedelta(days=1) if i + 1 < len(points) else None
            versions.append((p["cod"], p["sku"], p["ean"], descricao, b["marca"], b["categoria"], p["liquido"], emb[0], emb[1],
                             p["pack"], status, p["launch"], dt_desc, vstart, vend,
                             datetime.combine(vstart, time(9, rng.randint(0, 59)))))

    # Tabela de sorteio: (mês, grupo, slot 0..999) -> produto, respeitando lançamento/descontinuação e participação de marca
    slots = []
    m_last = (end.year - 2025) * 12 + end.month - 4
    for m in range(m_last + 1):
        ms, me = month_start(m), month_end(m)
        active = [p for p in products if p["launch"] <= ms and (p["disc"] is None or p["disc"] >= me)]
        shares = {k: v["share"] for k, v in BRANDS.items()}
        shares["NZ"] = 0.0 if ms < date(2025, 9, 1) else 0.02 + 0.05 * min(1.0, (m - 5) / 13)
        for g, affinity in PKG_AFFINITY.items():
            weights = []
            for brand in BRANDS:
                raw = [(p, affinity[p["pkg"]] * PACK_POPULARITY[p["pack"]] * (2.5 if p["promo"] else 1.0))
                       for p in active if p["brand"] == brand]
                tot = sum(w for _, w in raw)
                if tot == 0 or shares[brand] == 0:
                    continue
                weights.extend((p, shares[brand] * w / tot) for p, w in raw if w > 0)
            total = sum(w for _, w in weights)
            acc, lo = 0.0, 0
            for p, w in weights:
                acc += w / total
                hi = round(acc * 1000)
                slots.extend((m, g, s, p["cod"]) for s in range(lo, hi))
                lo = hi

    info = []
    for p in products:
        b = BRANDS[p["brand"]]
        emb = PACKAGES[p["pkg"]]
        litros = emb[1] * p["pack"] / 1000.0
        info.append((p["cod"], p["brand"], emb[0], litros, round(b["preco_litro"] * litros * emb[2], 2)))
    return versions, slots, info, products, promos, reused


# ============================================================================ driver: hierarquia
def build_hierarchy(start, end, faker_cls):
    rng = random.Random(SEED + 2)
    fk = faker_cls("pt_BR")
    fk.seed_instance(SEED + 2)

    def nome():
        return f"{fk.first_name()} {fk.last_name()}"

    regions = ["SUDESTE", "SUL", "NORDESTE", "CENTRO-OESTE", "NORTE", "CONTAS NACIONAIS"]
    n_sup = {"SUDESTE": 8, "SUL": 4, "NORDESTE": 4, "CENTRO-OESTE": 2, "NORTE": 2, "CONTAS NACIONAIS": 1}
    n_rep = {"SUDESTE": 56, "SUL": 26, "NORDESTE": 24, "CENTRO-OESTE": 12, "NORTE": 8, "CONTAS NACIONAIS": 4}
    managers = {r: (f"G{i + 1:02d}", nome()) for i, r in enumerate(regions)}
    ne_new_manager = ("G07", nome())
    sups, s = {}, 0
    for r in regions:
        sups[r] = []
        for _ in range(n_sup[r]):
            s += 1
            sups[r].append((f"S{s:03d}", nome()))
    sup_name = {c: n for r in regions for c, n in sups[r]}

    def manager(region, d):
        if region == "NORDESTE" and d >= REORGS[2]:  # troca do gerente regional do NE em 2026-01-01
            return ne_new_manager
        return managers[region]

    reps, versions, counter = {}, [], [0]

    def hire(region, d, registro):
        counter[0] += 1
        cod = f"V{counter[0]:04d}"
        reps[cod] = dict(nome=nome(), region=region, sup=rng.choice(sups[region])[0], start=d, registro=registro, active=True)

    def close(cod, end_d):
        r = reps[cod]
        g = manager(r["region"], r["start"])
        versions.append((cod, r["nome"], r["sup"], sup_name[r["sup"]], g[0], g[1], r["region"], r["start"], end_d,
                         r["registro"], datetime.combine(r["registro"], time(10, rng.randint(0, 59)))))

    first_hire, last_hire = date(2016, 1, 4), date(2025, 3, 31)
    for r in regions:
        for _ in range(n_rep[r]):
            d = first_hire + timedelta(days=rng.randint(0, (last_hire - first_hire).days))
            hire(r, d, d - timedelta(days=rng.randint(0, 10)))

    late = 0
    for q in REORGS:
        prev = q - timedelta(days=1)

        def registro():
            nonlocal late
            if rng.random() < 0.20:  # sujeira: mudança cadastrada dias depois do início da vigência
                late += 1
                return q + timedelta(days=rng.randint(3, 15))
            return q - timedelta(days=rng.randint(0, 5))

        active = sorted(c for c, r in reps.items() if r["active"])
        leaving = rng.choice([c for c in active if reps[c]["region"] != "CONTAS NACIONAIS"])
        close(leaving, prev)
        reps[leaving]["active"] = False
        active.remove(leaving)

        changes = {}
        for c in active:
            r = reps[c]
            if len(sups[r["region"]]) > 1 and rng.random() < 0.45:
                changes[c] = (r["region"], rng.choice([x for x, _ in sups[r["region"]] if x != r["sup"]]))
        if q == REORGS[4]:  # 2026-07-01: 6 vendedores do NE passam para o SE (história 3)
            for c in rng.sample([c for c in active if reps[c]["region"] == "NORDESTE"], 6):
                changes[c] = ("SUDESTE", rng.choice(sups["SUDESTE"])[0])
        if q == REORGS[2]:  # novo gerente no NE: todos os vendedores do NE ganham versão nova
            for c in active:
                if reps[c]["region"] == "NORDESTE" and c not in changes:
                    changes[c] = ("NORDESTE", reps[c]["sup"])
        for c, (region, sup) in changes.items():
            close(c, prev)
            reps[c].update(region=region, sup=sup, start=q, registro=registro())

        if q <= REORGS[4]:  # expansão do Nordeste
            for _ in range(2):
                hire("NORDESTE", q, registro())
        hire(rng.choice(["SUDESTE", "SUL", "CENTRO-OESTE", "NORTE"]), q, registro())

    for c, r in reps.items():
        if r["active"]:
            close(c, None)

    # Carteira: em cada período entre reorganizações, os vendedores ativos de cada região comercial
    period_starts = [start] + REORGS
    slots = []
    for p, ps in enumerate(period_starts):
        by_region = {}
        for v in versions:
            if v[7] <= ps and (v[8] is None or v[8] >= ps):
                by_region.setdefault(v[6], []).append(v[0])
        for region, cods in by_region.items():
            for i, cod in enumerate(sorted(cods)):
                slots.append((region, p, i, len(cods), cod))
    return versions, slots, late


# ============================================================================ Spark: clientes
PERSONA_SCHEMA = T.StructType([
    T.StructField("nome", T.StringType()), T.StructField("primeiro_nome", T.StringType()),
    T.StructField("sobrenome", T.StringType()), T.StructField("empresa", T.StringType()),
    T.StructField("logradouro", T.StringType()), T.StructField("bairro", T.StringType()),
    T.StructField("telefone", T.StringType()), T.StructField("email", T.StringType()),
    T.StructField("cnpj", T.StringType()), T.StructField("cpf", T.StringType()),
])


@F.pandas_udf(PERSONA_SCHEMA)
def persona(seeds: pd.Series) -> pd.DataFrame:
    """Dados de pessoa/empresa fake (Faker pt_BR), determinísticos pela semente de cada linha."""
    from faker import Faker

    fk = Faker("pt_BR")
    domains = ["gmail.com", "hotmail.com", "outlook.com", "yahoo.com.br", "uol.com.br", "bol.com.br"]
    rows = []
    for s in seeds:
        fk.seed_instance(int(s))
        first, last = fk.first_name(), fk.last_name()
        slug = lambda t: "".join(ch for ch in unicodedata.normalize("NFKD", t.lower()) if ch.isalnum())
        rows.append(dict(
            nome=f"{first} {last}", primeiro_nome=first, sobrenome=last, empresa=fk.company(),
            logradouro=fk.street_name(), bairro=fk.bairro(), telefone=fk.phone_number(),
            email=f"{slug(first)}.{slug(last)}{fk.random_int(1, 99)}@{fk.random_element(domains)}",
            cnpj="".join(ch for ch in fk.cnpj() if ch.isdigit()), cpf="".join(ch for ch in fk.cpf() if ch.isdigit()),
        ))
    return pd.DataFrame(rows, columns=[f.name for f in PERSONA_SCHEMA.fields])


def build_customer_base(spark, start, end, n_pdv, city_slots_df, cities_df):
    special_df = spark.createDataFrame(
        [(i, t, CITY_INDEX[m], r, f, s) for i, t, m, r, f, s in SPECIAL_CUSTOMERS],
        "idx long, tipo_esp string, city_esp int, razao_esp string, fantasia_esp string, tamanho double",
    )
    n_total = len(SPECIAL_CUSTOMERS) + n_pdv
    cod = F.format_string("C%06d", F.col("idx") + 1)
    base = (spark.range(n_total, numPartitions=16).withColumnRenamed("id", "idx")
            .join(F.broadcast(special_df), "idx", "left")
            .withColumn("cod_cliente", cod)
            .withColumn("tipo_cliente", F.coalesce(F.col("tipo_esp"), F.lit("PDV")))
            .withColumn("regiao_pdv", pick(u("regiao", cod), REGION_SHARE))
            .withColumn("slot_cidade", (u("cidade", cod) * 1000).cast("int")))
    slots = city_slots_df.select(F.col("regiao").alias("regiao_pdv"), F.col("slot").alias("slot_cidade"), F.col("city_idx").alias("city_pdv"))
    base = (base.join(F.broadcast(slots), ["regiao_pdv", "slot_cidade"], "left")
            .withColumn("city_idx0", F.coalesce(F.col("city_esp"), F.col("city_pdv")))
            .join(F.broadcast(cities_df.select(F.col("city_idx").alias("city_idx0"), F.col("uf").alias("uf0"),
                                               "regiao", F.col("variante").alias("variante0"))), "city_idx0"))

    is_pdv = F.col("tipo_cliente") == "PDV"
    days_window = (end - start).days
    old_from = date(2012, 1, 1)
    p_new = F.when(F.col("regiao") == "NORDESTE", 0.28).otherwise(0.12)
    dt_new = F.date_add(F.lit(start), (u("dtnovo", cod) * days_window).cast("int"))
    dt_old = F.date_add(F.lit(old_from), (u("dtantigo", cod) * ((start - old_from).days - 1)).cast("int"))
    dt_special = F.date_add(F.lit(date(2008, 1, 1)), (u("dtesp", cod) * 2500).cast("int"))
    base = base.withColumn(
        "dt_cadastro", F.when(~is_pdv, dt_special).when(u("novo", cod) < p_new, dt_new).otherwise(dt_old))

    inat_from = F.greatest(F.date_add(F.col("dt_cadastro"), 30), F.lit(start))
    inat_span = F.datediff(F.lit(end), inat_from)
    base = base.withColumn(
        "dt_inativacao",
        F.when(is_pdv & (u("inativa", cod) < 0.03) & (inat_span > 5),
               F.date_add(inat_from, (u("dtinat", cod) * inat_span).cast("int"))))

    seg0 = F.when(F.col("tipo_cliente") == "DISTRIBUIDOR", F.lit("DISTRIBUIDOR")).when(
        F.col("tipo_cliente") == "REDE", F.lit("SUPERMERCADO")).otherwise(pick(u("segmento", cod), PDV_SEGMENTS))
    rate_pdv = F.lit(0.0405) * lognormal(1.0, 0.55, "freq", cod) / F.lit(math.exp(0.55 ** 2 / 2))
    base = (base
            .withColumn("segmento0", seg0)
            .withColumn("base_rate", F.when(F.col("tipo_cliente") == "DISTRIBUIDOR", F.lit(0.85) * F.col("tamanho"))
                        .when(F.col("tipo_cliente") == "REDE", F.col("tamanho")).otherwise(rate_pdv))
            .withColumn("is_cpf", is_pdv & (u("pessoa_fisica", cod) < 0.08))
            .withColumn("doc_invalido", ~F.col("is_cpf") & (u("doc_invalido", cod) < 0.02))
            .withColumn("grafia0", F.col("variante0").isNotNull() & (u("grafia", cod) < 0.30))
            .withColumn("fora_da_carga", is_pdv & (F.col("dt_cadastro") >= F.lit(end - timedelta(days=60)))
                        & (u("orfao", cod) < 0.35)))
    return base.select("idx", "cod_cliente", "tipo_cliente", "regiao", "city_idx0", "uf0", "variante0", "segmento0",
                       "dt_cadastro", "dt_inativacao", "base_rate", "is_cpf", "doc_invalido", "grafia0", "fora_da_carga",
                       "razao_esp", "fantasia_esp")


def build_customer_versions(base, start, end, city_slots_df, cities_df, ceps_df):
    """Uma linha por versão de cadastro: eventos de mudança acumulados por data (extração full com histórico)."""
    cod = F.col("cod_cliente")
    is_pdv = F.col("tipo_cliente") == "PDV"
    span = F.datediff(F.lit(end), F.col("dt_cadastro"))
    n_by_type = {
        "contato": F.when(u("ncontato", cod) < 0.10, 2).when(u("ncontato", cod) < 0.35, 1).otherwise(0),
        "endereco": F.when(is_pdv & (u("nendereco", cod) < 0.15), 1).otherwise(0),
        "razao": F.when(is_pdv & (u("nrazao", cod) < 0.08), 1).otherwise(0),
        "segmento": F.when(is_pdv & (u("nsegmento", cod) < 0.05), 1).otherwise(0),
        "correcao": F.when(F.col("grafia0") & (u("ncorrecao", cod) < 0.5), 1).otherwise(0),
    }
    events = None
    for tipo, n in n_by_type.items():
        # filtra n >= 1 antes do explode: no Spark, sequence(1, 0) devolve [1, 0], não uma lista vazia
        ev = (base.where((span >= 3) & (n >= 1))
              .select(cod, "dt_cadastro", F.explode(F.sequence(F.lit(1), n)).alias("k"))
              .select(cod, F.lit(tipo).alias("tipo"), F.date_add(F.col("dt_cadastro"), (F.lit(1) + (
                  u("dtevento", cod, F.lit(tipo), F.col("k")) * (F.datediff(F.lit(end), F.col("dt_cadastro")) - 1))
                  .cast("int"))).alias("ev_date")))
        events = ev if events is None else events.unionByName(ev)
    events = (events
              .unionByName(base.where(F.col("dt_inativacao").isNotNull()).select(cod, F.lit("inativacao").alias("tipo"),
                                                                             F.col("dt_inativacao").alias("ev_date")))
              .unionByName(base.select(cod, F.lit("criacao").alias("tipo"), F.col("dt_cadastro").alias("ev_date"))))

    types = ["contato", "endereco", "razao", "segmento", "correcao", "inativacao"]
    daily = events.groupBy("cod_cliente", "ev_date").agg(
        *[F.sum(F.when(F.col("tipo") == t, 1).otherwise(0)).alias(f"d_{t}") for t in types])
    w = Window.partitionBy("cod_cliente").orderBy("ev_date")
    wc = w.rowsBetween(Window.unboundedPreceding, Window.currentRow)
    ver = daily.select(
        "cod_cliente", F.col("ev_date").alias("dt_inicio_vigencia"),
        F.date_sub(F.lead("ev_date").over(w), 1).alias("dt_fim_vigencia"),
        F.row_number().over(w).alias("versao"),
        *[F.sum(f"d_{t}").over(wc).alias(f"c_{t}") for t in types])
    ver = ver.join(base, "cod_cliente")

    # Endereço da versão: muda quando há evento de endereço; 10% das mudanças são para outra cidade da região
    moved = is_pdv & (F.col("c_endereco") >= 1) & (u("mudou_cidade", cod, F.col("c_endereco")) < 0.10)
    slots = city_slots_df.select(F.col("regiao"), F.col("slot").alias("slot2"), F.col("city_idx").alias("city_mv"))
    ver = (ver.withColumn("slot2", (u("cidade2", cod, F.col("c_endereco")) * 1000).cast("int"))
           .join(F.broadcast(slots), ["regiao", "slot2"], "left")
           .withColumn("city_idx", F.when(moved, F.col("city_mv")).otherwise(F.col("city_idx0")))
           .join(F.broadcast(cities_df.select("city_idx", "municipio_canonico", "uf", "variante", "n_ceps")), "city_idx")
           .withColumn("k_cep", (u("cep", cod, F.col("c_endereco")) * F.col("n_ceps")).cast("int"))
           .join(F.broadcast(ceps_df.select("city_idx", F.col("k").alias("k_cep"), "cep")), ["city_idx", "k_cep"]))

    p_end = persona(F.xxhash64(F.lit(SEED), F.lit("p_endereco"), cod, F.col("c_endereco")))
    p_raz = persona(F.xxhash64(F.lit(SEED), F.lit("p_razao"), cod, F.col("c_razao")))
    p_con = persona(F.xxhash64(F.lit(SEED), F.lit("p_contato"), cod, F.col("c_contato")))
    p_doc = persona(F.xxhash64(F.lit(SEED), F.lit("p_documento"), cod))
    ver = (ver.withColumn("pe", p_end).withColumn("pr", p_raz).withColumn("pc", p_con).withColumn("pd", p_doc))

    segmento = F.when(~is_pdv | (F.col("c_segmento") == 0), F.col("segmento0")).otherwise(
        pick(u("segmento2", cod, F.col("c_segmento")), PDV_SEGMENTS))
    doc_ok = F.when(F.col("is_cpf"), F.col("pd.cpf")).otherwise(F.col("pd.cnpj"))
    doc_bad = F.concat(F.expr("substring(doc_ok, 1, length(doc_ok) - 1)"),
                       ((F.expr("cast(substring(doc_ok, -1, 1) as int)") + 1) % 10).cast("string"))
    fantasia_pdv = (F.when(F.col("segmento") == "BAR", F.concat(F.lit("BAR DO "), F.upper("pr.primeiro_nome")))
                    .when(F.col("segmento") == "RESTAURANTE", F.concat(F.lit("RESTAURANTE "), F.upper("pr.sobrenome")))
                    .when(F.col("segmento") == "MERCADO", F.concat(F.lit("MERCADO "), F.upper("pr.sobrenome")))
                    .when(F.col("segmento") == "CONVENIENCIA", F.concat(F.lit("CONVENIENCIA "), F.upper("pr.primeiro_nome")))
                    .otherwise(F.concat(F.lit("ADEGA "), F.upper("pr.sobrenome"))))
    dirty = (F.col("grafia0") & (F.col("c_endereco") == 0) & (F.col("c_correcao") == 0) & F.col("variante").isNotNull())

    out = (ver
           .withColumn("segmento", segmento)
           .withColumn("doc_ok", doc_ok)
           .select(
               "cod_cliente", "tipo_cliente", "segmento",
               F.when(F.col("is_cpf"), F.lit("F")).otherwise(F.lit("J")).alias("tipo_pessoa"),
               F.when(F.col("doc_invalido"), doc_bad).otherwise(F.col("doc_ok")).alias("cnpj_cpf"),
               F.when(~is_pdv, F.col("razao_esp")).when(F.col("is_cpf"), F.upper("pr.nome"))
               .otherwise(F.upper("pr.empresa")).alias("razao_social"),
               F.when(~is_pdv, F.col("fantasia_esp")).otherwise(fantasia_pdv).alias("nome_fantasia"),
               F.col("pc.nome").alias("nome_responsavel"), F.col("pc.email").alias("email"),
               F.col("pc.telefone").alias("telefone"),
               "cep", F.col("pe.logradouro").alias("logradouro"),
               (F.lit(1) + (u("numero", cod, F.col("c_endereco")) * 1800).cast("int")).cast("string").alias("numero"),
               F.col("pe.bairro").alias("bairro"),
               F.when(dirty, F.col("variante")).otherwise(F.col("municipio_canonico")).alias("municipio"),
               "uf",
               F.when(F.col("c_inativacao") >= 1, F.lit("INATIVO")).otherwise(F.lit("ATIVO")).alias("status"),
               "dt_cadastro", "dt_inicio_vigencia", "dt_fim_vigencia",
               at_time(F.col("dt_inicio_vigencia"), "atu_cliente", cod, F.col("versao")).alias("dt_atualizacao"),
               "fora_da_carga", "c_contato", "c_endereco", "c_razao", "c_segmento", "c_correcao", "versao"))
    return out


# ============================================================================ Spark: pedidos
def build_orders(spark, base, start, end, rep_slots_df):
    days = (spark.sql(f"SELECT explode(sequence(DATE'{start}', DATE'{end}')) AS dt_pedido")
            .withColumn("dow", F.dayofweek("dt_pedido"))
            .withColumn("mes", F.month("dt_pedido")))
    dow_mean = sum(DOW_FACTOR.values()) / 7.0
    f_dow = case_map(F.col("dow"), {k: v / dow_mean for k, v in DOW_FACTOR.items()}, 1.0)
    f_mes = case_map(F.col("mes"), MONTH_FACTOR, 1.0)
    dt = F.col("dt_pedido")
    f_evento = (F.when(dt.isin(*[F.lit(h) for h in HOLIDAYS]), 0.1)
                .when(dt.between(F.lit(date(2026, 2, 14)), F.lit(date(2026, 2, 17))), 0.4)   # Carnaval
                .when(dt.between(F.lit(date(2026, 2, 7)), F.lit(date(2026, 2, 13))), 1.35)   # abastecimento pré-Carnaval
                .when(dt.isin(F.lit(date(2025, 12, 24)), F.lit(date(2025, 12, 31))), 0.3)
                .when(dt.between(F.lit(date(2025, 12, 10)), F.lit(date(2025, 12, 23))), 1.15)
                .otherwise(1.0))
    days = days.withColumn("f_dia", f_dow * f_mes * f_evento).withColumn(
        "t", F.datediff(dt, F.lit(start)) / F.lit(float((end - start).days)))

    regiao = F.col("regiao")
    g0 = case_map(regiao, {k: v[0] for k, v in GROWTH.items()}, 1.0)
    g1 = case_map(regiao, {k: v[1] for k, v in GROWTH.items()}, 1.0)
    f_regiao = (F.when((regiao == "NORDESTE") & (F.col("mes") == 6), 1.30)               # festas juninas
                .when((regiao == "SUL") & F.col("mes").isin(6, 7, 8), 0.92)               # inverno no Sul
                .otherwise(1.0))
    cod = F.col("cod_cliente")
    lam = F.col("base_rate") * F.col("f_dia") * f_regiao * (g0 + (g1 - g0) * F.col("t"))
    grid = (base.select("cod_cliente", "tipo_cliente", "regiao", "uf0", "segmento0", "dt_cadastro", "dt_inativacao",
                        "base_rate", "fora_da_carga")
            .crossJoin(days)
            .where((dt >= F.col("dt_cadastro")) & (F.col("dt_inativacao").isNull() | (dt < F.col("dt_inativacao"))))
            .withColumn("n", F.floor(lam + u("pedido", cod, dt)).cast("int"))
            .where(F.col("n") >= 1)
            .withColumn("seq", F.explode(F.sequence(F.lit(1), F.col("n")))))

    w_id = Window.orderBy("dt_pedido", "cod_cliente", "seq")
    ped = grid.withColumn("num_pedido", F.format_string("PV%08d", F.row_number().over(w_id)))
    oid = F.col("num_pedido")
    is_pdv = F.col("tipo_cliente") == "PDV"

    period = F.lit(0)
    for i, q in enumerate(REORGS):
        period = F.when(dt >= F.lit(q), F.lit(i + 1)).otherwise(period)
    ped = (ped
           .withColumn("month_idx", month_index_col(dt))
           .withColumn("periodo", period)
           .withColumn("regiao_comercial", F.when(F.col("tipo_cliente") == "REDE", F.lit("CONTAS NACIONAIS")).otherwise(regiao))
           .withColumn("grp", F.when(F.col("tipo_cliente") == "DISTRIBUIDOR", "G3").when(F.col("tipo_cliente") == "REDE", "G4")
                       .when(F.col("segmento0").isin("BAR", "RESTAURANTE"), "G1").otherwise("G2")))
    n_slots = rep_slots_df.select("regiao_comercial", "periodo", "n_slots").distinct()
    ped = (ped.join(F.broadcast(n_slots), ["regiao_comercial", "periodo"])
           .withColumn("slot", F.pmod(F.xxhash64(F.lit(SEED), cod), F.col("n_slots")).cast("int"))
           .join(F.broadcast(rep_slots_df.select("regiao_comercial", "periodo", "slot", "cod_vendedor")),
                 ["regiao_comercial", "periodo", "slot"]))

    n_itens = (F.when(F.col("tipo_cliente") == "DISTRIBUIDOR", F.lit(30) + (u("nitens", oid) * 21).cast("int"))
               .when(F.col("tipo_cliente") == "REDE", F.lit(18) + (u("nitens", oid) * 15).cast("int"))
               .otherwise(F.least(F.lit(12), F.lit(1) + F.floor(lognormal(2.5, 0.6, "nitens", oid)).cast("int"))))
    lag = pick(u("lag_faturamento", oid), [(0, 0.40), (1, 0.45), (2, 0.15)])
    ped = (ped
           .withColumn("n_itens", n_itens)
           .withColumn("dt_criacao", at_time(dt, "hora_pedido", oid))
           .withColumn("cancela_antes", u("cancela_antes", oid) < 0.02)
           .withColumn("parcial", ~F.col("cancela_antes") & (u("parcial", oid) < 0.05))
           .withColumn("cancela_depois", ~F.col("cancela_antes") & ~F.col("parcial") & (u("cancela_depois", oid) < 0.003))
           .withColumn("devolve", ~F.col("cancela_antes") & ~F.col("parcial") & ~F.col("cancela_depois")
                       & (u("devolucao", oid) < 0.012))
           .withColumn("dt_fat1", F.date_add(dt, lag.cast("int")))
           .withColumn("dt_fat2", F.date_add(F.col("dt_fat1"), (F.lit(1) + (u("lag2", oid) * 3).cast("int"))))
           .withColumn("dt_cancela", F.when(F.col("cancela_antes"), F.date_add(dt, (u("lag_cancela", oid) * 2).cast("int")))
                       .otherwise(F.date_add(F.col("dt_fat1"), F.lit(1) + (u("lag_cancela", oid) * 10).cast("int"))))
           .withColumn("dt_devolucao", F.date_add(F.col("dt_fat1"), F.lit(3) + (u("lag_devolucao", oid) * 18).cast("int")))
           .withColumn("faturado1", ~F.col("cancela_antes") & (F.col("dt_fat1") <= F.lit(end)))
           .withColumn("faturado2", F.col("parcial") & F.col("faturado1") & (F.col("dt_fat2") <= F.lit(end)))
           .withColumn("cancelado_depois", F.col("cancela_depois") & F.col("faturado1") & (F.col("dt_cancela") <= F.lit(end)))
           .withColumn("devolvido", F.col("devolve") & F.col("faturado1") & (F.col("dt_devolucao") <= F.lit(end))))
    status = (F.when(F.col("cancela_antes") | F.col("cancelado_depois"), F.lit("CANCELADO"))
              .when(~F.col("faturado1"), F.lit("ABERTO"))
              .when(F.col("parcial") & ~F.col("faturado2"), F.lit("PARCIALMENTE FATURADO"))
              .otherwise(F.lit("FATURADO")))
    last_event = (F.when(F.col("cancela_antes") | F.col("cancelado_depois"), F.col("dt_cancela"))
                  .when(F.col("faturado2"), F.col("dt_fat2")).when(F.col("faturado1"), F.col("dt_fat1")))
    ped = (ped.withColumn("status", status)
           .withColumn("dt_atualizacao", F.when(last_event.isNull(), F.col("dt_criacao"))
                       .otherwise(F.greatest(F.col("dt_criacao"), at_time(last_event, "hora_evento", oid, first_hour=8, hours=10)))))
    return ped.select("num_pedido", "cod_cliente", "tipo_cliente", "regiao", "uf0", "grp", "fora_da_carga", "cod_vendedor",
                      "dt_pedido", "month_idx", "n_itens", "dt_criacao", "status", "dt_atualizacao", "parcial",
                      "faturado1", "faturado2", "cancelado_depois", "devolvido", "dt_fat1", "dt_fat2", "dt_cancela",
                      "dt_devolucao")


def build_items(ped, product_slots_df, product_info_df):
    oid = F.col("num_pedido")
    it = (ped.select("num_pedido", "regiao", "grp", "dt_pedido", "month_idx", "dt_criacao",
                     F.explode(F.sequence(F.lit(1), F.col("n_itens"))).alias("linha"))
          .withColumn("slot", (u("produto", oid, F.col("linha")) * 1000).cast("int"))
          .join(F.broadcast(product_slots_df), ["month_idx", "grp", "slot"])
          .join(F.broadcast(product_info_df), "cod_produto"))
    first = Window.partitionBy("num_pedido", "cod_produto").orderBy("linha")
    it = it.withColumn("rn", F.row_number().over(first)).where(F.col("rn") == 1)
    it = it.withColumn("item_pedido", F.row_number().over(Window.partitionBy("num_pedido").orderBy("linha")))
    iid = F.col("item_pedido")

    q3_2026 = F.col("dt_pedido").between(F.lit(date(2026, 7, 1)), F.lit(date(2026, 9, 30)))
    pilsen_sul = (F.col("marca_cod") == "PIL") & (F.col("regiao") == "SUL")
    q = (F.when(F.col("grp") == "G1", lognormal(4.2, 0.7, "qtd", oid, iid))
         .when(F.col("grp") == "G2", lognormal(5.5, 0.7, "qtd", oid, iid))
         .when(F.col("grp") == "G3", lognormal(240.0, 0.8, "qtd", oid, iid))
         .otherwise(lognormal(128.0, 0.6, "qtd", oid, iid)))
    q = F.when(pilsen_sul & q3_2026, q * 1.25).otherwise(q)                      # guerra de preço da Pilsen no Sul
    q = F.when(F.col("embalagem") == "BARRIL", q / 8.0).otherwise(q)
    channel_price = case_map(F.col("grp"), {"G1": 1.0, "G2": 1.0, "G3": 0.74, "G4": 0.82}, 1.0)
    reajuste = F.when(F.col("dt_pedido") >= F.lit(date(2026, 1, 1)), 1.06).otherwise(1.0)
    unit = F.col("preco_base") * channel_price * reajuste * (F.lit(1.0) + F.lit(0.015) * normal("preco", oid, iid))
    channel_disc = case_map(F.col("grp"), {"G1": 0.03, "G2": 0.03, "G3": 0.06, "G4": 0.09}, 0.03)
    disc = (F.when(pilsen_sul & q3_2026, 0.18).when(pilsen_sul, 0.08).otherwise(channel_disc)
            + F.abs(normal("desconto", oid, iid)) * 0.015)
    it = (it.withColumn("quantidade", F.greatest(F.lit(1), F.round(q)).cast("int"))
          .withColumn("vl_unitario", money(F.round(unit, 2)))
          .withColumn("vl_bruto", F.col("quantidade").cast("decimal(15,2)") * F.col("vl_unitario"))
          .withColumn("vl_desconto", money(F.round(F.col("vl_bruto") * disc, 2)))
          .withColumn("vl_total_item", money(F.col("vl_bruto") - F.col("vl_desconto"))))
    return it.select("num_pedido", "item_pedido", "cod_produto", "quantidade", "vl_unitario", "vl_desconto",
                     "vl_total_item", "dt_criacao")


# ============================================================================ Spark: notas fiscais
def build_invoices(ped, items):
    flags = ped.select("num_pedido", "cod_cliente", "regiao", "uf0", "parcial", "faturado1", "faturado2",
                       "cancelado_depois", "devolvido", "dt_fat1", "dt_fat2", "dt_cancela", "dt_devolucao")
    x = items.join(flags, "num_pedido")
    # least(q - 1, ...) garante que a 2ª NF receba ao menos 1 unidade. A carga de 2026-10-19 foi gerada antes
    # desta correção (commit 9ef90fd) e tem itens com quantidade 0 na 2ª NF quando o pedido tinha 2 unidades (ADR-0008).
    qty1 = F.when(F.col("parcial") & (F.col("quantidade") >= 2),
                  F.least(F.col("quantidade") - 1, F.ceil(F.col("quantidade") * 0.6)).cast("int")).otherwise(
        F.col("quantidade"))
    x = x.withColumn("qtd1", qty1)
    seq1 = x.where("faturado1").select("*", F.lit(1).alias("seq"), F.col("qtd1").alias("qtd_nf"))
    seq2 = x.where(F.col("faturado2") & (F.col("quantidade") >= 2)).select(
        "*", F.lit(2).alias("seq"), (F.col("quantidade") - F.col("qtd1")).alias("qtd_nf"))
    seq3a = x.where("cancelado_depois").select("*", F.lit(3).alias("seq"), F.col("quantidade").alias("qtd_nf"))
    seq3b = x.where(F.col("devolvido") & (F.col("item_pedido") <= 2)).select(
        "*", F.lit(3).alias("seq"),
        F.greatest(F.lit(1), F.floor(F.col("quantidade") * (F.lit(0.2) + F.lit(0.8) * u("qtd_devolvida", F.col("num_pedido"),
                                                                                       F.col("item_pedido"))))
                   ).cast("int").alias("qtd_nf"))
    lines = seq1.unionByName(seq2).unionByName(seq3a).unionByName(seq3b)

    dt_emissao = (F.when(F.col("seq") == 1, F.col("dt_fat1")).when(F.col("seq") == 2, F.col("dt_fat2"))
                  .when(F.col("cancelado_depois"), F.col("dt_cancela")).otherwise(F.col("dt_devolucao")))
    lines = lines.withColumn("dt_emissao", dt_emissao)
    plant_uf = (F.when(F.col("regiao") == "NORDESTE", "BA").when(F.col("regiao").isin("SUL", "CENTRO-OESTE"), "SP")
                .when(F.col("uf0") == "SP", "SP").otherwise("MG"))
    same_uf = F.col("uf0") == plant_uf
    cfop = (F.when(F.col("seq") < 3, F.when(same_uf, "5102").otherwise("6102"))
            .otherwise(F.when(same_uf, "1202").otherwise("2202")))
    headers = (lines.select("num_pedido", "seq", "cod_cliente", "dt_emissao", cfop.alias("cfop")).distinct()
               .withColumn("num_nf", F.format_string("%09d", F.lit(100000000) + F.row_number().over(
                   Window.orderBy("dt_emissao", "num_pedido", "seq"))))
               .withColumn("dt_atualizacao", at_time(F.col("dt_emissao"), "hora_nf", F.col("num_pedido"), F.col("seq"),
                                                     first_hour=6, hours=14)))
    origem = headers.where(F.col("seq") == 1).select("num_pedido", F.col("num_nf").alias("num_nf_origem_seq1"))
    headers = headers.join(origem, "num_pedido", "left").withColumn(
        "num_nf_origem", F.when(F.col("seq") == 3, F.col("num_nf_origem_seq1")))

    nid = F.col("num_nf")
    delta = pick(u("delta_centavos", nid, F.col("item_pedido")),
                 [(-0.03, 1), (-0.02, 1), (-0.01, 2), (0.01, 2), (0.02, 1), (0.03, 1)])
    divergente = (F.col("seq") < 3) & (u("centavos", nid, F.col("item_pedido")) < 0.03)
    nfi = (lines.join(headers.select("num_pedido", "seq", "num_nf", "cfop", "dt_atualizacao"), ["num_pedido", "seq"])
           .withColumn("item_nf", F.row_number().over(Window.partitionBy("num_nf").orderBy("item_pedido")))
           .withColumn("bruto", F.col("qtd_nf").cast("decimal(15,2)") * F.col("vl_unitario"))
           .withColumn("desc", money(F.round(F.col("vl_desconto") * F.col("qtd_nf") / F.col("quantidade"), 2)))
           .withColumn("divergente", divergente)
           .withColumn("total", money(F.col("bruto") - F.col("desc")
                                      + F.when(F.col("divergente"), delta.cast("decimal(15,2)")).otherwise(F.lit(0)))))
    nf_items = nfi.select("num_nf", F.lit("1").alias("serie"), "item_nf", "num_pedido", "item_pedido", "cod_produto", "cfop",
                          F.col("qtd_nf").alias("quantidade"), "vl_unitario", F.col("desc").alias("vl_desconto"),
                          F.col("total").alias("vl_total_item"), "dt_atualizacao", "divergente")
    totals = nf_items.groupBy("num_nf").agg(money(F.sum("vl_total_item")).alias("vl_total_nf"))
    nf = (headers.join(totals, "num_nf")
          .select("num_nf", F.lit("1").alias("serie"), "num_pedido", "num_nf_origem", "cod_cliente", "cfop", "dt_emissao",
                  "vl_total_nf", "dt_atualizacao", "seq"))
    return nf, nf_items


# ============================================================================ escrita e gabarito
def write(df, path, files):
    df.coalesce(files).write.mode("overwrite").parquet(path)
    return path


def save_manifest(manifest_dir, dataset, content):
    os.makedirs(manifest_dir, exist_ok=True)
    with open(f"{manifest_dir}/manifest_{dataset}.json", "w", encoding="utf-8") as fh:
        json.dump(content, fh, ensure_ascii=False, indent=2, default=str)
    print(f"manifest_{dataset}: {json.dumps(content, ensure_ascii=False, default=str)}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--start", default="2025-04-19")
    parser.add_argument("--end", default="2026-10-18")
    parser.add_argument("--extraction-date", default="2026-10-19")
    parser.add_argument("--scale", type=float, default=1.0, help="fração dos PDVs (teste rápido: 0.02)")
    parser.add_argument("--output-root", default=None, help="padrão: /Volumes/<catalog>/landing")
    args = parser.parse_args()

    start, end = date.fromisoformat(args.start), date.fromisoformat(args.end)
    ext = args.extraction_date
    root = args.output_root or f"/Volumes/{args.catalog}/landing"
    erp, ref = f"{root}/erp", f"{root}/ref"
    work, manifests = f"{erp}/_work", f"{erp}/_manifests"
    n_pdv = max(50, round(N_PDV * args.scale))

    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "America/Sao_Paulo")
    from faker import Faker

    common = dict(semente=SEED, periodo=[args.start, args.end], data_extracao=ext, escala=args.scale)

    # ---- referências e mestres pequenos (driver)
    rng = random.Random(SEED)
    ceps, n_by_city = build_ceps(rng)
    cities_df = spark.createDataFrame(
        [(i, c[0], c[1], c[2], c[4], n_by_city[i]) for i, c in enumerate(CITIES)],
        "city_idx int, municipio_canonico string, uf string, regiao string, variante string, n_ceps int")
    ceps_df = spark.createDataFrame(ceps, "city_idx int, k int, cep string, municipio string, uf string")
    city_slots_df = spark.createDataFrame(build_city_slots(), "regiao string, slot int, city_idx int")

    p_cep = write(ceps_df.select("cep", "municipio", "uf").orderBy("cep"), f"{ref}/cep_municipio/{ext}", 1)
    save_manifest(manifests, "cep_municipio", {**common, "caminho": p_cep, "linhas": len(ceps)})

    prod_versions, prod_slots, prod_info, products, promos, reused = build_products(start, end)
    prod_schema = ("cod_produto string, sku string, ean string, descricao string, marca string, categoria string, "
                   "liquido string, embalagem string, volume_ml int, unidades_pack int, status string, dt_lancamento date, "
                   "dt_descontinuacao date, dt_inicio_vigencia date, dt_fim_vigencia date, dt_atualizacao timestamp")
    p_prod = write(spark.createDataFrame(prod_versions, prod_schema), f"{erp}/produtos/{ext}", 1)
    eans = {}
    for p in products:
        eans.setdefault(p["ean"], set()).add(p["cod"])
    save_manifest(manifests, "produtos", {
        **common, "caminho": p_prod, "linhas": len(prod_versions), "produtos": len(products),
        "skus_distintos": len({p["sku"] for p in products}),
        "skus_reaproveitados": {r["sku"]: [x["cod"] for x in products if x["sku"] == r["sku"]] for r in reused},
        "eans_duplicados": {e: sorted(c) for e, c in eans.items() if len(c) > 1},
        "produtos_com_descricao_padronizada_em_2025_11_01": sum(1 for p in products if p["desc_change"]),
        "produtos_descontinuados": sum(1 for p in products if p["disc"]),
        "lancamentos_na_janela": sum(1 for p in products if p["launch"] >= start),
        "ativos_no_fim": sum(1 for p in products if p["disc"] is None or p["disc"] >= end),
    })
    product_slots_df = spark.createDataFrame(prod_slots, "month_idx int, grp string, slot int, cod_produto string")
    product_info_df = spark.createDataFrame(prod_info, "cod_produto string, marca_cod string, embalagem string, "
                                                       "litros_pack double, preco_base double")

    hier_versions, rep_slots, late = build_hierarchy(start, end, Faker)
    hier_schema = ("cod_vendedor string, nome_vendedor string, cod_supervisor string, nome_supervisor string, "
                   "cod_gerente string, nome_gerente string, regiao_comercial string, dt_inicio_vigencia date, "
                   "dt_fim_vigencia date, dt_registro date, dt_atualizacao timestamp")
    p_hier = write(spark.createDataFrame(hier_versions, hier_schema), f"{erp}/hierarquia_comercial/{ext}", 1)
    ne_to_se = sum(1 for v in hier_versions if v[7] == REORGS[4] and v[6] == "SUDESTE"
                   and any(o[0] == v[0] and o[6] == "NORDESTE" and o[8] == REORGS[4] - timedelta(days=1) for o in hier_versions))
    save_manifest(manifests, "hierarquia_comercial", {
        **common, "caminho": p_hier, "linhas": len(hier_versions), "vendedores": len({v[0] for v in hier_versions}),
        "versoes_iniciadas_na_janela": sum(1 for v in hier_versions if v[7] >= start),
        "mudancas_cadastradas_com_atraso": late,
        "vendedores_migrados_ne_para_se_em_2026_07_01": ne_to_se,
        "troca_de_gerente_ne_em_2026_01_01": "G07",
        "vigentes_hoje": sum(1 for v in hier_versions if v[8] is None),
    })
    rep_slots_df = spark.createDataFrame(rep_slots, "regiao_comercial string, periodo int, slot int, n_slots int, cod_vendedor string")

    # ---- clientes (Spark + Faker)
    base = build_customer_base(spark, start, end, n_pdv, city_slots_df, cities_df)
    base_path = write(base, f"{work}/clientes_base", 4)
    base = spark.read.parquet(base_path)
    cust = build_customer_versions(base, start, end, city_slots_df, cities_df, ceps_df)
    cust_path = write(cust, f"{work}/clientes_versoes", 4)
    cust = spark.read.parquet(cust_path)
    cust_cols = ["cod_cliente", "tipo_cliente", "segmento", "tipo_pessoa", "cnpj_cpf", "razao_social", "nome_fantasia",
                 "nome_responsavel", "email", "telefone", "cep", "logradouro", "numero", "bairro", "municipio", "uf",
                 "status", "dt_cadastro", "dt_inicio_vigencia", "dt_fim_vigencia", "dt_atualizacao"]
    p_cli = write(cust.where(~F.col("fora_da_carga")).select(*cust_cols), f"{erp}/clientes_pdv/{ext}", 2)

    cv = (cust.where(~F.col("fora_da_carga"))
          .join(ceps_df.select("cep", F.col("municipio").alias("municipio_ref")), "cep", "left"))
    c = cv.agg(F.count("*").alias("linhas"), F.countDistinct("cod_cliente").alias("clientes"),
               F.sum(F.when(F.col("municipio") != F.col("municipio_ref"), 1).otherwise(0)).alias("versoes_grafia_divergente")
               ).first()
    # Versões que devem sobrar na dim_customer: a primeira e as que mudam algum atributo rastreado (ADR-0006),
    # com a cidade já padronizada pelo CEP. Mudanças só de contato, razão social, status ou grafia são fundidas.
    wv = Window.partitionBy("cod_cliente").orderBy("dt_inicio_vigencia")
    changed = F.lit(False)
    for x in ["tipo_cliente", "segmento", "cep", "logradouro", "numero", "bairro", "uf", "municipio_ref"]:
        changed = changed | (F.col(x) != F.lag(x).over(wv))
    expected_dim_versions = (cv.withColumn("mudou", (F.col("versao") == 1) | changed)  # janela não pode ir no WHERE
                             .where("mudou").count())
    b = base.agg(
        F.sum(F.when(F.col("fora_da_carga"), 1).otherwise(0)).alias("fora"),
        F.sum(F.when(F.col("doc_invalido") & ~F.col("fora_da_carga"), 1).otherwise(0)).alias("doc_invalido"),
        F.sum(F.when(F.col("is_cpf") & ~F.col("fora_da_carga"), 1).otherwise(0)).alias("cpf"),
        *[F.sum(F.when((F.col("tipo_cliente") == t) & ~F.col("fora_da_carga"), 1).otherwise(0)).alias(t)
          for t in ["PDV", "DISTRIBUIDOR", "REDE"]]).first()
    save_manifest(manifests, "clientes_pdv", {
        **common, "caminho": p_cli, "linhas": c["linhas"], "clientes": c["clientes"],
        "por_tipo": {"PDV": b["PDV"], "DISTRIBUIDOR": b["DISTRIBUIDOR"], "REDE": b["REDE"]},
        "clientes_pessoa_fisica_cpf": b["cpf"],
        "documentos_invalidos": b["doc_invalido"],
        "versoes_com_grafia_de_cidade_divergente_do_cep": c["versoes_grafia_divergente"],
        "versoes_esperadas_na_dim_customer_apos_fusao": expected_dim_versions,
        "clientes_fora_da_carga_fk_orfa": b["fora"],
    })

    # ---- pedidos, itens e notas (Spark)
    ped = build_orders(spark, base, start, end, rep_slots_df)
    ped_path = write(ped, f"{work}/pedidos", 8)
    ped = spark.read.parquet(ped_path)
    items = build_items(ped, product_slots_df, product_info_df)
    items_path = write(items, f"{work}/itens", 8)
    items = spark.read.parquet(items_path)

    totals = items.groupBy("num_pedido").agg(money(F.sum("vl_total_item")).alias("vl_total_pedido"))
    head = ped.join(totals, "num_pedido").select("num_pedido", "cod_cliente", "cod_vendedor", "dt_pedido", "status",
                                                 "vl_total_pedido", "dt_criacao", "dt_atualizacao")
    oid = F.col("num_pedido")
    exact = head.where(u("duplicata", oid) < 0.015)                                   # reenvio da integração
    quasi = (head.where((u("quase_duplicata", oid) < 0.005) & (F.col("status") != "ABERTO"))
             .withColumn("status", F.lit("ABERTO")).withColumn("dt_atualizacao", F.col("dt_criacao")))
    p_cab = write(head.unionByName(exact).unionByName(quasi), f"{erp}/pedidos_cab/{ext}", 4)

    neg = u("negativo", oid, F.col("item_pedido")) < 0.001
    items_out = items.select(
        "num_pedido", "item_pedido", "cod_produto",
        F.when(neg, -F.col("quantidade")).otherwise(F.col("quantidade")).alias("quantidade"), "vl_unitario",
        F.when(neg, -F.col("vl_desconto")).otherwise(F.col("vl_desconto")).alias("vl_desconto"),
        F.when(neg, -F.col("vl_total_item")).otherwise(F.col("vl_total_item")).alias("vl_total_item"),
        F.col("dt_criacao").alias("dt_atualizacao"))
    p_itens = write(items_out, f"{erp}/pedidos_itens/{ext}", 8)

    nf, nf_items = build_invoices(ped, items)
    nf_path = write(nf, f"{work}/nf", 4)
    nfi_path = write(nf_items, f"{work}/nf_itens", 8)
    nf, nf_items = spark.read.parquet(nf_path), spark.read.parquet(nfi_path)
    p_nf = write(nf.drop("seq"), f"{erp}/notas_fiscais/{ext}", 4)
    p_nfi = write(nf_items.drop("divergente"), f"{erp}/notas_fiscais_itens/{ext}", 8)

    # ---- gabarito dos fatos (lido do que foi gravado)
    cab = spark.read.parquet(p_cab)
    k = cab.agg(F.count("*").alias("linhas"), F.countDistinct("num_pedido").alias("pedidos")).first()
    distinct_rows = cab.distinct().count()
    status = {r["status"]: r["n"] for r in head.groupBy("status").agg(F.count("*").alias("n")).collect()}
    orphan_orders = ped.where("fora_da_carga").count()
    pp = ped.agg(F.sum(F.col("cancelado_depois").cast("int")).alias("ca"),
                 F.sum((F.col("parcial") & F.col("faturado1") & ~F.col("faturado2")).cast("int")).alias("pa"),
                 F.sum(F.col("faturado2").cast("int")).alias("f2"), F.sum(F.col("devolvido").cast("int")).alias("dv")).first()
    save_manifest(manifests, "pedidos_cab", {
        **common, "caminho": p_cab, "linhas": k["linhas"], "pedidos_distintos": k["pedidos"],
        "duplicatas_exatas": k["linhas"] - distinct_rows, "quase_duplicatas_status_antigo": distinct_rows - k["pedidos"],
        "pedidos_por_status_final": status, "cancelados_apos_faturamento": pp["ca"],
        "pedidos_de_clientes_fora_da_carga": orphan_orders,
    })
    it_out = spark.read.parquet(p_itens)
    ki = it_out.agg(F.count("*").alias("linhas"), F.sum(F.when(F.col("quantidade") < 0, 1).otherwise(0)).alias("neg")).first()
    save_manifest(manifests, "pedidos_itens", {**common, "caminho": p_itens, "linhas": ki["linhas"],
                                               "itens_com_quantidade_e_valor_negativos": ki["neg"]})
    kn = nf.agg(F.count("*").alias("linhas"), F.sum(F.when(F.col("seq") < 3, 1).otherwise(0)).alias("vendas"),
                F.sum(F.when(F.col("seq") == 3, 1).otherwise(0)).alias("devolucoes")).first()
    save_manifest(manifests, "notas_fiscais", {
        **common, "caminho": p_nf, "linhas": kn["linhas"], "nfs_de_venda_cfop_5102_6102": kn["vendas"],
        "nfs_de_devolucao_cfop_1202_2202": kn["devolucoes"], "pedidos_com_faturamento_parcial_em_2_nfs": pp["f2"],
        "pedidos_parciais_aguardando_2a_nf": pp["pa"], "devolucoes_parciais": pp["dv"],
        "devolucoes_por_cancelamento_apos_faturamento": pp["ca"],
    })
    kf = nf_items.agg(F.count("*").alias("linhas"), F.sum(F.col("divergente").cast("int")).alias("div"),
                      F.sum(F.when(F.col("cfop").isin("1202", "2202"), 1).otherwise(0)).alias("dev")).first()
    save_manifest(manifests, "notas_fiscais_itens", {**common, "caminho": p_nfi, "linhas": kf["linhas"],
                                                     "itens_com_divergencia_de_centavos": kf["div"],
                                                     "itens_de_devolucao": kf["dev"]})

    shutil.rmtree(work, ignore_errors=True)
    print("carga inicial concluída")


if __name__ == "__main__":
    main()
