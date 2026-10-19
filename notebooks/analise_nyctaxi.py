# Databricks notebook source
# MAGIC %md
# MAGIC # Análise exploratória: corridas de táxi em Nova York
# MAGIC
# MAGIC Fonte: `samples.nyctaxi.trips` (tabela de exemplo do Databricks, táxis amarelos de NYC).
# MAGIC
# MAGIC | Coluna | Tipo | Significado |
# MAGIC |---|---|---|
# MAGIC | `tpep_pickup_datetime` | timestamp | Embarque |
# MAGIC | `tpep_dropoff_datetime` | timestamp | Desembarque |
# MAGIC | `trip_distance` | double | Distância (milhas) |
# MAGIC | `fare_amount` | double | Tarifa (USD) |
# MAGIC | `pickup_zip` / `dropoff_zip` | int | CEP de embarque / desembarque |
# MAGIC
# MAGIC **Perguntas:**
# MAGIC 1. Em que horas do dia acontecem mais corridas?
# MAGIC 2. Como a tarifa cresce com a distância?
# MAGIC 3. Quais CEPs concentram os embarques?

# COMMAND ----------

from pyspark.sql import functions as F
import plotly.graph_objects as go

# Estilo comum dos gráficos: uma série por gráfico, um tom de azul, eixos discretos.
AZUL = "#2a78d6"
SUPERFICIE = "#fcfcfb"
TINTA = "#0b0b0b"
TINTA_SECUNDARIA = "#52514e"
GRADE = "#e1e0d9"
EIXO = "#c3c2b7"


def estilizar(fig, titulo, eixo_x, eixo_y, altura=420):
    fig.update_layout(
        title=dict(text=titulo, font=dict(size=16, color=TINTA), x=0, xanchor="left"),
        plot_bgcolor=SUPERFICIE,
        paper_bgcolor=SUPERFICIE,
        font=dict(color=TINTA_SECUNDARIA, size=12),
        height=altura,
        margin=dict(l=60, r=30, t=60, b=50),
        bargap=0.25,
        hoverlabel=dict(bgcolor="white", font_color=TINTA, bordercolor=EIXO),
        showlegend=False,
    )
    fig.update_xaxes(title_text=eixo_x, showgrid=False, linecolor=EIXO, ticks="")
    fig.update_yaxes(title_text=eixo_y, gridcolor=GRADE, zeroline=True, zerolinecolor=EIXO)
    return fig

# COMMAND ----------

# MAGIC %md
# MAGIC ## Visão geral e limpeza
# MAGIC
# MAGIC Removemos corridas com distância ou tarifa não positivas e durações implausíveis (menos de 1 minuto ou mais de 3 horas).

# COMMAND ----------

bruto = spark.read.table("samples.nyctaxi.trips")

corridas = (
    bruto
    .withColumn(
        "duracao_min",
        (F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime")) / 60,
    )
    .filter(
        (F.col("trip_distance") > 0)
        & (F.col("fare_amount") > 0)
        & F.col("duracao_min").between(1, 180)
    )
    .withColumn("hora", F.hour("tpep_pickup_datetime"))
)

resumo = corridas.agg(
    F.count("*").alias("corridas"),
    F.min("tpep_pickup_datetime").alias("primeira"),
    F.max("tpep_pickup_datetime").alias("ultima"),
    F.round(F.avg("trip_distance"), 2).alias("distancia_media_mi"),
    F.round(F.avg("fare_amount"), 2).alias("tarifa_media_usd"),
    F.round(F.avg("duracao_min"), 1).alias("duracao_media_min"),
)

print(f"Linhas brutas: {bruto.count():,} | após limpeza: {corridas.count():,}")
display(resumo)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Corridas por hora do dia

# COMMAND ----------

por_hora = (
    corridas.groupBy("hora")
    .agg(F.count("*").alias("corridas"), F.round(F.avg("fare_amount"), 2).alias("tarifa_media"))
    .orderBy("hora")
    .toPandas()
)

pico = por_hora.loc[por_hora["corridas"].idxmax()]

fig = go.Figure(
    go.Bar(
        x=por_hora["hora"],
        y=por_hora["corridas"],
        marker=dict(color=AZUL),
        customdata=por_hora["tarifa_media"],
        hovertemplate="<b>%{x}h</b><br>Corridas: %{y:,}<br>Tarifa média: US$ %{customdata:.2f}<extra></extra>",
    )
)
fig.add_annotation(
    x=pico["hora"], y=pico["corridas"], text=f"Pico: {int(pico['hora'])}h",
    showarrow=False, yshift=12, font=dict(color=TINTA, size=12),
)
estilizar(fig, "Corridas por hora de embarque", "Hora do dia", "Corridas")
fig.update_xaxes(tickmode="linear", dtick=2, ticksuffix="h")
fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Tarifa média por faixa de distância

# COMMAND ----------

faixa = (
    F.when(F.col("trip_distance") < 1, "0–1")
    .when(F.col("trip_distance") < 2, "1–2")
    .when(F.col("trip_distance") < 3, "2–3")
    .when(F.col("trip_distance") < 5, "3–5")
    .when(F.col("trip_distance") < 10, "5–10")
    .when(F.col("trip_distance") < 20, "10–20")
    .otherwise("20+")
)
ordem = ["0–1", "1–2", "2–3", "3–5", "5–10", "10–20", "20+"]

por_distancia = (
    corridas.withColumn("faixa", faixa)
    .groupBy("faixa")
    .agg(F.count("*").alias("corridas"), F.round(F.avg("fare_amount"), 2).alias("tarifa_media"))
    .toPandas()
    .set_index("faixa")
    .reindex(ordem)
    .dropna()
    .reset_index()
)

fig = go.Figure(
    go.Bar(
        x=por_distancia["faixa"],
        y=por_distancia["tarifa_media"],
        marker=dict(color=AZUL),
        customdata=por_distancia["corridas"],
        text=[f"US$ {v:.0f}" for v in por_distancia["tarifa_media"]],
        textposition="outside",
        textfont=dict(color=TINTA_SECUNDARIA),
        hovertemplate="<b>%{x} milhas</b><br>Tarifa média: US$ %{y:.2f}<br>Corridas: %{customdata:,}<extra></extra>",
    )
)
estilizar(fig, "Tarifa média por faixa de distância", "Distância (milhas)", "Tarifa média (US$)")
fig.update_yaxes(tickprefix="US$ ")
fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Os 10 CEPs com mais embarques

# COMMAND ----------

top_ceps = (
    corridas.filter(F.col("pickup_zip").isNotNull())
    .groupBy("pickup_zip")
    .agg(F.count("*").alias("corridas"))
    .orderBy(F.desc("corridas"))
    .limit(10)
    .toPandas()
    .sort_values("corridas")  # maior no topo do gráfico horizontal
)
total = corridas.count()
top_ceps["pct"] = top_ceps["corridas"] / total * 100

fig = go.Figure(
    go.Bar(
        x=top_ceps["corridas"],
        y=top_ceps["pickup_zip"].astype(str),
        orientation="h",
        marker=dict(color=AZUL),
        customdata=top_ceps["pct"],
        text=[f"{p:.1f}%" for p in top_ceps["pct"]],
        textposition="outside",
        textfont=dict(color=TINTA_SECUNDARIA),
        hovertemplate="<b>CEP %{y}</b><br>Corridas: %{x:,}<br>%{customdata:.1f}% do total<extra></extra>",
    )
)
estilizar(fig, "Os 10 CEPs com mais embarques", "Corridas", "CEP de embarque", altura=460)
fig.update_xaxes(showgrid=True, gridcolor=GRADE)
fig.update_yaxes(showgrid=False, type="category")
fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conclusões
# MAGIC
# MAGIC Base: 21.737 corridas válidas (de 21.932) entre jan e fev de 2016. Média de 2,86 milhas, US$ 12,32 e 13 minutos por corrida.
# MAGIC
# MAGIC - **Horário**: o pico é às **18h** (1.442 corridas) e o vale às **5h** (212), cerca de 7 vezes menos. A demanda sobe forte a partir das 7h e fica alta até as 22h.
# MAGIC - **Distância × tarifa**: a tarifa cresce com a distância (US$ 5 abaixo de 1 milha, US$ 62 acima de 20), mas o custo por milha cai nas corridas longas, porque a bandeirada fixa se dilui.
# MAGIC - **Concentração geográfica**: os 10 CEPs com mais embarques, todos em Manhattan (100xx), somam **cerca de 45%** das corridas; o primeiro é o **10001** (1.218).
# MAGIC
# MAGIC **Próximo passo no projeto:** transformar esta limpeza em uma camada *silver* de um pipeline SDP, com as regras de filtro virando *expectations*.
