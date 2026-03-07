
"""
Camada Gold - Métricas Analíticas de Mercado

Este notebook consome os dados da camada Silver e gera métricas
analíticas para consumo em dashboards e análises financeiras.

Métricas calculadas:
- média móvel de 7 dias
- média móvel de 30 dias
- volatilidade
- retorno acumulado
- ranking de ativos por performance

Entrada:
financial_market.silver.alpha_vantage_daily_clean

Saída:
financial_market.gold.market_analytics
"""

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

spark.sql("CREATE CATALOG IF NOT EXISTS financial_market")
spark.sql("CREATE SCHEMA IF NOT EXISTS financial_market.gold")

# COMMAND ----------

df_silver = spark.table("financial_market.silver.alpha_vantage_daily_clean")

# COMMAND ----------

window_symbol = Window.partitionBy("symbol").orderBy("price_date")

window_7 = window_symbol.rowsBetween(-6, 0)
window_30 = window_symbol.rowsBetween(-29, 0)

df_gold = (
    df_silver
    .withColumn(
        "ma_7",
        F.avg("close").over(window_7)
    )
    .withColumn(
        "ma_30",
        F.avg("close").over(window_30)
    )
    .withColumn(
        "volatility_7",
        F.stddev("daily_return").over(window_7)
    )
    .withColumn(
        "volatility_30",
        F.stddev("daily_return").over(window_30)
    )
    .withColumn(
        "cumulative_return",
        F.sum("daily_return").over(window_symbol)
    )
)

# COMMAND ----------

ranking_window = Window.partitionBy("price_date").orderBy(F.col("daily_return").desc())

df_gold = df_gold.withColumn(
    "daily_rank",
    F.rank().over(ranking_window)
)

# COMMAND ----------

df_gold.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("symbol") \
    .saveAsTable("financial_market.gold.market_analytics")