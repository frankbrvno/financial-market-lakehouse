"""
Camada Silver - Limpeza e Padronização de Dados de Mercado

Este notebook consome os dados da camada Bronze e aplica regras de
qualidade e transformação para produzir um dataset mais confiável.

Transformações aplicadas:
- remoção de registros duplicados
- validação de valores nulos
- filtragem de preços inválidos
- cálculo de métricas básicas como retorno diário e amplitude de preço

O resultado é salvo como tabela Delta na camada Silver.

Entrada:
financial_market.bronze.alpha_vantage_daily

Saída:
financial_market.silver.alpha_vantage_daily_clean
"""
# COMMAND ----------
from pyspark.sql import functions as F

# COMMAND ----------
spark.sql("CREATE CATALOG IF NOT EXISTS financial_market")
spark.sql("CREATE SCHEMA IF NOT EXISTS financial_market.silver")

# COMMAND ----------
df_bronze = spark.table("financial_market.bronze.alpha_vantage_daily")

# COMMAND ----------
df_silver = (
    df_bronze
    .dropDuplicates(["symbol", "price_date"])
    .filter(F.col("symbol").isNotNull())
    .filter(F.col("price_date").isNotNull())
    .filter(F.col("open").isNotNull())
    .filter(F.col("high").isNotNull())
    .filter(F.col("low").isNotNull())
    .filter(F.col("close").isNotNull())
    .filter(F.col("volume").isNotNull())
    .filter(F.col("open") > 0)
    .filter(F.col("high") > 0)
    .filter(F.col("low") > 0)
    .filter(F.col("close") > 0)
    .filter(F.col("volume") >= 0)
    .withColumn(
        "daily_return",
        (F.col("close") - F.col("open")) / F.col("open")
    )
    .withColumn(
        "price_range",
        F.col("high") - F.col("low")
    )
)

# COMMAND ----------
df_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("symbol") \
    .saveAsTable("financial_market.silver.alpha_vantage_daily_clean")