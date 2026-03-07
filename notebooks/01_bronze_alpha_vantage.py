"""
Camada Bronze - Dados de Mercado Alpha Vantage

Este notebook lê os dados brutos armazenados no S3 provenientes da API
Alpha Vantage e transforma o JSON em um formato tabular estruturado.

A transformação inclui:
- extração das séries temporais diárias
- conversão de tipos de dados
- padronização de datas
- inclusão de colunas técnicas de ingestão

O resultado é salvo como tabela Delta na camada Bronze do Lakehouse.

Entrada:
S3 (raw) → dados JSON da API Alpha Vantage

Saída:
financial_market.bronze.alpha_vantage_daily
"""

# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import MapType, StringType

# COMMAND ----------
spark.sql("CREATE CATALOG IF NOT EXISTS financial_market")
spark.sql("CREATE SCHEMA IF NOT EXISTS financial_market.bronze")
spark.sql("CREATE SCHEMA IF NOT EXISTS financial_market.silver")
spark.sql("CREATE SCHEMA IF NOT EXISTS financial_market.gold")

# COMMAND ----------
raw_path = "s3://financial-market-lakehouse-bruno/raw/alpha_vantage/daily/"

df_raw = spark.read.option("multiline", "true").json(raw_path)

# COMMAND ----------
time_series_map_type = MapType(
    StringType(),
    MapType(StringType(), StringType())
)

df_bronze = (
    df_raw
    .select(
        F.col("metadata.symbol").alias("symbol"),
        F.col("metadata.extraction_date").alias("extraction_date"),
        F.col("metadata.ingestion_timestamp_utc").alias("ingestion_timestamp_utc"),
        F.from_json(
            F.to_json(F.col("payload.`Time Series (Daily)`")),
            time_series_map_type
        ).alias("time_series")
    )
    .select(
        "symbol",
        "extraction_date",
        "ingestion_timestamp_utc",
        F.explode("time_series").alias("price_date", "daily_values")
    )
    .select(
        F.lit("alpha_vantage").alias("source"),
        F.lit("daily").alias("dataset"),
        "symbol",
        F.to_date("price_date").alias("price_date"),
        F.col("daily_values")["1. open"].cast("double").alias("open"),
        F.col("daily_values")["2. high"].cast("double").alias("high"),
        F.col("daily_values")["3. low"].cast("double").alias("low"),
        F.col("daily_values")["4. close"].cast("double").alias("close"),
        F.col("daily_values")["5. volume"].cast("long").alias("volume"),
        F.to_date("extraction_date").alias("extraction_date"),
        F.to_timestamp("ingestion_timestamp_utc").alias("ingestion_timestamp_utc"),
        F.current_date().alias("load_date")
    )
    .filter(F.col("price_date").isNotNull())
)

# COMMAND ----------
df_bronze.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("symbol") \
    .saveAsTable("financial_market.bronze.alpha_vantage_daily")