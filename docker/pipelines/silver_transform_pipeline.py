from pyspark.sql import SparkSession
from delta.tables import DeltaTable

spark = SparkSession.builder \
    .appName("silver") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

df = spark.read.json("data/bronze/")

egg_df = df.filter(df.event_type == "egg_purchase")
send_chicks_df = df.filter(df.event_type == "send_chicks")
slaughter_df = df.filter(df.event_type == "slaughter")

egg_df = egg_df.select(
    "event_id",
    "timestamp",
    "quantity",
    "egg_batch_id",
    "matriz_farm"
)

send_chicks_df = send_chicks_df.select(
    "event_id",
    "timestamp",
    "quantity",
    "chick_batch_id",
    "egg_batch_id"
)

slaughter_df = slaughter_df.select(
    "event_id",
    "timestamp",
    "quantity",
    "slaughter_batch_id",
    "chick_batch_id"
)

def merge(path, df):
    if DeltaTable.isDeltaTable(spark, path):
        delta_table = DeltaTable.forPath(spark, path)

        delta_table.alias("target").merge(
            df.alias("source"),
            "target.event_id = source.event_id"
        ).whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()
    else:
        df.write.format("delta").save(path)

merge("data/silver/egg_purchases", egg_df)
merge("data/silver/send_chicks_batches", send_chicks_df)
merge("data/silver/slaughter_batches", slaughter_df)