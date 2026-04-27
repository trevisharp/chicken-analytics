from pyspark.sql import SparkSession

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

egg_df.write.format("delta").mode("overwrite").save("data/silver/egg_purchases")
send_chicks_df.write.format("delta").mode("overwrite").save("data/silver/hatch_batches")
slaughter_df.write.format("delta").mode("overwrite").save("data/silver/slaughter_batches")