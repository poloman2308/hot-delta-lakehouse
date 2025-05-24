from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from pyspark.sql.functions import avg, hour, to_timestamp

# Set up SparkSession with Delta support
builder = SparkSession.builder \
    .appName("Aggregate IoT Data - Gold Layer") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Load Silver table
silver_path = "delta_tables/silver/sensor_logs"
df_silver = spark.read.format("delta").load(silver_path)

# Ensure timestamp is a timestamp type (if not already)
df_silver = df_silver.withColumn("timestamp", to_timestamp("timestamp"))

# Aggregate: average temperature & humidity per hour per location
df_gold = df_silver \
    .withColumn("hour", hour("timestamp")) \
    .groupBy("location", "hour") \
    .agg(
        avg("temperature").alias("avg_temp"),
        avg("humidity").alias("avg_humidity")
    ) \
    .orderBy("location", "hour")

# Write Gold table
gold_path = "delta_tables/gold/sensor_hourly_avg"
df_gold.write.format("delta").mode("overwrite").save(gold_path)

print("✅ Aggregated Silver → Gold: hourly averages written to", gold_path)

spark.stop()

