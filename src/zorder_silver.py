from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

builder = SparkSession.builder \
    .appName("Manual Z-Order Simulation - Silver Layer") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Load Silver table
silver_path = "delta_tables/silver/sensor_logs"
df = spark.read.format("delta").load(silver_path)

# Simulate Z-Ordering: sort + repartition
df_sorted = df.sort("sensor_id", "timestamp").repartition("sensor_id")

# Overwrite the same table
df_sorted.write.format("delta").mode("overwrite").save(silver_path)

print("✅ Manually optimized Silver table with simulated Z-Ordering.")

spark.stop()

