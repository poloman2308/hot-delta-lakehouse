from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
import os

# Set up SparkSession with Delta Lake support
builder = SparkSession.builder \
    .appName("Ingest IoT Data - Bronze Layer") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Load raw CSV file
raw_path = "data/raw/sensor_logs.csv"
df_raw = spark.read.option("header", True).option("inferSchema", True).csv(raw_path)

# Optional: Show sample of data
df_raw.show(5)
df_raw.printSchema()

# Write to Delta Lake - Bronze
bronze_path = "delta_tables/bronze/sensor_logs"
df_raw.write.format("delta").mode("overwrite").save(bronze_path)

print("✅ Raw sensor data ingested into Bronze Delta table at:", bronze_path)

spark.stop()

