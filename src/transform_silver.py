from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

# Set up SparkSession with Delta Lake support
builder = SparkSession.builder \
    .appName("Transform IoT Data - Silver Layer") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Read from Bronze Delta table
bronze_path = "delta_tables/bronze/sensor_logs"
df_bronze = spark.read.format("delta").load(bronze_path)

# 🧹 Clean data: remove duplicates and nulls
df_silver = df_bronze.dropDuplicates(["sensor_id", "timestamp"]) \
                     .dropna(subset=["sensor_id", "timestamp", "temperature", "humidity", "location"])

# Optional: Filter temperature range (valid sensor range)
df_silver = df_silver.filter((df_silver.temperature >= 50) & (df_silver.temperature <= 100))

# Write to Silver Delta path
silver_path = "delta_tables/silver/sensor_logs"
df_silver.write.format("delta").mode("overwrite").save(silver_path)

print("✅ Transformed Bronze → Silver: cleaned data written to", silver_path)

spark.stop()

