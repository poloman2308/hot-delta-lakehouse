from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable

# Start Spark session
builder = SparkSession.builder \
    .appName("Time Travel - Silver Layer") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Load DeltaTable object for Silver
silver_path = "delta_tables/silver/sensor_logs"
delta_table = DeltaTable.forPath(spark, silver_path)

# Get version history
history_df = delta_table.history(10)  # show last 10 versions
history_df.select("version", "timestamp", "operation", "operationParameters").show(truncate=False)

# Load a previous version (manually set based on what you see in history)
version = 0  # replace with a real older version number after viewing history
df_old = spark.read.format("delta").option("versionAsOf", version).load(silver_path)

print(f"\n✅ Showing Silver table at version {version}:\n")
df_old.show(5)

spark.stop()

