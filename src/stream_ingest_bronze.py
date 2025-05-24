from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp
from delta import configure_spark_with_delta_pip

# Spark Session with Delta
builder = SparkSession.builder \
    .appName("Stream Ingest IoT - Bronze") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.shuffle.partitions", "1")  # for local dev

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Define streaming DataFrame
input_path = "data/stream"
bronze_path = "delta_tables/bronze/sensor_logs"

df_stream = spark.readStream \
    .option("header", True) \
    .option("inferSchema", True) \
    .schema("sensor_id INT, timestamp STRING, temperature DOUBLE, humidity DOUBLE, location STRING") \
    .csv(input_path)

df_stream = df_stream.withColumn("timestamp", to_timestamp("timestamp"))

# Write to Delta in append mode
query = df_stream.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "delta_tables/bronze/_checkpoints/sensor_logs") \
    .start(bronze_path)

print("🚀 Streaming ingestion started. Watching:", input_path)
query.awaitTermination()

