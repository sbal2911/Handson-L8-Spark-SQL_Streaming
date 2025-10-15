from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# Create a Spark session
spark = SparkSession.builder.appName("RideSharingAnalytics").getOrCreate()

# Define the schema for incoming JSON data
schema = StructType([
    StructField("trip_id", StringType(), True),
    StructField("driver_id", StringType(), True),
    StructField("distance_km", DoubleType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("timestamp", StringType(), True)
])

# -------------------------------------------------------------
# Read streaming data from socket (provided by data_generator.py)
# -------------------------------------------------------------
raw_stream = spark.readStream \
    .format("socket") \
    .option("host", "0.0.0.0") \
    .option("port", 9999) \
    .load()

# -------------------------------------------------------------
# Parse the incoming JSON messages into columns using schema
# -------------------------------------------------------------
parsed_stream = raw_stream.select(
    from_json(col("value"), schema).alias("data")
).select("data.*")

# -------------------------------------------------------------
# Write parsed data to CSV files in real time
# -------------------------------------------------------------
query = parsed_stream.writeStream \
    .outputMode("append") \
    .format("csv") \
    .option("path", "outputs/task1/") \
    .option("checkpointLocation", "rides_checkpoint_task1/") \
    .option("header", True) \
    .start()

# Keep the query running
query.awaitTermination()
