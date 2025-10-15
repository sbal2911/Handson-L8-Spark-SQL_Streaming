from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg, sum
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# Create Spark session
spark = SparkSession.builder \
    .appName("RideSharingAnalytics_Task2") \
    .config("spark.sql.streaming.checkpointLocation", "rides_checkpoint_task2/") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# -------------------------------------------------------------
# Define schema for the incoming JSON ride data
# -------------------------------------------------------------
schema = StructType([
    StructField("trip_id", StringType(), True),
    StructField("driver_id", StringType(), True),
    StructField("distance_km", DoubleType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("timestamp", StringType(), True)
])

# -------------------------------------------------------------
# Read the streaming data from the socket (localhost:9999)
# -------------------------------------------------------------
raw_stream = spark.readStream \
    .format("socket") \
    .option("host", "0.0.0.0") \
    .option("port", 9999) \
    .load()

# -------------------------------------------------------------
# Parse JSON messages into structured columns
# -------------------------------------------------------------
parsed_stream = raw_stream.select(
    from_json(col("value"), schema).alias("data")
).select("data.*")

# -------------------------------------------------------------
# Convert timestamp column to proper TimestampType & watermark
# -------------------------------------------------------------
parsed_stream = parsed_stream \
    .withColumn("timestamp", col("timestamp").cast(TimestampType())) \
    .withWatermark("timestamp", "10 minutes")

# -------------------------------------------------------------
# Perform aggregations per driver:
#     • Total fare amount (sum)
#     • Average trip distance (avg)
# -------------------------------------------------------------
aggregated_stream = parsed_stream.groupBy("driver_id").agg(
    sum("fare_amount").alias("total_fare"),
    avg("distance_km").alias("avg_distance")
)

# -------------------------------------------------------------
# Define helper to write each micro-batch to a unique CSV file
# -------------------------------------------------------------
def write_to_csv(batch_df, batch_id):
    if batch_df.isEmpty():
        print(f"Batch {batch_id} empty — no output written.")
        return

    output_path = f"outputs/task2/batch_{batch_id}"
    (
        batch_df.write
        .mode("overwrite")
        .option("header", True)
        .csv(output_path)
    )
    print(f"Batch {batch_id} written to {output_path}")

# -------------------------------------------------------------
# Start streaming query with foreachBatch sink
# -------------------------------------------------------------
query = aggregated_stream.writeStream \
    .outputMode("update") \
    .foreachBatch(write_to_csv) \
    .option("checkpointLocation", "rides_checkpoint_task2/") \
    .start()

# -------------------------------------------------------------
# Keep the query running
# -------------------------------------------------------------
query.awaitTermination()
