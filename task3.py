# Import necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, sum, window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# -------------------------------------------------------------
# Create Spark Session
# -------------------------------------------------------------
spark = SparkSession.builder \
    .appName("RideSharingAnalytics_Task3") \
    .config("spark.sql.streaming.checkpointLocation", "rides_checkpoint_task3/") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# -------------------------------------------------------------
# Define the schema for incoming JSON ride data
# -------------------------------------------------------------
schema = StructType([
    StructField("trip_id", StringType(), True),
    StructField("driver_id", StringType(), True),
    StructField("distance_km", DoubleType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("timestamp", StringType(), True)
])

# -------------------------------------------------------------
# Read streaming data from the socket
# -------------------------------------------------------------
raw_stream = spark.readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

# -------------------------------------------------------------
# Parse JSON payload into structured DataFrame columns
# -------------------------------------------------------------
parsed_stream = raw_stream.select(
    from_json(col("value"), schema).alias("data")
).select("data.*")

# -------------------------------------------------------------
# Convert timestamp column to TimestampType and add watermark
# -------------------------------------------------------------
parsed_stream = parsed_stream \
    .withColumn("timestamp", col("timestamp").cast(TimestampType())) \
    .withWatermark("timestamp", "1 minute")   # watermark ensures late data is handled

# -------------------------------------------------------------
# Perform windowed aggregation
#     - 5-minute window
#     - Sliding every 1 minute
#     - Aggregate total fare per driver per window
# -------------------------------------------------------------
windowed_agg = parsed_stream.groupBy(
    window(col("timestamp"), "5 minutes", "1 minute"),
    col("driver_id")
).agg(sum("fare_amount").alias("total_fare"))

# -------------------------------------------------------------
# Extract window start/end times into separate columns
# -------------------------------------------------------------
result = windowed_agg.select(
    col("window.start").alias("window_start"),
    col("window.end").alias("window_end"),
    col("driver_id"),
    col("total_fare")
)

# -------------------------------------------------------------
# Define per-batch CSV writer with header
# -------------------------------------------------------------
def write_to_csv(batch_df, batch_id):
    if batch_df.isEmpty():
        print(f"Batch {batch_id} has no records — skipping write.")
        return

    output_path = f"outputs/task3/batch_{batch_id}"
    (
        batch_df.write
        .mode("overwrite")
        .option("header", True)
        .csv(output_path)
    )
    print(f"Batch {batch_id} successfully written to {output_path}")

# -------------------------------------------------------------
# Start the streaming query using foreachBatch
# -------------------------------------------------------------
query = result.writeStream \
    .outputMode("update") \
    .foreachBatch(write_to_csv) \
    .option("checkpointLocation", "rides_checkpoint_task3/") \
    .start()

# -------------------------------------------------------------
# Keep the query running
# -------------------------------------------------------------
query.awaitTermination()
