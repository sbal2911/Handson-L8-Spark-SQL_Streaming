# Ride Sharing Analytics Using Spark Streaming and Spark SQL.
---
**About:** This project implements a real-time ride-sharing analytics pipeline using Apache Spark Structured Streaming and Spark SQL. Simulated ride data is streamed over a socket, ingested, parsed, and processed in real-time to perform aggregations and time-based trend analysis. The pipeline demonstrates ingestion, driver-level earnings analysis, and windowed fare trend computation, with results written to CSV files for each task.

---

## **Prerequisites**
Before starting the assignment, ensure you have the following software installed and properly configured on your machine:
1. **Python 3.x**:
   - [Download and Install Python](https://www.python.org/downloads/)
   - Verify installation:
     ```bash
     python3 --version
     ```

2. **PySpark**:
   - Install using `pip`:
     ```bash
     pip install pyspark
     ```

3. **Faker**:
   - Install using `pip`:
     ```bash
     pip install faker
     ```

---

## **Setup Instructions**

### **1. Project Structure**

Ensure your project directory follows the structure below:

```
ride-sharing-analytics/
├── outputs/
│   ├── task1
│   |    └── CSV files of task 1.
|   ├── task2
│   |    └── CSV files of task 2.
|   └── task3
│       └── CSV files of task 3.
├── task1.py
├── task2.py
├── task3.py
├── data_generator.py
└── README.md
```

- **data_generator.py/**: generates a constant stream of input data of the schema (trip_id, driver_id, distance_km, fare_amount, timestamp)  
- **outputs/**: CSV files of processed data of each task stored in respective folders.
  
---

### **2. Running the Analysis Tasks**

1. **Execute Each Task **: The data_generator.py should be continuosly running on a terminal. open a new terminal to execute each of the tasks.
   ```bash
     python data_generator.py
     python task1.py
     python task2.py
     python task3.py
   ```

2. **Verify the Outputs**:
   Check the `outputs/` directory for the resulting files:
   ```bash
   ls outputs/
   ```

---

## **Overview**

In this assignment, we will build a real-time analytics pipeline for a ride-sharing platform using Apache Spark Structured Streaming. we will process streaming data, perform real-time aggregations, and analyze trends over time.

## **Objectives**

By the end of this assignment, you should be able to:

1. Task 1: Ingest and parse real-time ride data.
2. Task 2: Perform real-time aggregations on driver earnings and trip distances.
3. Task 3: Analyze trends over time using a sliding time window.

---

## **Assignment approach**

**1. Simulated Real-Time Data Generation:** Generate continuous synthetic ride-sharing events using the Faker library and stream them over a TCP socket for ingestion.

**2. Real-Time Data Ingestion:** Use Apache Spark Structured Streaming to read the incoming JSON data from the socket, parsing it into structured DataFrames with predefined schemas.

**3. Task-Based Processing:** Implement separate Spark streaming jobs for different analytical tasks—basic data parsing, driver-level aggregations, and windowed time-based analytics.

**4. Watermarking and Windowing:** Handle late-arriving data with watermarking and perform sliding window aggregations to analyze trends over time.

**5. Output Management:** Persist the processed results as CSV files in task-specific output directories, using checkpointing and batch-wise writing for fault tolerance and efficient streaming.

---

## **Data generation step**

1. Continuously generate synthetic ride data using the Faker library, including fields such as trip_id, driver_id, distance_km, fare_amount, and timestamp.
2. Stream the generated data as newline-delimited JSON messages over a socket connection on localhost:9999 for real-time ingestion by Spark.

## **Programming logic:**
1. The data_generator.py script uses Python’s socket module to stream fake ride-sharing data to localhost:9999. 
2. Each record is a JSON object containing fields like trip_id, driver_id, distance_km, fare_amount, and timestamp, generated using the Faker library. 
3. The data is sent continuously every second to simulate a real-time stream, which Spark can then consume for processing.

## **Output:**

```bash
Streaming data to localhost:9999...
New client connected: ('127.0.0.1', 42496)
Sent: {'trip_id': '8c45ef39-2f98-490c-b6a5-a659e57a6cb0', 'driver_id': 57, 'distance_km': 29.4, 'fare_amount': 23.35, 'timestamp': '2025-10-15 14:41:37'}
Sent: {'trip_id': 'b9ac301b-f85b-4816-b016-b6c9bcd50ed7', 'driver_id': 96, 'distance_km': 11.97, 'fare_amount': 73.04, 'timestamp': '2025-10-15 14:41:38'}
Sent: {'trip_id': '1af08588-c29c-491e-a8a9-05429ab15b6a', 'driver_id': 27, 'distance_km': 48.98, 'fare_amount': 75.07, 'timestamp': '2025-10-15 14:41:39'}
```
---

## **Task 1: Basic Streaming Ingestion and Parsing**

1. Ingest streaming data from the provided socket (e.g., localhost:9999) using Spark Structured Streaming.
2. Parse the incoming JSON messages into a Spark DataFrame with proper columns (trip_id, driver_id, distance_km, fare_amount, timestamp).

## **Programming logic:**
1. The streaming data is read from a socket on localhost:9999 using Spark Structured Streaming. 
2. Each incoming JSON message is parsed into a structured DataFrame with fields like trip_id, driver_id, distance_km, etc. 
3. The parsed data is then written in real-time to CSV files in the outputs/task1/ directory using the append output mode. 
4. A checkpoint directory is used to maintain streaming state and ensure fault tolerance.

## **Output:**

```bash
trip_id,driver_id,distance_km,fare_amount,timestamp
40f9aa48-3d3c-4a1a-8d61-a680b6d85c8f,91,33.01,139.47,2025-10-15 14:41:45
8c45ef39-2f98-490c-b6a5-a659e57a6cb0,57,29.4,23.35,2025-10-15 14:41:37
6a17aab0-0516-45a2-a576-ee60faf5969a,84,22.26,130.4,2025-10-15 14:41:46
```
---

## **Task 2: Real-Time Aggregations (Driver-Level)**

1. Aggregate the data in real time to answer the following questions:
  • Total fare amount grouped by driver_id.
  • Average distance (distance_km) grouped by driver_id.
2. Output these aggregations to the console in real time.

## **Programming logic:**

1. Streaming data is ingested from the socket and parsed into structured columns. 
2. The timestamp is cast to TimestampType and a watermark is applied to handle late data. 
3. The stream is grouped by driver_id to compute total fare (SUM) and average trip distance (AVG). 
4. The aggregated results are written to separate CSV files for each micro-batch using the foreachBatch sink.

## **Output:**

**Please Note:** Multiple **csv** files are placed in the **outputs/task2** folder to show data variety and genuineness. Few of the generated output is shown below for your convinience:

```bash
*batch_1*
driver_id,total_fare,avg_distance
54,35.21,37.18
85,64.51,15.04
3,166.92000000000002,17.77

*batch_2*
driver_id,total_fare,avg_distance
22,154.74,33.724999999999994
44,82.18,13.88
21,129.32,20.88
```
---

## **Task 3: Windowed Time-Based Analytics**

1. Convert the timestamp column to a proper TimestampType.
2. Perform a 5-minute windowed aggregation on fare_amount (sliding by 1 minute and watermarking by 1 minute).

## **Programming logic:**

1. The streaming data is parsed and the timestamp field is cast to TimestampType with a 1-minute watermark to manage late data. 
2. A sliding window aggregation is performed over a 5-minute window with 1-minute slide intervals to calculate the total fare per driver. 
3. The results include window start/end times and are written as CSV files per micro-batch using the foreachBatch sink. 
4. This enables time-based trend analysis of driver earnings.

## **Output:**

**Please Note:** Multiple **csv** files are placed in the **outputs/task3** folder to show data variety and genuineness. Few of the generated output is shown below for your convinience:

```bash
*batch_1*
window_start,window_end,driver_id,total_fare
2025-10-15T14:56:00.000Z,2025-10-15T15:01:00.000Z,76,81.79
2025-10-15T14:53:00.000Z,2025-10-15T14:58:00.000Z,62,175.86
2025-10-15T14:55:00.000Z,2025-10-15T15:00:00.000Z,99,140.57

*batch_2*
window_start,window_end,driver_id,total_fare
2025-10-15T14:55:00.000Z,2025-10-15T15:00:00.000Z,27,149.44
2025-10-15T14:54:00.000Z,2025-10-15T14:59:00.000Z,64,81.22
2025-10-15T14:56:00.000Z,2025-10-15T15:01:00.000Z,61,142.3
```
---

## **Challenges faced**

While running ```python task2.py ``` I was not getting sufficient amount of batch files in my console and neither were the output files getting generated. Everytime I tried to run the command, it gave me **batch_0** file that was empty and then it did not show me any other files generation.

## **Challenges fixed**

I tried stopping the previous ```python task1.py``` and then ran the command ```python task2.py``` in a separate terminal and it started producing sufficient files. It was mandatory to run one task at a time in separate terminals.

---

