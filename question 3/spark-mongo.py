import json
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType, TimestampType
from pyspark.sql.functions import col, avg, count, to_timestamp, from_json
from pymongo import MongoClient

# MongoDB connection
mongo_client = MongoClient("mongodb://localhost:27017/")
db = mongo_client["vehicle_data"]
raw_collection = db["raw_positions"]
processed_collection = db["processed_data"]

# Spark Session with MongoDB Connector
spark = SparkSession.builder \
    .appName("KafkaSparkProcessing") \
    .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/vehicle_data") \
    .getOrCreate()

# Kafka Schema
schema = StructType([
    StructField("name", IntegerType(), True),
    StructField("origin", StringType(), True),
    StructField("destination", StringType(), True),
    StructField("time", StringType(), True),  # Will convert to Timestamp later
    StructField("link", StringType(), True),
    StructField("position", FloatType(), True),
    StructField("spacing", FloatType(), True),
    StructField("speed", FloatType(), True)
])

# Kafka broker and topic settings
kafka_broker = 'localhost:9092'
topic = 'vehicle_positions'

# Define the DataFrame reader for Kafka
kafka_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_broker) \
    .option("subscribe", topic) \
    .option("startingOffsets", "earliest") \
    .load()

# Define the schema and parse the message values
kafka_df = kafka_stream.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Empty DataFrame for accumulating data
accumulated_df = spark.createDataFrame([], schema=schema)

# Function to process and save data
def process_and_save_data(df, epoch_id):
    global accumulated_df

    # Convert 'time' column to TimestampType
 df = df.withColumn("time", to_timestamp(col("time"), "dd/MM/yyyy HH:mm:ss"))

    # Store raw data to MongoDB
    raw_data = df.toPandas().to_dict(orient='records')
    if raw_data:
        raw_collection.insert_many(raw_data)

    # Accumulate the data
    accumulated_df = accumulated_df.union(df)

    # Calculate vcount and vspeed, grouped by both 'time' and 'link'
    result_df = accumulated_df.groupBy("time", "link").agg(
        count("name").alias("vcount"),
        avg("speed").alias("vspeed")
    )

    # Display the result for each batch (you can remove this in production)
    result_df.show(truncate=False)
# Store processed data to MongoDB with upsert (insert or update)
    processed_data = result_df.toPandas().to_dict(orient='records')
    for record in processed_data:
        # Use 'time' and 'link' as the unique identifier for the update
        processed_collection.update_one(
            {"time": record['time'], "link": record['link']},  # Search by time and link
            {
                "$set": {  # Update vcount and vspeed
                    "vcount": record['vcount'],
                    "vspeed": record['vspeed']
                }
            },
            upsert=True  # Insert if it doesn't exist
        )

# Write the stream to MongoDB using foreachBatch
query = kafka_df.writeStream \
    .foreachBatch(process_and_save_data) \
    .outputMode("update") \
    .start()
try:
    query.awaitTermination()
except KeyboardInterrupt:
    print("Stream processing interrupted by user.")
finally:
    query.stop()
    mongo_client.close()
    print("Stream processing stopped, MongoDB connection closed.")


