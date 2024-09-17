from kafka import KafkaConsumer
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType, TimestampType
from pyspark.sql.functions import col, avg, count, to_timestamp
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
    StructField("time", StringType(), True),  # Θα το μετατρέψουμε σε Timestamp αργότερα
    StructField("link", StringType(), True),
    StructField("position", FloatType(), True),
    StructField("spacing", FloatType(), True),
    StructField("speed", FloatType(), True)
])

# Kafka broker and topic settings
kafka_broker = 'localhost:9092'
topic = 'vehicle_positions'

# Kafka Consumer
consumer = KafkaConsumer(
    topic,
    bootstrap_servers=[kafka_broker],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='vehicle_positions_group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

def process_message(message):
    # Process the raw Kafka message
    data = {
        "name": message['name'],
        "origin": message['origin'],
        "destination": message['destination'],
        "time": message['time'],
        "link": message['link'],
        "position": message['position'],
        "spacing": message['spacing'],
        "speed": message['speed']
    }
    return data

def save_raw_data_to_mongodb(data):
    # Insert raw data into MongoDB
    raw_collection.insert_one(data)

def save_processed_data_to_mongodb(df):
    # Insert processed data into MongoDB
    for row in df.collect():
        processed_collection.insert_one(row.asDict())

try:
    for message in consumer:
        message_value = message.value
        print(f"Received: {message_value}")

        # Process the raw Kafka message
        processed_data = [process_message(message_value)]

        # Create DataFrame from the processed data
        df = spark.createDataFrame(processed_data, schema=schema)

        # Convert 'time' column to TimestampType
        df = df.withColumn("time", to_timestamp(col("time"), "dd/MM/yyyy HH:mm:ss"))

        # Store raw data to MongoDB
        for data in processed_data:
            save_raw_data_to_mongodb(data)

        # Aggregation (vcount and vspeed)
        result_df = df.groupBy("time", "link").agg(
            count("name").alias("vcount"),
            avg("speed").alias("vspeed")
        )

        # Show the processed data
        result_df.show()

        # Store processed data to MongoDB
        save_processed_data_to_mongodb(result_df)

except KeyboardInterrupt:
    print("Consumer interrupted by user.")

finally:
    consumer.close()
    mongo_client.close()
    print("Kafka consumer closed, MongoDB connection closed.")
