from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

import os
os.environ['JAVA_HOME'] = 'C:\\java\\jdk'
os.environ['HADOOP_HOME'] = 'C:\\hadoop'

if __name__ == "__main__":
    # Create a SparkSession
    spark = SparkSession.builder \
        .appName("Read from Kafka") \
        .getOrCreate()

    # Define the schema to match the JSON structure
    schema = StructType([
        StructField("name", IntegerType(), True),
        StructField("time", StringType(), True),
        StructField("link", StringType(), True),
        StructField("speed", DoubleType(), True)
    ])

    # Read streaming data from Kafka
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "vehicle_positions") \
        .load()

    # Parse JSON data and select relevant fields
    parsed_df = kafka_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.name", "data.time", "data.link", "data.speed")

    # Start the streaming query
    query = parsed_df.writeStream \
        .outputMode("append") \
        .format("console") \
        .start()

    # Wait for the termination of the query
    query.awaitTermination()

    # Stop the SparkSession
    spark.stop()
