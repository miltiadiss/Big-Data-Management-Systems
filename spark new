import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, FloatType
from pyspark.sql.functions import col, from_json, to_timestamp, avg, count
import time

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Kafka and MongoDB configuration
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "vehicle_positions"
MONGODB_URI = "mongodb://127.0.0.1/traffic_db"

# Initialize Spark session
spark = SparkSession.builder \
    .appName("KafkaSparkProcessing") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2,org.mongodb.spark:mongo-spark-connector_2.12:3.>
    .config("spark.mongodb.write.connection.uri", f"{MONGODB_URI}.processed_data") \
    .config("spark.mongodb.read.connection.uri", f"{MONGODB_URI}.raw_data") \
 .getOrCreate()

# Kafka stream schema
schema = StructType() \
    .add("name", StringType()) \
    .add("origin", StringType()) \
    .add("destination", StringType()) \
    .add("time", StringType()) \
    .add("link", StringType()) \
    .add("position", FloatType()) \
    .add("spacing", FloatType()) \
    .add("speed", FloatType())

def read_from_kafka():
    df = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "earliest") \
        .load()
    logging.info(f"Kafka stream initialized. Schema: {df.schema}")
  return df

def parse_kafka_data(df):
    parsed = df.selectExpr("CAST(value AS STRING)") \
        .withColumn("value", from_json(col("value"), schema)) \
        .select("value.*") \
        .withColumn("time", to_timestamp(col("time"), "dd/MM/yyyy HH:mm:ss"))
    logging.info(f"Parsed Kafka data. Schema: {parsed.schema}")
    return parsed

def write_raw_to_mongo(batch_df, batch_id):
    try:
        count = batch_df.count()
        logging.info(f"Writing raw batch {batch_id} to MongoDB. Row count: {count}")
        if count > 0:
            batch_df.write \
                .format("mongodb") \
                .mode("append") \
                .option("database", "traffic_db") \
                .option("collection", "raw_data") \
.save()
            logging.info(f"Successfully wrote raw batch {batch_id} to MongoDB")
        else:
            logging.warning(f"Batch {batch_id} is empty. Nothing to write.")
    except Exception as e:
        logging.error(f"Error writing raw batch {batch_id} to MongoDB: {str(e)}", exc_info=True)

def write_processed_to_mongo(batch_df, batch_id):
    try:
        count = batch_df.count()
        logging.info(f"Processing batch {batch_id}. Row count: {count}")
        if count > 0:
            processed_df = batch_df.groupBy("time", "link").agg(
                count("name").alias("vcount"),
                avg("speed").alias("vspeed")
            )
            processed_df.write \
                .format("mongodb") \
                .mode("append") \
 .option("database", "traffic_db") \
                .option("collection", "processed_data") \
                .save()
            logging.info(f"Successfully wrote processed batch {batch_id} to MongoDB")
        else:
            logging.warning(f"Batch {batch_id} is empty. Nothing to process or write.")
    except Exception as e:
        logging.error(f"Error writing processed batch {batch_id} to MongoDB: {str(e)}", exc_info=True)

def main():
    try:
        # Read from Kafka as stream
        df = read_from_kafka()
  
        # Parse the Kafka data
        df_parsed = parse_kafka_data(df)
  
        # Stream raw data to MongoDB
        raw_query = df_parsed.writeStream \
.foreachBatch(write_raw_to_mongo) \
            .trigger(processingTime='10 seconds') \
            .start()
  
        # Stream processed data to MongoDB
        processed_query = df_parsed.writeStream \
            .foreachBatch(write_processed_to_mongo) \
            .trigger(processingTime='10 seconds') \
            .start()
  
        logging.info("Streaming queries started. Waiting for termination...")
  
        # Wait for both streams to terminate
        spark.streams.awaitAnyTermination()
    except Exception as e:
 logging.error(f"An error occurred: {str(e)}", exc_info=True)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
