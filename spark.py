from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StringType, DoubleType

# Δημιουργία του SparkSession με τις απαραίτητες βιβλιοθήκες για το Kafka
spark = SparkSession.builder \
    .appName("KafkaStreamingProcessing") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1,org.apache.kafka:kafka-clients:2.6.0") \
    .getOrCreate()

# Ορισμός του σχήματος των δεδομένων
schema = StructType() \
    .add("name", StringType()) \
    .add("origin", StringType()) \
    .add("destination", StringType()) \
    .add("time", StringType()) \
    .add("link", StringType()) \
    .add("position", DoubleType()) \
    .add("spacing", DoubleType()) \
    .add("speed", DoubleType())

# Διαβάστε τα δεδομένα από το Kafka topic σε ένα streaming DataFrame
kafka_stream_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "vehicle_positions") \
    .option("startingOffsets", "earliest") \
    .load()

# Μετατροπή των δεδομένων σε συμβολοσειρά και στη συνέχεια σε JSON
kafka_stream_df = kafka_stream_df.selectExpr("CAST(value AS STRING)")

# Ανάλυση των JSON δεδομένων σύμφωνα με το σχήμα που ορίσαμε
vehicles_df = kafka_stream_df.select(from_json(col("value"), schema).alias("data")).select("data.*")

# Ομαδοποίηση δεδομένων και υπολογισμός των στατιστικών
vehicles_grouped_df = vehicles_df.groupBy("time", "link") \
    .agg(
        count("name").alias("vcount"),
        avg("speed").alias("vspeed")
    )

print(vehicles_grouped_df)
