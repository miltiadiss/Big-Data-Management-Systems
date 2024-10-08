kafka import KafkaConsumer
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType, TimestampType
from pyspark.sql.functions import col, avg, count, to_timestamp

# Δημιουργία SparkSession
spark = SparkSession.builder \
    .appName("KafkaSparkProcessing") \
    .getOrCreate()

# Σχημα των δεδομένων
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

# Ορίζουμε τις παραμέτρους του Kafka broker και το topic
kafka_broker = 'localhost:9092'
topic = 'vehicle_positions'

# Δημιουργούμε ένα KafkaConsumer αντικείμενο
consumer = KafkaConsumer(
    topic,
    bootstrap_servers=[kafka_broker],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='vehicle_positions_group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("Kafka Consumer is listening...")

# Αρχικοποίηση ενός κενού DataFrame για συσσώρευση δεδομένων
accumulated_df = spark.createDataFrame([], schema)

def process_message(message):
 # Επεξεργασία του μηνύματος
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

try:
    # Διαβάζουμε τα μηνύματα από το Kafka broker
    for message in consumer:
        # Το μήνυμα είναι ένα αντικείμενο Kafka Message
        message_value = message.value
        print(f"Received: {message_value}")

        # Επεξεργασία του μηνύματος
        processed_data = [process_message(message_value)]  # Process one vehicle at a time

        # Μετατροπή σε DataFrame
        df = spark.createDataFrame(processed_data, schema=schema)

        # Μετατροπή της στήλης 'time' σε TimestampType χρησιμοποιώντας to_timestamp
        df = df.withColumn("time", to_timestamp(col("time"), "dd/MM/yyyy HH:mm:ss"))

        # Συσσώρευση δεδομένων σε ένα DataFrame που περιέχει όλα τα δεδομένα μέχρι τώρα
        accumulated_df = accumulated_df.union(df)

        # Υπολογισμός vcount και vspeed για όλα τα δεδομένα που έχουν ληφθεί μέχρι τώρα
        result_df = accumulated_df.groupBy("time", "link") \
            .agg(
                count("name").alias("vcount"),
                avg("speed").alias("vspeed")
            )

        # Εμφάνιση του DataFrame με τα ενημερωμένα vcount και vspeed
        result_df.show()
except KeyboardInterrupt:
    print("Consumer interrupted by user.")

finally:
    # Κλείνουμε τον consumer
    consumer.close()
    print("Kafka consumer closed.")


