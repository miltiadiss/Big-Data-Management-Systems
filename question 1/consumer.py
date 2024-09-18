from kafka import KafkaConsumer
import json

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

try:
    # Διαβάζουμε τα μηνύματα από το Kafka broker
    for message in consumer:
        # Το μήνυμα είναι ένα αντικείμενο Kafka Message
        message_value = message.value
        print(f"Received: {message_value}")

except KeyboardInterrupt:
    print("Consumer interrupted by user.")

finally:
    # Κλείνουμε τον consumer
    consumer.close()
    print("Kafka consumer closed.")
