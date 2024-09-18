import pandas as pd
import json
import time
from kafka import KafkaProducer
from datetime import datetime, timedelta

# Διαβάζουμε τα δεδομένα από το CSV αρχείο
csv_file_path = 'sources/vehicles.csv'
data = pd.read_csv(csv_file_path)

# Ορίζουμε τις παραμέτρους του Kafka broker
kafka_broker = 'localhost:9092'
topic = 'vehicle_positions'

# Δημιουργούμε ένα KafkaProducer αντικείμενο
producer = KafkaProducer(
    bootstrap_servers=[kafka_broker],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

# Ημερομηνία και ώρα εκκίνησης του producer
start_time = datetime.now()

# Διάστημα χρόνου σε δευτερόλεπτα
N = 5

# Συνάρτηση για να δημιουργήσουμε το JSON αντικείμενο
def create_json_object(row, send_time):
    return {
        "name": row["name"],
        "origin": row["orig"],
        "destination": row["dest"],
        "time": send_time.strftime("%d/%m/%Y %H:%M:%S"),
        "link": row["link"],
        "position": row["x"],
        "spacing": row["s"],
        "speed": row["v"]
    }

try:
    # Στέλνουμε τα δεδομένα στον Kafka broker κάθε Ν δευτερόλεπτα
    for index, row in data.iterrows():
        # Υπολογίζουμε τον χρόνο αποστολής βασισμένο στην ώρα εκκίνησης και το πεδίο `t`
        send_time = start_time + timedelta(seconds=row["t"])

        # Αν το όχημα είναι "waiting at origin node", παραλείπουμε το μήνυμα
        if row["link"] == "waiting at origin node":
            continue

        # Δημιουργούμε το JSON αντικείμενο
        json_object = create_json_object(row, send_time)

        # Περιμένουμε μέχρι την κατάλληλη στιγμή για να στείλουμε το μήνυμα
        time_to_wait = (send_time - datetime.now()).total_seconds()
        if time_to_wait > 0:
            time.sleep(time_to_wait)

        # Στέλνουμε το μήνυμα στον Kafka broker
        producer.send(topic, value=json_object)
        print(f"Sent: {json_object}")

except KeyboardInterrupt:
    print("Simulation interrupted by user.")

finally:
    # Κλείνουμε τον producer
    producer.close()
    print("Kafka producer closed.")
