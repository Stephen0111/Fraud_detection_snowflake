from confluent_kafka import Producer
import json
import random
import time

# 🔴 CHANGE THIS
KAFKA_BOOTSTRAP_SERVERS = "vitalstream-kafka-stephen-c769.d.aivencloud.com:16892"

conf = {
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "security.protocol": "SSL",
    "ssl.ca.location": "kafka-certs/ca.pem",
    "ssl.certificate.location": "kafka-certs/service.cert",
    "ssl.key.location": "kafka-certs/service.key"
}

producer = Producer(conf)


def delivery_report(err, msg):
    if err:
        print("❌ Delivery failed:", err)
    else:
        print(f"✅ Sent to {msg.topic()} [{msg.partition()}]")


print("📡 Starting vitals producer...")

while True:
    data = {
        "patient_id": "patient_001",
        "heart_rate": random.randint(60, 150),
        "spo2": random.randint(85, 100),
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S")
    }

    producer.produce(
        topic="patient_vitals",
        key=data["patient_id"].encode("utf-8"),
        value=json.dumps(data).encode("utf-8"),
        callback=delivery_report
    )

    producer.poll(0)
    time.sleep(2)
