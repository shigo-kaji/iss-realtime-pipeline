import json, time
from datetime import datetime, timezone
import requests
from kafka import KafkaProducer

API_URL = "https://api.wheretheiss.at/v1/satellites/25544"
TOPIC = "iss_telemetry"

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)
print(f"[Producer 2] Sending to '{TOPIC}'... (Ctrl+C to stop)")

while True:
    try:
        d = requests.get(API_URL, timeout=10).json()
        msg = {
            "record_time": datetime.fromtimestamp(d["timestamp"], timezone.utc).isoformat(),
            "footprint_km": round(d["footprint"], 3),
            "daynum": d["daynum"],
            "solar_lat": round(d["solar_lat"], 6),
            "solar_lon": round(d["solar_lon"], 6),
            "units": d["units"],
        }
        producer.send(TOPIC, msg)
        producer.flush()
        print(f"[Producer 2] Sent: {msg}")
    except Exception as e:
        print(f"[Producer 2] Error: {e}")
    time.sleep(5)
