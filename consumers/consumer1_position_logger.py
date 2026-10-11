import json
from kafka import KafkaConsumer

TOPIC, GROUP = "iss_position", "position-logger"
REQUIRED = {"record_time", "latitude", "longitude", "altitude_km", "velocity_kmh", "visibility"}

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers="localhost:9092",
    group_id=GROUP,
    auto_offset_reset="latest",
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
)
print(f"[Consumer 1 | {GROUP}] Listening to '{TOPIC}'... (Ctrl+C to stop)")

for msg in consumer:
    d = msg.value
    if not REQUIRED.issubset(d):
        print(f"[Consumer 1] INVALID message skipped: {d}")
        continue
    print(f"[Consumer 1] offset={msg.offset} | {d['record_time']} | "
          f"lat={d['latitude']:.4f} lon={d['longitude']:.4f} | "
          f"alt={d['altitude_km']:.1f} km | vel={d['velocity_kmh']:.0f} km/h | "
          f"{d['visibility']} | over: {d.get('location', '-')}")
