import json
from kafka import KafkaConsumer

TOPIC, GROUP = "iss_telemetry", "telemetry-logger"
REQUIRED = {"record_time", "footprint_km", "daynum", "solar_lat", "solar_lon", "units"}

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers="localhost:9092",
    group_id=GROUP,
    auto_offset_reset="latest",
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
)
print(f"[Consumer 3 | {GROUP}] Listening to '{TOPIC}'... (Ctrl+C to stop)")

for msg in consumer:
    d = msg.value
    if not REQUIRED.issubset(d):
        print(f"[Consumer 3] INVALID message skipped: {d}")
        continue
    print(f"[Consumer 3] offset={msg.offset} | {d['record_time']} | "
          f"footprint={d['footprint_km']:.1f} {d['units']} | daynum={d['daynum']:.5f} | "
          f"solar_lat={d['solar_lat']:.3f} solar_lon={d['solar_lon']:.3f}")
