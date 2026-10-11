import json
from kafka import KafkaConsumer

TOPIC, GROUP = "iss_telemetry", "telemetry-stats"
REQUIRED = {"record_time", "footprint_km"}

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers="localhost:9092",
    group_id=GROUP,
    auto_offset_reset="latest",
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
)
print(f"[Consumer 4 | {GROUP}] Computing running stats from '{TOPIC}'... (Ctrl+C to stop)")

count, total = 0, 0.0
fmin, fmax = float("inf"), float("-inf")
for msg in consumer:
    d = msg.value
    if not REQUIRED.issubset(d):
        print(f"[Consumer 4] INVALID message skipped: {d}")
        continue
    f = d["footprint_km"]
    count += 1
    total += f
    fmin, fmax = min(fmin, f), max(fmax, f)
    print(f"[Consumer 4] messages={count} | latest footprint={f:.1f} km | "
          f"avg={total/count:.1f} km | min={fmin:.1f} km | max={fmax:.1f} km")
