import json, math
from kafka import KafkaConsumer

TOPIC, GROUP = "iss_position", "position-tracker"
REQUIRED = {"record_time", "latitude", "longitude"}

def distance_km(lat1, lon1, lat2, lon2):
    """Great-circle distance between two points (Haversine formula)."""
    r = 6371
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = math.radians(lat2 - lat1), math.radians(lon2 - lon1)
    a = math.sin(dp/2)**2 + math.cos(p1) * math.cos(p2) * math.sin(dl/2)**2
    return 2 * r * math.asin(math.sqrt(a))

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers="localhost:9092",
    group_id=GROUP,
    auto_offset_reset="latest",
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
)
print(f"[Consumer 2 | {GROUP}] Tracking ISS location from '{TOPIC}'... (Ctrl+C to stop)")

prev, last_place, total_km = None, None, 0.0
for msg in consumer:
    d = msg.value
    if not REQUIRED.issubset(d):
        print(f"[Consumer 2] INVALID message skipped: {d}")
        continue
    place = d.get("location", "Unknown")
    if prev:
        step = distance_km(prev["latitude"], prev["longitude"], d["latitude"], d["longitude"])
        total_km += step
        print(f"[Consumer 2] Moved {step:.1f} km since last update | total tracked: {total_km:.1f} km")
    if place != last_place:
        if last_place:
            print(f"[Consumer 2] *** LOCATION CHANGE: {last_place} -> {place} at {d['record_time']} ***")
        else:
            print(f"[Consumer 2] ISS is currently over: {place}")
        last_place = place
    prev = d
