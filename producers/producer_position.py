import json, time
from datetime import datetime, timezone
import requests, pycountry
from kafka import KafkaProducer

API_URL = "https://api.wheretheiss.at/v1/satellites/25544"
COORD_URL = "https://api.wheretheiss.at/v1/coordinates/{},{}"
TOPIC = "iss_position"

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

def ocean_name(lat, lon):
    """Approximate ocean from coordinates (used when not over a country)."""
    if lat <= -60:
        return "Southern Ocean"
    if lat >= 66:
        return "Arctic Ocean"
    if -70 <= lon <= 20:
        return "Atlantic Ocean"
    if 20 < lon <= 120 and lat < 30:
        return "Indian Ocean"
    return "Pacific Ocean"

def get_location(lat, lon):
    try:
        c = requests.get(COORD_URL.format(lat, lon), timeout=10).json()
        code = c.get("country_code", "??")
    except Exception:
        code = "??"
    if code and code != "??":
        country = pycountry.countries.get(alpha_2=code)
        return code, (country.name if country else code)
    return "OCEAN", ocean_name(lat, lon)

print(f"[Producer 1] Sending to '{TOPIC}'... (Ctrl+C to stop)")

while True:
    try:
        d = requests.get(API_URL, timeout=10).json()
        code, place = get_location(d["latitude"], d["longitude"])
        msg = {
            "record_time": datetime.fromtimestamp(d["timestamp"], timezone.utc).isoformat(),
            "latitude": round(d["latitude"], 6),
            "longitude": round(d["longitude"], 6),
            "altitude_km": round(d["altitude"], 3),
            "velocity_kmh": round(d["velocity"], 3),
            "visibility": d["visibility"],
            "country_code": code,
            "location": place,
        }
        producer.send(TOPIC, msg)
        producer.flush()
        print(f"[Producer 1] Sent: {msg}")
    except Exception as e:
        print(f"[Producer 1] Error: {e}")
    time.sleep(5)
