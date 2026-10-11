from cassandra.cluster import Cluster
from flask import Flask, jsonify, render_template

app = Flask(__name__)

cluster = Cluster(["localhost"], port=9042)
session = cluster.connect("iss_data")

def fetch(table, limit=50):
    rows = session.execute(
        f"SELECT * FROM {table} WHERE satellite='ISS' LIMIT %s", (limit,)
    )
    return [r._asdict() for r in rows]

def count(table):
    return session.execute(
        f"SELECT COUNT(*) FROM {table} WHERE satellite='ISS'"
    ).one()[0]

@app.route("/")
def index():
    position = fetch("iss_position")
    telemetry = fetch("iss_telemetry")
    points = [[r["latitude"], r["longitude"]] for r in position
              if r["latitude"] is not None and r["longitude"] is not None]
    latest = position[0] if position else None
    return render_template(
        "index.html",
        position=position,
        telemetry=telemetry,
        latest=latest,
        location=(latest.get("location") if latest else None) or "Unknown",
        points=points,
        pos_count=count("iss_position"),
        tel_count=count("iss_telemetry"),
    )

@app.route("/api/data")
def api_data():
    return jsonify(position=fetch("iss_position", 20),
                   telemetry=fetch("iss_telemetry", 20))

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
