import json
import random
import uuid
from datetime import datetime, timedelta

# Seed for reproducibility
random.seed(42)

VEHICLE_IDS = [f"VH-{str(i).zfill(4)}" for i in range(1, 21)]
ROUTES = ["BLR-MYS", "BLR-HYD", "BLR-CHN", "MYS-COI", "HYD-VIZ"]

def generate_event(vehicle_id, timestamp):
    speed = round(random.gauss(72, 18), 2)
    engine_temp = round(random.gauss(90, 12), 2)
    # inject anomalies
    if random.random() < 0.03:
        speed = round(random.uniform(130, 160), 2)   # overspeed
    if random.random() < 0.02:
        engine_temp = round(random.uniform(115, 130), 2)  # overheat

    return {
        "event_id": str(uuid.uuid4()),
        "vehicle_id": vehicle_id,
        "event_timestamp": timestamp.isoformat(),
        "gps_lat": round(12.9716 + random.uniform(-0.5, 0.5), 6),
        "gps_lon": round(77.5946 + random.uniform(-0.5, 0.5), 6),
        "speed_kmh": max(0, speed),
        "engine_temp_c": engine_temp,
        "fuel_level_pct": round(random.uniform(10, 100), 1),
        "rpm": random.randint(700, 5500),
        "brake_pressure_bar": round(random.uniform(0, 80), 2),
        "odometer_km": random.randint(5000, 200000),
        "route": random.choice(ROUTES),
        "sensor_version": "v2.3.1",
        "ingestion_source": "event_hub_telemetry"
    }

events = []
base_time = datetime(2024, 1, 1, 6, 0, 0)
for vehicle in VEHICLE_IDS:
    t = base_time
    for _ in range(50):
        events.append(generate_event(vehicle, t))
        t += timedelta(seconds=random.randint(30, 120))

with open("telemetry_raw.json", "w") as f:
    for e in events:
        f.write(json.dumps(e) + "\n")

print(f"Generated {len(events)} telemetry events across {len(VEHICLE_IDS)} vehicles.")
