import csv
import random
from datetime import datetime, timedelta
import os

# Output path
os.makedirs("data/raw", exist_ok=True)
file_path = "data/raw/sensor_logs.csv"

# Generate data
num_records = 500
sensor_ids = [1001, 1002, 1003, 1004, 1005]
locations = ["Room_A", "Room_B", "Room_C", "Room_D"]

start_time = datetime(2025, 5, 1, 0, 0, 0)

with open(file_path, mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(["sensor_id", "timestamp", "temperature", "humidity", "location"])

    for i in range(num_records):
        sensor_id = random.choice(sensor_ids)
        timestamp = start_time + timedelta(seconds=i * 5)
        temperature = round(random.uniform(65.0, 80.0), 2)
        humidity = round(random.uniform(30.0, 60.0), 2)
        location = random.choice(locations)

        writer.writerow([sensor_id, timestamp.strftime('%Y-%m-%d %H:%M:%S'), temperature, humidity, location])

print(f"✅ Generated {num_records} IoT sensor records at: {file_path}")

