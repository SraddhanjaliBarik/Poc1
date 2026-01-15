import json
import time
import requests
import paho.mqtt.client as mqtt
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from datetime import datetime

# -----------------------------
# MQTT Broker
# -----------------------------
BROKER = "test.mosquitto.org"
PORT = 1883
TOPIC = "industrial/weather"


client = mqtt.Client(client_id="weather_iot", protocol=mqtt.MQTTv311)
client.connect(BROKER, PORT, 60)

# -----------------------------
# InfluxDB Config
# -----------------------------
INFLUX_URL = "https://eu-central-1-1.aws.cloud2.influxdata.com/"  # Replace with your InfluxDB URL
TOKEN = "EHIwyUgnGkMTIbXxHYizFPqHT2lrsKl7C0daukFuXjYjzeX_yY0-2D5SEHCImFmv7zNNCTwyHN_ZvfmWmZPDUw=="  # Replace with your token
ORG = "cgi"
BUCKET = "Factory_Data"

influx_client = InfluxDBClient(url=INFLUX_URL, token=TOKEN, org=ORG)
write_api = influx_client.write_api(write_options=SYNCHRONOUS)

# -----------------------------
# OpenWeatherMap Config
# -----------------------------
CITY = "Stockholm,SE"
API_KEY = "4527e20130b2d03e5ee77f6c48d17222"
URL = f"http://api.openweathermap.org/data/2.5/weather?q={CITY}&appid={API_KEY}&units=metric"

# -----------------------------
# Continuous Fetch & Publish
# -----------------------------
try:
    while True:
        response = requests.get(URL)
        data = response.json()

        payload = {
            "temperature": data["main"]["temp"],
            "humidity": data["main"]["humidity"],
            "pressure": data["main"]["pressure"]
        }

        # Publish to MQTT
        client.publish(TOPIC, json.dumps(payload))
        print(f"Published to MQTT: {payload}")

        # Write to InfluxDB
        point = Point("industrial_iot").time(datetime.utcnow(), WritePrecision.NS)
        for key, value in payload.items():
            point.field(key, value)
        write_api.write(bucket="Factory_Data", record=point)

        time.sleep(60)  # Fetch every 1 minute

except KeyboardInterrupt:
    print("Stopped.")
    client.disconnect()
