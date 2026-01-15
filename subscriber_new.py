import json
import paho.mqtt.client as mqtt
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import ASYNCHRONOUS
from datetime import datetime
import time
 
# MQTT Broker details
BROKER = "broker.hivemq.com"
PORT = 1883
TOPIC = "sensors/#"  # Subscribe to all sensor topics
 
# InfluxDB details
INFLUX_URL = " https://eu-central-1-1.aws.cloud2.influxdata.com/ "
TOKEN = "GwSWdg8UQncF4Tojs-ANSOz8Ja40XJUiVf7f9Wh3SHdjqZ2MpESnH6RU3pNKEltFI7j62w00DhzeFM3lpPK78A=="  # Replace with your token
ORG = "cgi"
BUCKET = "Poc_Data"
 
# Setup InfluxDB client
influx_client = InfluxDBClient(url=INFLUX_URL, token=TOKEN, org=ORG, verify_ssl=False)
write_api = influx_client.write_api(write_options=ASYNCHRONOUS)
 
def get_measurement(topic):
    """Determine measurement name based on topic."""
    topic = topic.lower()
    print("topic======",topic)
    if "temperature" in topic or "aht20" in topic or "suhu" in topic:
        return "sensor_temperature"
    elif "humidity" in topic or "hum" in topic:
        return "sensor_humidity"
    elif "light" in topic or "lux" in topic:
        return "sensor_light"
    elif "ppfd" in topic:
        return "sensor_ppfd"
    elif "status" in topic:
        return "sensor_status"
    else:
        return "sensor_misc"
 
def on_connect(client, userdata, flags, rc):
    print("Connected to MQTT Broker")
    client.subscribe(TOPIC)
 
def on_message(client, userdata, msg):
    try:
        raw_payload = msg.payload.decode().strip()
        if not raw_payload:
            print("Empty payload received")
            return
 
        # Try to parse JSON, fallback to scalar
        try:
            payload = json.loads(raw_payload)
        except json.JSONDecodeError:
            payload = raw_payload
 
        print(f"Received on {msg.topic}: {payload}")
 
        # Determine measurement
        measurement = get_measurement(msg.topic)
 
        # Prepare InfluxDB point
        point = Point(measurement).tag("topic", msg.topic).time(datetime.utcnow(), WritePrecision.NS)
 
        # Normalize fields and tags
        if isinstance(payload, dict):
            for key, value in payload.items():
                if isinstance(value, (int, float)):
                    point.field(key, value)
                else:
                    point.tag(key, str(value))
        else:
            # Scalar value
            try:
                scalar = float(payload)
                point.field("value", scalar)
            except ValueError:
                point.tag("raw", str(payload))
 
        # Write to InfluxDB
        write_api.write(bucket=BUCKET, record=point)
        print(f"Written to InfluxDB: {measurement}")
 
    except Exception as e:
        print("Error processing message:", e)
 
# MQTT Client setup
client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message
 
client.connect(BROKER, PORT, 60)
client.loop_forever()
 