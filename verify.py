import json
from datetime import datetime, timezone
import paho.mqtt.client as mqtt
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

# -------- InfluxDB --------
INFLUX_URL = "https://eu-central-1-1.aws.cloud2.influxdata.com/"  # Replace with your InfluxDB URL
TOKEN = "Token name here"  # Replace with your token you created in influx db
ORG = "org name here"  # Replace with your org you created in influx db
BUCKET = "Bucket name here" # Replace with your bucket you created in influx db

client_influx = InfluxDBClient(url=INFLUX_URL, token=TOKEN, org=ORG)
write_api = client_influx.write_api(write_options=SYNCHRONOUS)

print("Influx health:", client_influx.health().status)

# -------- MQTT --------
BROKER = "mqtt.tdengine.com"
PORT = 1883
TOPICS = ["sites", "inverters", "strings", "weather", "grid"]

def measurement(topic):
    return f"industrial_{topic}"

def flatten(obj, parent=""):
    out = {}
    if isinstance(obj, dict):
        for k, v in obj.items():
            out.update(flatten(v, f"{parent}_{k}" if parent else k))
    elif isinstance(obj, list):
        for i, v in enumerate(obj):
            out.update(flatten(v, f"{parent}_{i}"))
    else:
        out[parent] = obj
    return out

def on_connect(client, userdata, flags, rc):
    print("Connected to MQTT:", rc)
    for t in TOPICS:
        client.subscribe(t)

def on_message(client, userdata, msg):
    try:
        data = json.loads(msg.payload.decode())
        point = (
            Point(measurement(msg.topic))
            .tag("topic", msg.topic)
            .time(datetime.now(timezone.utc), WritePrecision.NS)
        )

        for k, v in flatten(data).items():
            if isinstance(v, (int, float, bool)):
                point.field(k, float(v))  # 🔑 avoid schema conflict
            else:
                if len(str(v)) < 100:
                    point.tag(k, str(v))
                else:
                    point.field(k, str(v))

        write_api.write(bucket=BUCKET, record=point)
        print(f"✅ Written {msg.topic}")

    except Exception as e:
        print("❌ Error:", e)

mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1)
mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message
mqtt_client.connect(BROKER, PORT, 60)
mqtt_client.loop_forever()

