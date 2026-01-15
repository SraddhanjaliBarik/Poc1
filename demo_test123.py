
import json
from datetime import datetime
import paho.mqtt.client as mqtt
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

# -----------------------------
# InfluxDB setup
# -----------------------------
INFLUX_URL = "https://eu-central-1-1.aws.cloud2.influxdata.com"  # no spaces, no trailing slash
TOKEN = "GwSWdg8UQncF4Tojs-ANSOz8Ja40XJUiVf7f9Wh3SHdjqZ2MpESnH6RU3pNKEltFI7j62w00DhzeFM3lpPK78A=="  # Replace with your token
ORG = "cgi"
BUCKET = "Poc_Data"

influx_client = InfluxDBClient(url=INFLUX_URL, token=TOKEN, org=ORG)  # TLS verify enabled by default
write_api = influx_client.write_api(write_options=SYNCHRONOUS)

# Optional: quick health check (run once)
try:
    health = influx_client.health()
    print("InfluxDB health:", health.status if hasattr(health, "status") else health)
except Exception as e:
    print("InfluxDB health check failed:", e)

# -----------------------------
# MQTT setup
# -----------------------------
BROKER = "mqtt.tdengine.com"
PORT = 1883
TOPICS = [("sites", 0), ("inverters", 0), ("strings", 0), ("weather", 0), ("grid", 0)]

def get_measurement(topic: str) -> str:
    """Return measurement name based on MQTT topic"""
    t = topic.strip().lower()
    # Name measurements a bit more standard (no spaces)
    return f"industrial_{t}"

def flatten_json(obj, parent_key="", sep="_"):
    """Recursively flattens nested dicts/lists into a single level dict with composite keys."""
    items = {}
    if isinstance(obj, dict):
        for k, v in obj.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else str(k)
            items.update(flatten_json(v, new_key, sep=sep))
    elif isinstance(obj, list):
        for i, v in enumerate(obj):
            new_key = f"{parent_key}{sep}{i}" if parent_key else str(i)
            items.update(flatten_json(v, new_key, sep=sep))
    else:
        items[parent_key] = obj
    return items

# -----------------------------
# MQTT callbacks
# -----------------------------
def on_connect(client, userdata, flags, rc):
    print(f"Connected to TDengine MQTT Broker (rc={rc})")
    for topic, qos in TOPICS:
        client.subscribe(topic, qos)
    print("Subscribed to topics:", [t[0] for t in TOPICS])

def on_message(client, userdata, msg):
    try:
        raw = msg.payload.decode(errors="ignore").strip()
        if not raw:
            print("Empty payload on topic:", msg.topic)
            return

        # Try JSON; if not JSON, treat as scalar/string
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            parsed = raw

        measurement = get_measurement(msg.topic)
        point = Point(measurement).time(datetime.utcnow(), WritePrecision.NS).tag("topic", msg.topic)

        had_numeric_field = False

        if isinstance(parsed, dict):
            flat = flatten_json(parsed)  # flatten nested JSON
            for key, value in flat.items():
                # Numeric/boolean -> fields; everything else -> tag (careful with cardinality)
                if isinstance(value, (int, float, bool)):
                    point.field(str(key), value)
                    had_numeric_field = True
                else:
                    # Prefer keeping identifiers/status fields as tags; avoid turning large texts into tags
                    sval = str(value)
                    if len(sval) <= 128:  # crude guard to avoid huge cardinality
                        point.tag(str(key), sval)
                    else:
                        point.field(str(key), sval)  # long strings as fields
            if not had_numeric_field:
                point.field("raw_present", True)  # ensure at least one field
        else:
            # Non-JSON payload
            try:
                scalar = float(parsed)
                point.field("value", scalar)
            except ValueError:
                point.field("raw", str(parsed))  # store as field to keep point valid

        # Write point to InfluxDB (synchronous for debugging)
        write_api.write(bucket=BUCKET, record=point)
        print(f"Written {msg.topic} -> {measurement}")

    except Exception as e:
        print("Error processing message:", e)

# -----------------------------
# MQTT client connection
# -----------------------------
# Enforce v1 callback API to avoid signature mismatches on paho-mqtt>=2.0
client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1)
client.on_connect = on_connect
client.on_message = on_message

# If the broker requires auth, set it here:
# client.username_pw_set("user", "password")

client.connect(BROKER, PORT, keepalive=60)
client.loop_forever()

