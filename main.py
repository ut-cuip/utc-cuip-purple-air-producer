from flask import Flask, abort, request
import json
from kafka import KafkaProducer
import configparser
from datetime import datetime
import yaml

app = Flask(__name__)
with open("config.yml", "r") as f:
    config = yaml.load(f.read(), Loader=yaml.FullLoader)

producer = KafkaProducer(bootstrap_servers=config["kafka"]["bootstrap_servers"])
sensors = []
mac_topic_map = {value["mac"]: value["topic"] for value in config["sensors"]}
mac_nicename_map = {value["mac"]: value["nicename"] for value in config["sensors"]}


def format_location(reading):
    reading["location"] = f"{reading['lat']}, {reading['lon']}"
    return reading

def add_nicename(reading):
    reading["nicename"] = mac_nicename_map[reading["SensorId"]]
    return reading

def format_timestamp(reading):
    reading["timestamp"] = (
        1000 * datetime.strptime(reading["DateTime"], "%Y/%m/%dT%H:%M:%Sz").timestamp()
    )
    return reading


@app.route("/api", methods=["POST"])
def foo():
    try:
        if not request.json:
            abort(400)
        reading = json.loads(request.get_data())
        reading = format_location(reading)
        reading = format_timestamp(reading)
        reading = add_nicename(reading)
        sensorId = reading["SensorId"]
        producer.send(mac_topic_map[sensorId], json.dumps(reading).encode())
    except Exception as e:
        pass
    return ("", 200)


@app.route("/check", methods=["GET"])
def check_status():
    return ("PurpleAir Listener is running", 200)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=80, debug=True)
