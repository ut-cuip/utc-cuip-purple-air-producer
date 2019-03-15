from flask import Flask, abort, request
import json
from kafka import KafkaProducer
import configparser

app = Flask(__name__)
config = configparser.ConfigParser()
config.read('config.ini')
producer = KafkaProducer(bootstrap_servers=config['KAFKA']['bootstrap_servers'])

@app.route('/api', methods=['POST'])
def foo():
    if not request.json:
        abort(400)
    #print(request.get_data())
    reading = json.loads(request.get_data())
    sensorId = reading['SensorId']
    if sensorId == "84:f3:eb:44:d8:24":
        print("MLK/Central")
        producer.send(config['KAFKA']['central_topic'], bytes(str(request.get_data()).encode()))
    elif sensorId == "84:f3:eb:91:44:60":
        print("MLK/Douglas")
        producer.send(config['KAFKA']['douglas_topic'], bytes(str(request.get_data()).encode()))
    elif sensorId == "84:f3:eb:45:1a:53":
        print("MLK/Magnolia")
        producer.send(config['KAFKA']['magnolia_topic'], bytes(str(request.get_data()).encode()))
    elif sensorId == "84:f3:eb:91:44:38":
        print("MLK/Peeples")
        producer.send(config['KAFKA']['peeples_topic'],null, request.get_data())
    
#    producer.send(config['KAFKA']['topic'], bytes(
#        str(request.get_data()).encode()))
    return ('', 200)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=80, debug=True)
