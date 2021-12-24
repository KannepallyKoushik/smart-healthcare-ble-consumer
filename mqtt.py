#!/usr/bin/env python3
import paho.mqtt.client as mqtt
import pika
import json
import requests

main_server_ip = "192.168.0.111"

# Handler Functions for Each topic


def handle_bp(data):
    # Consume BP and Pulse and seperate values
    mac, bp_pluse = data.split('-')
    bp_pulse = bp_pluse.split(',')

    bp_dict = dict()
    bp_dict['systolic_blood_pressure'] = bp_pulse[0]
    bp_dict['diastolic_blood_pressure'] = bp_pulse[1]
    bp_dict['pulse'] = bp_pulse[2]
    bp_pulse_json = json.dumps(bp_dict)
    print("Publishing BP and pulse to Rabbit MQTT server", bp_pulse_json)
    publish_to_broker("blood_pressure", mac, bp_pulse_json)


def handle_temp(data):
    data = data.split("-")
    temp_dict = dict()
    temp_dict['temperature'] = int(data[1])+16
    temp_json = json.dumps(temp_dict)
    print("Publishing Temperature to RabbitMQ")
    publish_to_broker("temperature", data[0], temp_json)


def handle_emergency(data):
    print("Emergency Alert called from device:", data,
          "\n------------------------------------------")
    emergency = dict()
    emergency['mac_address'] = data
    API_ENDPOINT = "http://"+main_server_ip+": 8000/consumer/emergency"
    r = requests.post(url=API_ENDPOINT, data=emergency)
    print(r)


# Functions to Connect to Brokers / Publish / Subscribe (Controllers)

def establish_Channels():
    topic_name1 = 'blood_pressure'
    topic_name2 = 'temperature'
    channel.exchange_declare(exchange=topic_name1, exchange_type='topic')
    channel.exchange_declare(exchange=topic_name2, exchange_type='topic')


def publish_to_broker(topic_name, routing_key, message):
    channel.basic_publish(exchange=topic_name,
                          routing_key=routing_key, body=message)
    print(" [x] Sent to RabbitMQ broker %r:%r" % (routing_key, message))


def on_connect(client, userdata, flags, rc):
    print("Connected to MQTT broker")


def on_message(client, userdata, message):
    data = message.payload
    topic = message.topic
    qos = message.qos
    data = data.decode('utf-8')
    # print("' Received message '", data, "' on topic '",
    #       str(topic), "' with QoS ", str(qos))

    if topic == 'bp':
        handle_bp(data)
    elif topic == 'temp':
        handle_temp(data)
    elif topic == 'emergency':
        handle_emergency(data)
    else:
        print("Unknown topic '", topic, "'")


def on_disconnect(client, userdata, rc):
    if rc != 0:
        print("Unexpected disconnection.")


# Initializing Connections with brokers
# 1.Mqtt
client = mqtt.Client()
client.username_pw_set("admin1", password='admin1')
client.connect("localhost", 1883)

# 2.RabbitMQ
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()
establish_Channels()

# Call back Functions for MQTT
client.on_connect = on_connect  # attach function to callback
client.on_message = on_message  # attach function to callback
client.on_disconnect = on_disconnect


# Subscriptions and Channel Establishments
client.subscribe("emergency")
client.subscribe("temp")
client.loop_forever()  # start the loop
