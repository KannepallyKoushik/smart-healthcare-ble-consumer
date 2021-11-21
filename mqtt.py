import paho.mqtt.client as mqtt
import pika
import json
import time


# Functions to Connect to Brokers / Publish / Subscribe (Controllers)

def establish_Channels():
    topic_name = 'blood_pressure'
    channel.exchange_declare(exchange=topic_name, exchange_type='topic')


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
    print("' Received message '", int(data), "' on topic '",
          str(topic), "' with QoS ", str(qos))
    bp_dict = dict()
    bp_dict['systolic_blood_pressure'] = int(data)
    bp_json = json.dumps(bp_dict)
    print("Publishing it to RabbitMQ broker")
    publish_to_broker("blood_pressure", "bp.*", bp_json)


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
client.subscribe("bp")
client.loop_forever()  # start the loop
