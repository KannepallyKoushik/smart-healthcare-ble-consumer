import paho.mqtt.client as mqtt
import pika
import json

# Handler Functions for Each topic


def handle_bp(data):
    bp_dict = dict()
    bp_dict['systolic_blood_pressure'] = data
    bp_dict['diastolic_blood_pressure'] = data
    bp_json = json.dumps(bp_dict)
    print("Publishing it to RabbitMQ broker")
    publish_to_broker("blood_pressure", "bp.*", bp_json)


def handle_temp(data):
    temp_dict = dict()
    temp_dict['temperature'] = data
    temp_json = json.dumps(temp_dict)
    print("Publishing Temperature to RabbitMQ")
    publish_to_broker("temperature", "temp.*", temp_json)


def handle_emergency(data):
    print("Calling Emergency Services\n------------------------------------------")


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
        print("In Emergency Handler")
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
client.subscribe("bp")
client.subscribe("temp")
client.loop_forever()  # start the loop
