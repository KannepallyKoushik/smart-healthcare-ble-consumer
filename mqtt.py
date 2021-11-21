# mqtt_sub.py - Python MQTT subscribe example
#
import paho.mqtt.client as mqtt


def on_connect(client, userdata, flags, rc):
    print("Connected to broker")


def on_message(client, userdata, message):
    data = message.payload
    topic = message.topic
    qos = message.qos
    print("' Received message '" + data.decode('utf-8') + "' on topic '" +
          topic.decode('utf-8') + "' with QoS " + qos.decode('utf-8'))


def on_disconnect(client, userdata, rc):
    if rc != 0:
        print("Unexpected disconnection.")


client = mqtt.Client()
client.username_pw_set("admin1", password='admin1')
client.connect("localhost", 1883)

client.on_connect = on_connect  # attach function to callback
client.on_message = on_message  # attach function to callback
client.on_disconnect = on_disconnect

client.subscribe("bp")
client.loop_forever()  # start the loop
