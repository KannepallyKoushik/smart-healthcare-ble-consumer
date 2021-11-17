import bluetooth
import json
import time

sensor_address = '78:67:D7:07:51:39'
socket = bluetooth.BluetoothSocket(bluetooth.RFCOMM)
socket.connect((sensor_address, 1))

buffer = ""

while True:
    data = socket.recv(1024)
    buffer += str(data, encoding='ascii')
    try:
        data = json.loads(buffer)
        print("Received chunk", data)
        # log_stats(influx, sensor_address, data)
        buffer = ""
        time.sleep(3)
        print("Sleep completed")
    except json.JSONDecodeError as e:
        continue

socket.close()
