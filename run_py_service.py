import os
import time

while 1:
	try:
		os.system("python3 mqtt.py")
		print("\n\n============================================\nRestarting Microservice in 10 secs")
		print("press Ctrl+C to exit before starting\n==========================")
		time.sleep(10)
	except KeyboardInterrupt:
		exit()
