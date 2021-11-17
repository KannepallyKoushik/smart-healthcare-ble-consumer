import json
from random import randrange

records = []

for i in range(1000):
	records.append({
			"systolic_blood_pressure": {
	        		"value": randrange(70, 190),
				"unit": "mmHg"
    		},
    			"diastolic_blood_pressure": {
        			"value": randrange(40, 130),
        			"unit": "mmHg"
    		},
    			"effective_time_frame": {
        			"date_time": "2020-%02d-%02dT%02d:%02d:%02d-%02d:%02d"%(randrange(1,12), randrange(1,30), randrange(0, 23),randrange(0, 59), randrange(0, 59), randrange(0, 10), randrange(0, 59))
    		}
	})

with open("bp_data_file.json", "w") as write_file:
	json.dump(records, write_file, indent=4)
