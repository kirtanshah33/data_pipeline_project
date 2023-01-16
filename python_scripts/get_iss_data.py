import csv
import requests
import json

import time

t_end = time.time() + 60 * 1
all_data = []
while time.time() < t_end:
    api_response = requests.get(
        "http://api.open-notify.org/iss-now.json")

    # print(api_response.content)

    obj = json.loads(api_response.content)

    t = obj['timestamp']
    lat = obj['iss_position']['latitude']
    lon = obj['iss_position']['latitude']
    current_pass = []

    #store the timestamp and lat/log from the request
    current_pass.append(t)   
    current_pass.append(lat)
    current_pass.append(lon)

    all_data.append(current_pass)

export_file = "export_iss_data_file.csv"

with open(export_file, 'w') as fp:
	csvw = csv.writer(fp, delimiter='|')
	csvw.writerows(all_data)

fp.close()