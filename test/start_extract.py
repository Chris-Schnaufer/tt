#!/usr/bin/env python
  
"""Starts the extract process
"""

import sys
import json
import time
import requests

data_load = sys.argv[1]
uri = sys.argv[2]

print("Data: >" + data_load + "<")
print("URL: " + uri)

json_data = json.loads(str(data_load))
headers = {"accept": "application/json", "Content-Type":"application/json"}

print("JSON data: "+str(json_data))
print("headers: " + str(headers))

for i in range(1, 5):
    res = requests.post(uri, headers=headers, data=json.dumps(json_data))
    res.raise_for_status()
    time.sleep(5)