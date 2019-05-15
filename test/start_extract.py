#!/usr/bin/env python

"""Starts the extract process
"""

import sys
import requests

data_load = sys.argv[1]
uri = sys.argv[2]

print("Data: >" + data_load + "<")
print("URL: " + uri)

headers = {"accept": "application/json", "Content-Type":"application/json"}

res = requests.post(uri, headers=headers, data=data_load)
res.raise_for_status()
