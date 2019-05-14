#!/usr/bin/env python

"""Extracts the results from Clowder
"""

import os
import sys
import json
import requests

API_BASE = "http://localhost:9000/api"

# Get the name of the test dataset
test_ds_name = "test_dataset"
argc = len(sys.argv)
if argc > 1:
    test_ds_name = sys.argv[1]

# Get all the dataset names
key = os.getenv("API_KEY")
KEY_PARAM = "key=%s" % (key)
headers = {"accept": "application/json"}

url = "%s/datasets?%s" % (API_BASE, KEY_PARAM)
res = requests.post(url, headers=headers)
res.raise_for_status()

# Create a storage locations for datasets
destdir = "./datasets"
os.makedirs(destdir)

# Get all the datasets
return_ds = {}
datasets = res.json()
for ds in datasets:
    if 'name' in ds and 'id' in ds and ds['name'] != test_ds_name:
        url = "%s/datasets/%s/files?%s" % (API_BASE, ds['id'], KEY_PARAM)
        res = requests.post(url, headers=headers)
        res.raise_for_status()

        # Download and store each file in the dataset under the dataset name
        files = res.json()
        ds_files = []
        for fn in files:
            url = "%s/files/%s?%s" % (API_BASE, fn['id'], KEY_PARAM)
            res = requests.get(url, stream=True)
            res.raise_for_status()

            filepath = os.path.join(destdir, ds['name'])
            if not os.path.isdir(filepath):
                os.makedirs(filepath)
            dest = os.path.join(filepath, fn['name'])
            try:
                with os.fdopen(dest, "w") as out_file:
                    for chunk in res.iter_content(chunk_size=10*1024):
                        out_file.write(dest)
            except:
                os.remove(dest)
                raise
            ds_files.append(dest)
        return_ds[ds['name']] = ds_files

print(json.dumps(return_ds))
