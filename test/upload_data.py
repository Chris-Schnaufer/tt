#!/usr/bin/env python

"""Simple python file to upload files to a dataset
"""
import sys
import os
import requests

# pylint: disable=invalid-name

# Get the list of files to upload
data_path = "./data"
files = [os.path.join(data_path, f) for f in os.listdir(data_path) if os.path.isfile(os.path.join(data_path, f))]
num_files = len(files)
if num_files <= 0:
    sys.exit(0)

# Prepare to upload the files
key = os.getenv("API_KEY")
dataset = os.getenv("DATASET_ID")

url = "https://localhost:9000/api/uploadToDataset/" + dataset + "?extract=false&key=" + key
headers = {"Content-Type": "multipart/form-data", "accept": "application/json"}

for one_file in files:
    print("Attempting upload of file '" + one_file + "'")
    with open(one_file, 'rb') as fh:
        res = requests.post(url, headers=headers, data={"File": fh})
        res.raise_for_status()
