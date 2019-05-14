#!/usr/bin/env python

"""Registers the extractor using the file passed in as a parameter
"""
import re
import sys
import time
import datetime
import subprocess

CONTAINER_ID_LOOP_MAX = 10
SLEEP_SECONDS_ID = 5
SLEEP_SECONDS_FINISH = 20

# Make sure we're configured correctly
num_args = len(sys.argv)
if num_args < 2:
    raise RuntimeError("Missing the extractor name")

extractorName = sys.argv[1].strip()

# Find the ID
dockerId = None
for i in range(0, CONTAINER_ID_LOOP_MAX):
    res = subprocess.check_output(["/bin/bash", "-c", "docker ps | grep '" + extractorName +
                                  "' || echo ' '"])
    print("Check result: "+str(res))
    if not extractorName in str(res):
        print("Sleeping while waiting for extractor...")
        time.sleep(SLEEP_SECONDS_ID)
    else:
        try:
            dockerId = re.search(r"^\S*", res).group(0)
        except Exception:
            pass

        if not dockerId is None:
            break

if dockerId is None:
    raise RuntimeError("Unable to find Docker ID of extractor: '" + extractorName + "'")

# Loop here until we detect the end of processing
done = False
starttime = datetime.datetime.now()
print("Begining monitoring of extractor: " + extractorName)
while not done:
    res = subprocess.check_output(["/bin/bash", "-c", "docker logs " + dockerId + " 2>&1 | tail -n 50"])
    if "StatusMessage.done: Done processing" in res:
        print("Detected end of processing")
        sys.exit(0)
    if "exit status" in res:
        print("Extractor status command exited with an error.")
        print("Partial results follows.")
        print(res)
        raise RuntimeError("Early exit from checking docker container status: "  + extractorName)
    if "Traceback" in res:
        print("Docker container appears to have thrown an unhandled exception")
        print("Partial results follow.")
        print(res)
        raise RuntimeError("Container threw an exception: " + extractorName)
    curtime = datetime.datetime.now()
    timedelta = curtime - starttime
    print("Sleep while wiating on container: " + timedelta.total_seconds() + " elapsed seconds")
    time.sleep(SLEEP_SECONDS_FINISH)
