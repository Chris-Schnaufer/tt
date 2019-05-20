#!/bin/bash

set -ev

# Make the folder for the data files
mkdir data

# Copy and decompress the TAR file
cp "$1" data/
cd  data
tar -xv -f "$1"

# Remove the tar file copy
rm "$1"

export MESSAGE_TEST="THis is a test message"
