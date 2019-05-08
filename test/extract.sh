#!/bin/bash

set -ev

# Make the folder for the data files
mkdir data

# Copy and decompress the TAR file
cp test_data.tar data/
cd  data
tar -xv -f test_data.tar

# Remove the tar file copy
rm test_data.tar

