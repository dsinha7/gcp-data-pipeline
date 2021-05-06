#!/bin/bash
echo "Uploading to bucket $BUCKET..."
gsutil -m cp *.csv gs://$BUCKET/flights/raw/
