#!/bin/bash

#calls the cloud funtion using https

if [ "#$" -ne 4 ]; then 
    echo "usage ./call_cf.sh destination-bucket-name compute-region ingest-url  personal-access-token"
    echo "   eg: ./call_cf.sh firm-alchemy-312121 us-central1 inget_flights_KEKhViL6QWe0LVIEFOx5XZPeGRSnJhHz DI8TWPzTedNF0b3B8meFPxXSWw6m3bKG"
    exit
fi

PROJECT=$(gcloud config get-value project)
BUCKET=$1
REGION=$2
UPATH=$3
TOKEN=$4

URL="https://${REGION}-${PROJECT}.cloudfunctions.net/${UPATH}"
echo $URL

echo {\"year\":\"2016\"\,\"month\":\"02\"\,\"bucket\":\"${BUCKET}\"\,\"token\":\"${TOKEN}\"} > /tmp/message
cat /tmp/message

curl -k -X POST $URL -H "Content-Type:application/json" --data-binary @/tmp/message
