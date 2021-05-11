#!/bin/bash 
REGION=us-central1
URL='inget_flights_KEKhViL6QWe0LVIEFOx5XZPeGRSnJhHz'
echo $URL
gcloud functions deploy $URL --entry-point ingest_flights --runtime python37 --trigger-http --timeout 480s --allow-unauthenticated --region=$REGION
