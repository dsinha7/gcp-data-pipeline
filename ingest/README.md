    export PROJECT_ID=$(gcloud info --format='value(config.project)')
    export BUCKET=${PROJECT_ID}
    gsutil mb gs://$BUCKET

    #firm-alchemy-312121
    