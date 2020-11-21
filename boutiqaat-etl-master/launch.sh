#!/bin/sh
/app/google-cloud-sdk/bin/gcloud auth activate-service-account --key-file /credentials/bq_credentials.json --project "boutiqaat-online-shopping" -q
$*