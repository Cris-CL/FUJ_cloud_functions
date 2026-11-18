#!/usr/bin/env bash
# Deploys the upload-amazon Cloud Function (Gen 2) from this folder.
#
# Usage:  make deploy upload-amazon      (or: bash amazon/upload_amazon/deploy.sh)
# Check:  make check upload-amazon
#
# The values below reproduce the live configuration (project test-bigquery-cc, us-central1,
# python312, GCS finalize trigger on bucket amazon-raw, no retry). It was last edited in the
# console; deploy from here only.
set -euo pipefail
FUNCTION=upload-amazon
REGION=us-central1
PROJECT=test-bigquery-cc
cd "$(dirname "$0")"

${GCLOUD:-gcloud} functions deploy "$FUNCTION" \
  --project "$PROJECT" \
  --gen2 \
  --region "$REGION" \
  --runtime python312 \
  --source . \
  --entry-point amazon_process \
  --trigger-bucket amazon-raw \
  --trigger-location us-central1 \
  --service-account 967608949943-compute@developer.gserviceaccount.com \
  --trigger-service-account 967608949943-compute@developer.gserviceaccount.com \
  --memory 512Mi \
  --timeout 60s \
  --max-instances 1000 \
  --concurrency 1 \
  --set-env-vars DATASET_ID=Amazon,NEW_BUCKET=fujiorg-sales-data,PROJECT_ID=test-bigquery-cc,TABLE_1=order_central_master,TABLE_2=transaction_view_master
