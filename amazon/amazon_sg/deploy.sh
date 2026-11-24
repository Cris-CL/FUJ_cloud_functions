#!/usr/bin/env bash
# Deploys the amazon_sg Cloud Function (Gen 2) from this folder.
#
# Usage:  make deploy amazon_sg      (or: bash amazon/amazon_sg/deploy.sh)
# Check:  make check amazon_sg
#
# The values below reproduce the live configuration (project test-bigquery-cc, us-central1,
# python312, GCS finalize trigger on bucket amazon_sg, no retry). The live CPU (167m) is not the
# Gen 2 default for 512Mi, so --cpu is passed explicitly.
set -euo pipefail
FUNCTION=amazon_sg
REGION=us-central1
PROJECT=test-bigquery-cc
cd "$(dirname "$0")"

${GCLOUD:-gcloud} functions deploy "$FUNCTION" \
  --project "$PROJECT" \
  --gen2 \
  --region "$REGION" \
  --runtime python312 \
  --source . \
  --entry-point amazon_sg_process \
  --trigger-bucket amazon_sg \
  --trigger-location us-central1 \
  --service-account 967608949943-compute@developer.gserviceaccount.com \
  --trigger-service-account 967608949943-compute@developer.gserviceaccount.com \
  --memory 512Mi \
  --cpu 0.167 \
  --timeout 60s \
  --max-instances 4 \
  --concurrency 1 \
  --set-env-vars DATASET_ID=Amazon,NEW_BUCKET=fujiorg-sales-data,PROJECT_ID=test-bigquery-cc,TABLE_1=oc_sg,TABLE_2=tv_sg
