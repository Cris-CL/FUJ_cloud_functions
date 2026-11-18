#!/usr/bin/env bash
# Deploys the bucket-stripe-bq Cloud Function (Gen 2) from this folder.
#
# Usage:  make deploy bucket-stripe-bq      (or: bash cloud_stripe_report/bucket-stripe-bq/deploy.sh)
# Check:  make check bucket-stripe-bq
#
# The values below reproduce the live configuration (project test-bigquery-cc, us-central1,
# python310, GCS finalize trigger on bucket fujiorg-stripe-raw in location us, no retry).
set -euo pipefail
FUNCTION=bucket-stripe-bq
REGION=us-central1
PROJECT=test-bigquery-cc
cd "$(dirname "$0")"

${GCLOUD:-gcloud} functions deploy "$FUNCTION" \
  --project "$PROJECT" \
  --gen2 \
  --region "$REGION" \
  --runtime python310 \
  --source . \
  --entry-point upload_stripe_bq \
  --trigger-bucket fujiorg-stripe-raw \
  --trigger-location us \
  --service-account 967608949943-compute@developer.gserviceaccount.com \
  --trigger-service-account cris-cl@test-bigquery-cc.iam.gserviceaccount.com \
  --memory 512Mi \
  --cpu 2 \
  --timeout 60s \
  --max-instances 5 \
  --concurrency 2 \
  --set-env-vars DATASET_ID=Shopify,PROJECT_ID=test-bigquery-cc,TABLE_ID=stripe_api,TABLE_ID_AMA=amazon_pay,TABLE_ID_PAY=stripe_payouts
