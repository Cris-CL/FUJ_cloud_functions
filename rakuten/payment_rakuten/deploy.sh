#!/usr/bin/env bash
# Deploys the payment-rakuten Cloud Function (Gen 2) from this folder.
#
# Usage:  make deploy payment-rakuten      (or: bash rakuten/payment_rakuten/deploy.sh)
# Check:  make check payment-rakuten
#
# The values below reproduce the live configuration (project test-bigquery-cc,
# us-central1, python314, GCS finalize trigger on bucket rakuten_payment_raw).
set -euo pipefail
FUNCTION=payment-rakuten
REGION=us-central1
PROJECT=test-bigquery-cc
cd "$(dirname "$0")"

${GCLOUD:-gcloud} functions deploy "$FUNCTION" \
  --project "$PROJECT" \
  --gen2 \
  --region "$REGION" \
  --runtime python314 \
  --source . \
  --entry-point update_payment \
  --trigger-bucket rakuten_payment_raw \
  --trigger-location us \
  --service-account cris-cl@test-bigquery-cc.iam.gserviceaccount.com \
  --trigger-service-account cris-cl@test-bigquery-cc.iam.gserviceaccount.com \
  --memory 512Mi \
  --timeout 60s \
  --max-instances 1000 \
  --set-env-vars DATASET_ID=Rakuten,TABLE_ID_1=payment_main,TABLE_ID_2=payment_coupon,TABLE_ID_3=statement,COST_DATASET_ID=free,COST_TABLE_ID=rakuten_cost_full
