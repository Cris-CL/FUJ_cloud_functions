#!/usr/bin/env bash
# Deploys the payment-rakuten Cloud Function (Gen 2) from this folder.
#
# Usage:  bash rakuten/payment_rakuten/deploy.sh
# Check:  gcloud functions describe payment-rakuten --region us-central1 --format=yaml
#
# The values below reproduce the live configuration (project test-bigquery-cc,
# us-central1, python310, GCS finalize trigger on bucket rakuten_payment_raw).
set -euo pipefail
cd "$(dirname "$0")"

gcloud functions deploy payment-rakuten \
  --project test-bigquery-cc \
  --gen2 \
  --region us-central1 \
  --runtime python310 \
  --source . \
  --entry-point update_payment \
  --trigger-bucket rakuten_payment_raw \
  --trigger-location us \
  --service-account cris-cl@test-bigquery-cc.iam.gserviceaccount.com \
  --trigger-service-account cris-cl@test-bigquery-cc.iam.gserviceaccount.com \
  --memory 512Mi \
  --timeout 60s \
  --max-instances 1000 \
  --set-env-vars DATASET_ID=Rakuten,TABLE_ID_1=payment_main,TABLE_ID_2=payment_coupon
