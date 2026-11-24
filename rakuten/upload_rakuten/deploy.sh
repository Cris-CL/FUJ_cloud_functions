#!/usr/bin/env bash
# Deploys the rakuten-update Cloud Function (Gen 2) from this folder.
#
# Usage:  make deploy rakuten-update      (or: bash rakuten/upload_rakuten/deploy.sh)
# Check:  make check rakuten-update
#
# The values below reproduce the live configuration (project test-bigquery-cc, us-central1,
# python312, GCS finalize trigger on bucket rakuten_orders_raw in location us, no retry).
# The live CPU (0.1666) is not the Gen 2 default for 512Mi, so --cpu is passed explicitly.
# The local notebook orders_process.ipynb is excluded by .gcloudignore.
set -euo pipefail
FUNCTION=rakuten-update
REGION=us-central1
PROJECT=test-bigquery-cc
cd "$(dirname "$0")"

${GCLOUD:-gcloud} functions deploy "$FUNCTION" \
  --project "$PROJECT" \
  --gen2 \
  --region "$REGION" \
  --runtime python312 \
  --source . \
  --entry-point upload_rakuten_bq \
  --trigger-bucket rakuten_orders_raw \
  --trigger-location us \
  --service-account 967608949943-compute@developer.gserviceaccount.com \
  --trigger-service-account cris-cl@test-bigquery-cc.iam.gserviceaccount.com \
  --memory 512Mi \
  --cpu 0.1666 \
  --timeout 60s \
  --max-instances 4 \
  --concurrency 1 \
  --set-env-vars DATASET_ID=Rakuten,PROJECT_ID=test-bigquery-cc,TABLE_ID=orders
