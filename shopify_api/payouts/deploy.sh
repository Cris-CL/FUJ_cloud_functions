#!/usr/bin/env bash
# Deploys the FUJ_shopify_payouts Cloud Function (Gen 2) from this folder.
#
# Usage:  bash shopify_api/payouts/deploy.sh
# Check:  gcloud functions describe FUJ_shopify_payouts --region us-central1 --format=yaml
#
# The values below reproduce the live configuration (project test-bigquery-cc, us-central1,
# python312, Pub/Sub trigger on topic shopify_payouts_weekly, no retry). It was first deployed
# from the console; SHOPIFY_KEY / SHOPIFY_PASS come from Secret Manager (pinned to version 1).
set -euo pipefail
cd "$(dirname "$0")"

gcloud functions deploy FUJ_shopify_payouts \
  --project test-bigquery-cc \
  --gen2 \
  --region us-central1 \
  --runtime python312 \
  --source . \
  --entry-point update_shopify_payout \
  --trigger-topic shopify_payouts_weekly \
  --no-retry \
  --service-account cris-cl@test-bigquery-cc.iam.gserviceaccount.com \
  --trigger-service-account 967608949943-compute@developer.gserviceaccount.com \
  --memory 1Gi \
  --cpu 1 \
  --timeout 60s \
  --max-instances 100 \
  --concurrency 1 \
  --set-env-vars API_VERSION=2024-04,BUCKET=fujiorg-shopify-payouts-raw,PROJECT_NAME=test-bigquery-cc,STORE_NAME=fuji-organics,TABLE_NAME_PAY=shopify_payouts_api \
  --set-secrets SHOPIFY_KEY=SH_PAY_API_KEY:1,SHOPIFY_PASS=SH_PAY_PASS:1
