#!/usr/bin/env bash
# Deploys the FUJ_shopify_payouts Cloud Function (Gen 2) from this folder.
#
# Usage:  make deploy FUJ_shopify_payouts      (or: bash shopify_api/payouts/deploy.sh)
# Check:  make check FUJ_shopify_payouts
#
# The values below reproduce the live configuration (project test-bigquery-cc, us-central1,
# python312, Pub/Sub trigger on topic shopify_payouts_weekly, no retry). It was first deployed
# from the console; SHOPIFY_KEY / SHOPIFY_PASS come from Secret Manager (pinned to version 1).
set -euo pipefail
FUNCTION=FUJ_shopify_payouts
REGION=us-central1
PROJECT=test-bigquery-cc
cd "$(dirname "$0")"

${GCLOUD:-gcloud} functions deploy "$FUNCTION" \
  --project "$PROJECT" \
  --gen2 \
  --region "$REGION" \
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
