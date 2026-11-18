#!/usr/bin/env bash
# Deploys the stripe-requests-reps Cloud Function (Gen 2) from this folder.
#
# Usage:  make deploy stripe-requests-reps      (or: bash cloud_stripe_report/create_report_stripe/deploy.sh)
# Check:  make check stripe-requests-reps
#
# The values below reproduce the live configuration (project test-bigquery-cc, us-central1,
# python310, Pub/Sub trigger on topic weekly_stripe_report, no retry). The Stripe key comes from
# Secret Manager (secret "stripe", version 1).
set -euo pipefail
FUNCTION=stripe-requests-reps
REGION=us-central1
PROJECT=test-bigquery-cc
cd "$(dirname "$0")"

${GCLOUD:-gcloud} functions deploy "$FUNCTION" \
  --project "$PROJECT" \
  --gen2 \
  --region "$REGION" \
  --runtime python310 \
  --source . \
  --entry-point request_reports \
  --trigger-topic weekly_stripe_report \
  --no-retry \
  --service-account 967608949943-compute@developer.gserviceaccount.com \
  --trigger-service-account 967608949943-compute@developer.gserviceaccount.com \
  --memory 256Mi \
  --timeout 60s \
  --max-instances 2 \
  --concurrency 1 \
  --set-env-vars TABLE_QUERY=test-bigquery-cc.Shopify.stripe_api,TABLE_QUERY_PAY=test-bigquery-cc.Shopify.stripe_payouts \
  --set-secrets stripe_key=stripe:1
