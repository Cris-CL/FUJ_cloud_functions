#!/usr/bin/env bash
# Deploys the stripe-webhook Cloud Function (Gen 2) from this folder.
#
# Usage:  make deploy stripe-webhook      (or: bash cloud_stripe_report/webhook/deploy.sh)
# Check:  make check stripe-webhook
#
# The values below reproduce the live configuration (project test-bigquery-cc, us-central1,
# python310, HTTP trigger open to allUsers; the code verifies the Stripe signature). Its URL
# https://us-central1-test-bigquery-cc.cloudfunctions.net/stripe-webhook is registered in Stripe:
# do not rename the function. Stripe key and webhook secret come from Secret Manager (version 1).
set -euo pipefail
FUNCTION=stripe-webhook
REGION=us-central1
PROJECT=test-bigquery-cc
cd "$(dirname "$0")"

${GCLOUD:-gcloud} functions deploy "$FUNCTION" \
  --project "$PROJECT" \
  --gen2 \
  --region "$REGION" \
  --runtime python310 \
  --source . \
  --entry-point webhook \
  --trigger-http \
  --allow-unauthenticated \
  --service-account 967608949943-compute@developer.gserviceaccount.com \
  --memory 256Mi \
  --timeout 60s \
  --max-instances 100 \
  --concurrency 1 \
  --set-env-vars STRIPE_BUCKET=fujiorg-stripe-raw,STRIPE_PAYOUT_BUCKET=stripe-payouts,TABLE_QUERY=test-bigquery-cc.Shopify.stripe_payouts \
  --set-secrets stripe_key=stripe:1,web_secret=web_hook_stripe:1
