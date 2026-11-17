#!/usr/bin/env bash
# Deploys the FUJ_shopify_orders Cloud Function (Gen 2) from this folder.
#
# Usage:  bash shopify_api/orders/deploy.sh
# Check:  gcloud functions describe FUJ_shopify_orders --region us-central1 --format=yaml
#
# The values below reproduce the live configuration (project test-bigquery-cc, us-central1,
# python312, Pub/Sub trigger on topic shopify_orders, no retry). It was first deployed from
# the console; SHOPIFY_KEY / SHOPIFY_PASS come from Secret Manager.
set -euo pipefail
cd "$(dirname "$0")"

gcloud functions deploy FUJ_shopify_orders \
  --project test-bigquery-cc \
  --gen2 \
  --region us-central1 \
  --runtime python312 \
  --source . \
  --entry-point main \
  --trigger-topic shopify_orders \
  --no-retry \
  --service-account cris-cl@test-bigquery-cc.iam.gserviceaccount.com \
  --trigger-service-account 967608949943-compute@developer.gserviceaccount.com \
  --memory 512Mi \
  --cpu 1 \
  --timeout 300s \
  --max-instances 100 \
  --concurrency 1 \
  --set-env-vars API_VERSION=2024-04,BUCKET=fujiorg-shopify-orders-raw,FULFILLMENT_TABLE=Shopify.fulfillments,PROJECT_NAME=test-bigquery-cc,STORE_NAME=fuji-organics,TABLE_NAME=Shopify.orders_master \
  --set-secrets SHOPIFY_KEY=api_key_shop:latest,SHOPIFY_PASS=shopy_password:latest
