# FUJ_cloud_functions

The Google Cloud Functions (Gen 2) that load sales and payment data from Amazon, Shopify, Stripe and Rakuten into
BigQuery project `test-bigquery-cc`, plus the tooling to check and deploy them one at a time.

Not in this repo: the Komoju functions (`../komoju`, own deploy script), and the other functions in `test-bigquery-cc`
that have no code here. BigQuery SQL lives in `queries_bq/`, `datastudio/` and `report_queries_shopify/`; the BigQuery
and pipeline documentation lives in [`bigquery_documentation/`](bigquery_documentation/README.md).

## Prerequisites

- `gcloud` authenticated with an account that can read and deploy functions in `test-bigquery-cc`
  (`make check` also reads the deployed source zip from `gs://gcf-v2-sources-967608949943-us-central1`).
- GNU make (3.81 is enough) and `python3` (standard library only).

Every command passes `--project` and `--region` explicitly, so your active gcloud configuration does not matter.

## Commands

Run from the repo root. `<function>` is the GCP function name exactly as in GCP (first column of the index below).

| Command | What it does |
|---|---|
| `make list` | every function with a `deploy.sh`: name, region, folder |
| `make check <function>` | read-only: compares `deploy.sh` with the live config (runtime, trigger, service accounts, memory/CPU, timeout, scaling, retry, env vars, secrets, public access) and the local folder with the deployed source zip. Exit 0 = both match, 1 = config drift, 2 = source drift only |
| `make deploy <function>` | shows the folder and any uncommitted changes, asks `Deploy <function>? [y/N]`, then runs the folder's `deploy.sh`. `YES=1` skips the question, `DRY_RUN=1` only prints the gcloud command |
| `make describe <function>` | `gcloud functions describe` as YAML |

There is no "deploy all". Rules:

- Deploy only from this repo, never edit a function in the console: a deploy from here replaces whatever was edited there.
- Run `make check <function>` before deploying. SOURCE DRIFT means the live code differs from the folder: find out why
  before you overwrite it.
- The working tree is what gets uploaded (minus `.gcloudignore`), committed or not.

## Function index

| Function | Folder | Trigger | Writes to | Status | Runtime |
|---|---|---|---|---|---|
| [upload-amazon](amazon/upload_amazon/README.md) | `amazon/upload_amazon/` | GCS `amazon-raw` | `Amazon.order_central_master`, `Amazon.transaction_view_master` | active | python310 (deprecated) |
| [amazon_sg](amazon/amazon_sg/README.md) | `amazon/amazon_sg/` | GCS `amazon_sg` | `Amazon.oc_sg`, `Amazon.tv_sg` | legacy (still deployed) | python310 (deprecated) |
| [FUJ_shopify_orders](shopify_api/orders/README.md) | `shopify_api/orders/` | Pub/Sub `shopify_orders` (daily 10:00 Asia/Tokyo) | `Shopify.orders_master`, `Shopify.fulfillments` | active | python312 |
| [FUJ_shopify_payouts](shopify_api/payouts/README.md) | `shopify_api/payouts/` | Pub/Sub `shopify_payouts_weekly` (Mon 11:00 Asia/Tokyo) | `Shopify.shopify_payouts_api` | active | python312 |
| [stripe-requests-reps](cloud_stripe_report/create_report_stripe/README.md) | `cloud_stripe_report/create_report_stripe/` | Pub/Sub `weekly_stripe_report` (Mon 08:00 Asia/Tokyo) | Stripe report runs | active | python312 |
| [stripe-webhook](cloud_stripe_report/webhook/README.md) | `cloud_stripe_report/webhook/` | HTTP (Stripe webhook, public) | `gs://fujiorg-stripe-raw` | active | python310 (deprecated) |
| [bucket-stripe-bq](cloud_stripe_report/bucket-stripe-bq/README.md) | `cloud_stripe_report/bucket-stripe-bq/` | GCS `fujiorg-stripe-raw` | `Shopify.stripe_api`, `Shopify.stripe_payouts`, `Shopify.amazon_pay` | active | python310 (deprecated) |
| [rakuten-update](rakuten/upload_rakuten/README.md) | `rakuten/upload_rakuten/` | GCS `rakuten_orders_raw` | `Rakuten.orders` | active | python310 (deprecated) |
| [payment-rakuten](rakuten/payment_rakuten/README.md) | `rakuten/payment_rakuten/` | GCS `rakuten_payment_raw` | `Rakuten.payment_main`, `Rakuten.payment_coupon`, `Rakuten.statement`, `free.rakuten_cost_full` | active | python314 |

All functions: project `test-bigquery-cc`, region `us-central1`, Gen 2.

`python310` is deprecated by Google Cloud: a redeploy of the functions still on it may be refused or warned; they are being
upgraded to python312 one at a time. Upgrading is a
separate change per function (their pins `pandas==1.5.1`, `numpy==1.23.4`, `pyarrow==9.0.0` have no wheels for
Python 3.12+).

## Repo layout

```
Makefile, scripts/          make list/check/deploy/describe (scripts/fn.sh, scripts/fn_check.py)
amazon/                     upload_amazon/, amazon_sg/
shopify_api/                orders/, payouts/
cloud_stripe_report/        create_report_stripe/, webhook/, bucket-stripe-bq/
rakuten/                    upload_rakuten/, payment_rakuten/; rakuten.py (local legacy script, not a function)
old_functions/              retired / never-deployed function code (see its README)
bigquery_documentation/     BigQuery, pipeline and function-overview docs
queries_bq/, datastudio/, report_queries_shopify/, appscript/   SQL and Apps Script (out of scope for make)
```

Each function folder holds exactly one function: `main.py` (entry point), its helper modules, `requirements.txt`,
`.gcloudignore`, `deploy.sh` and `README.md`.

Retired code: [`old_functions/`](old_functions/README.md) (Shopify refunds, Stripe test functions, hello-world
webhooks, old Stripe payout requesters). Nothing there is deployed and `make` ignores it.

## Adding a new function

1. Create a folder (one or two levels below the root) with `main.py`, `requirements.txt`, `.gcloudignore`
   (copy one from an existing function; it must exclude `.env`, `*.ipynb`, `__pycache__/`, `README.md`, `deploy.sh`,
   `.gcloudignore` and any local-only script), `deploy.sh` and `README.md`.
2. In `deploy.sh`, copy an existing one: keep `FUNCTION=`, `REGION=` and `PROJECT=` each on its own line at column 0
   (that is how `make` finds the function), call `${GCLOUD:-gcloud} functions deploy "$FUNCTION" ...` and pass every
   setting explicitly.
3. Add a row to the function index above.
4. `make list`, `DRY_RUN=1 make deploy <function>`, then deploy and `make check <function>`.
