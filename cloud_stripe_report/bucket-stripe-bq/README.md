# bucket-stripe-bq

Loads Stripe report CSVs (balance change and payout reconciliation) and Amazon Pay settlement text files dropped into
`fujiorg-stripe-raw` into BigQuery. Last step of the Stripe chain
[stripe-requests-reps](../create_report_stripe/README.md) -> Stripe -> [stripe-webhook](../webhook/README.md) -> this function.

| | |
|---|---|
| Status | active (`Shopify.stripe_api` / `Shopify.stripe_payouts` last modified 2026-10-11; `Shopify.amazon_pay` last modified 2025-01-10) |
| Folder | `cloud_stripe_report/bucket-stripe-bq/` |
| Runtime / entry point | python312 / `upload_stripe_bq` in `main.py` |
| Trigger | GCS finalize on `fujiorg-stripe-raw` (trigger location us, trigger SA `cris-cl@`), no retry |
| Reads | the uploaded object |
| Writes | `Shopify.stripe_api`, `Shopify.stripe_payouts`, `Shopify.amazon_pay` (all WRITE_APPEND); Amazon Pay files also to `gs://fujiorg-sales-data/Shopify/amazon_pay/` |

## How it works

Routing is by the first three characters of the object name:

- `BAL...` (balance change report, written by stripe-webhook) -> `TABLE_ID` (`Shopify.stripe_api`), BigQuery load from
  the GCS URI with an explicit schema, header row skipped.
- `PAY...` (payout reconciliation report, written by stripe-webhook) -> `TABLE_ID_PAY` (`Shopify.stripe_payouts`), same way.
- `AMA...` (Amazon Pay settlement `.txt`, uploaded by hand) -> `amazon_pay_txt_process.clean_txt` parses it (settlement end
  date, header row, numeric columns, adds `settlement_end_date`, `FILE_NAME`, `UPDATED_AT`), appends to `TABLE_ID_AMA`
  (`Shopify.amazon_pay`), writes the cleaned CSV to `gs://fujiorg-sales-data/Shopify/amazon_pay/<year>/<name>.csv`
  (year = characters 5-8 of the name, i.e. names look like `AMA_YYYY...`) and moves the raw file to
  `gs://fujiorg-sales-data/Shopify/amazon_pay/RAW/<name>`.
- Any other prefix: nothing happens.

`BAL` / `PAY` files stay in `fujiorg-stripe-raw` after loading (the `file_classifier` map in `main.py` is unused).

## Dedup / reruns

None in the loader. A re-delivered or re-uploaded file appends duplicate rows. The Stripe request is incremental
(stripe-requests-reps starts from `MAX(created_utc)`), and downstream `stripe_rep` excludes settlements already in
`free.stripe_freee_full`.

## Configuration

Env vars (values in `deploy.sh`): `PROJECT_ID`, `DATASET_ID` (`Shopify`), `TABLE_ID` (balance), `TABLE_ID_PAY` (payouts),
`TABLE_ID_AMA` (Amazon Pay). No secrets. Live resources differ from the defaults: `--cpu 2 --concurrency 2 --max-instances 5`.

## Deploy

```
make check bucket-stripe-bq
make deploy bucket-stripe-bq      # DRY_RUN=1 prints the command only
make describe bucket-stripe-bq
```

## Known issues / open questions

- Upgraded from python310 (deprecated) to python312 on 2026-11-18 (pandas 1.5.1 -> 2.2.2, numpy 1.26.4, pyarrow 15.0.2).
  pandas 2 no longer converts the offset dates of Amazon Pay files (`2024-12-11T00:10:52 +0900`) to naive UTC inside
  `astype`, so `clean_txt` now does it explicitly. Checked on all 127 raw files in
  `gs://fujiorg-sales-data/Shopify/amazon_pay/RAW/`: the BigQuery upload data is identical to the old stack's.
- Is Amazon Pay still used? `Shopify.amazon_pay` has not changed since 2025-01-10.
