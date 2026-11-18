# upload-amazon

Loads Amazon Japan Seller Central reports (Order Central and settlement / Transaction View) dropped into the
`amazon-raw` bucket into BigQuery, then archives the file in `fujiorg-sales-data`.

| | |
|---|---|
| Status | active (`Amazon.order_central_master` / `Amazon.transaction_view_master` last modified 2026-10-13) |
| Folder | `amazon/upload_amazon/` |
| Runtime / entry point | python312 / `amazon_process` in `main.py` |
| Trigger | GCS finalize on `amazon-raw` (trigger location us-central1), no retry |
| Reads | the uploaded object; `SELECT DISTINCT FILE_NAME` from the destination table |
| Writes | `Amazon.order_central_master` or `Amazon.transaction_view_master` (WRITE_APPEND); archive in `gs://fujiorg-sales-data/Amazon/` |

## How it works

1. Reads the uploaded file with pandas. Report type is detected by encoding, not by name: if the file reads as UTF-8
   it is a **tv** (transaction view / settlement) report, otherwise it is re-read as `ms932` and treated as **oc**
   (order central). An order report that happens to be valid UTF-8 would be misclassified and then fail on the tv dtypes.
2. Looks up the file name in the destination table (`FILE_NAME` column).
3. New file: lower-cases / normalises the column names, keeps only the columns listed in `dict_utils_ama.py`
   (`data_types[report_type]`), casts them, blanks `nan` / `null` / empty strings, and adds `FILE_NAME` and
   `UPLOADED_DATETIME`.
4. Moves the object (copy + delete) to `fujiorg-sales-data`:
   - tv: `Amazon/transaction_view/settlement_<year>/TV_<name>`
   - oc: `Amazon/order_central/sales_<year>/OC_<name>`
   - name already loaded: `Amazon/repeated_files/<TV|OC>_<name>`
5. Appends the dataframe with a load job and an explicit schema derived from the dtypes.

File naming: order reports `YYYYMM_W<n>.txt` (e.g. `202605_W3.txt`), settlements `YYYYMMDD.txt` (e.g. `20240627.txt`).
The archive year folder is `name[:4]`, so every file name must start with the 4-digit year.

## Dedup / reruns

- By exact file name only. A name already in the table is not loaded and goes to `repeated_files/` (no replace logic,
  `## TODO` in that branch). A renamed re-export is loaded again.
- If the `FILE_NAME` lookup query fails, the code proceeds with an empty list and loads the file.
- Downstream, `FUJ_extra.AMA_SUB()` only processes settlements whose `settlement_id` is not yet in `free.amazon_report_full`.

## Configuration

Env vars (values in `deploy.sh`): `PROJECT_ID`, `DATASET_ID` (`Amazon`), `TABLE_1` (order central table),
`TABLE_2` (transaction view table), `NEW_BUCKET` (archive bucket). No secrets.
`dict_utils_ama.py` holds the column -> dtype map per report type. The git-ignored `amazon/.env` is a local test
config (`DATASET_ID=z_test_tables`), not used by the deployed function.

## Deploy

```
make check upload-amazon      # config + source vs live, read-only
make deploy upload-amazon     # DRY_RUN=1 prints the command only
make describe upload-amazon
```

## Known issues / open questions

- Upgraded from python310 (deprecated) to python312 on 2026-11-18 (pandas 1.5.1 -> 2.2.2, numpy 1.26.4, pyarrow 15.0.2).
  Three pandas 2 changes needed code changes: the bare `datetime64` dtypes in `dict_utils_ama.py` now say
  `datetime64[ns]`; the offset dates (`+09:00`) are converted to naive UTC explicitly before `astype` (pandas 1 did it
  implicitly); `UPLOADED_DATETIME` from `datetime.now()` is `datetime64[us]` and is mapped to DATETIME. Checked on all
  323 archived 2025-2026 reports (2,408,130 rows): the BigQuery upload data is identical to the old stack's except 34
  rows whose empty `ship_state` pandas 1.5 stored as the text `"None"` and pandas 2 stores as NULL (the table already has
  ~800 NULL and 7 `"None"` values; no query reads `ship_state`).
- The file is moved to the archive **before** the BigQuery load; if the load fails, the file is archived but not loaded
  and has to be re-uploaded by hand.
- The repo was synced on 2026-11-18 with the deployed source (column filter on `data_types` keys, previously only in
  the console-edited version).
- The git-ignored local notebook `amazon/amazon_update.ipynb` may still `import amazon_process` (old module name).
