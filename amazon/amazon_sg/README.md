# amazon_sg

Legacy loader for Amazon Singapore reports (Order Central and Transaction View) dropped into the `amazon_sg`
bucket. Same idea as [upload-amazon](../upload_amazon/README.md), with SG-specific parsing and tables.

| | |
|---|---|
| Status | legacy, still deployed. Amazon Singapore is inactive; `Amazon.tv_sg` / `Amazon.oc_sg` last modified 2025-05-08 |
| Folder | `amazon/amazon_sg/` |
| Runtime / entry point | python310 (deprecated runtime) / `amazon_sg_process` in `main.py` |
| Trigger | GCS finalize on `amazon_sg` (trigger location us-central1), no retry |
| Reads | the uploaded object; `SELECT DISTINCT FILE_NAME` from the destination table |
| Writes | `Amazon.oc_sg` or `Amazon.tv_sg` (WRITE_APPEND); archive in `gs://fujiorg-sales-data/Amazon_SG/` |

## How it works

1. Report type comes from the file name: `tv` in the name -> Transaction View (header on row 8, or row 7 for the newer
   layout; a missing `promotion-id` column is added empty); `oc` in the name -> Order Central. Any other name is logged
   ("incorrect report") and the function returns without moving or loading the file.
2. Looks up the file name in the destination table (`FILE_NAME` column).
3. New file: normalises column names, casts with `config_asg.data_types`, parses the date / datetime columns with the
   formats in `config_asg.py`, blanks `nan` / `null` / empty strings, adds `FILE_NAME` and `UPLOADED_DATETIME`.
4. Moves the object (copy + delete) to `fujiorg-sales-data`:
   - tv: `Amazon_SG/transaction_view/settlement_2024/<name>`
   - oc: `Amazon_SG/order_central/sales_2024/<name>`
   - name already loaded: `Amazon_SG/repeated_files/<TV|OC>_<name>`
5. Appends to BigQuery with an explicit schema.

The archive year is hard-coded as `year = "2024"` in `config_asg.py`.

## Dedup / reruns

By exact file name only (same as upload-amazon). If the lookup query fails, the file is loaded anyway.

## Configuration

Env vars (values in `deploy.sh`): `PROJECT_ID`, `DATASET_ID` (`Amazon`), `TABLE_1` (`oc_sg`), `TABLE_2` (`tv_sg`),
`NEW_BUCKET` (archive bucket). No secrets. Parsing config (dtypes, date formats, columns) is in `config_asg.py`.

## Deploy

```
make check amazon_sg
make deploy amazon_sg         # DRY_RUN=1 prints the command only
make describe amazon_sg
```

The live CPU (167m) is not the Gen 2 default for 512Mi, so `deploy.sh` passes `--cpu 0.167`.

## Known issues / open questions

- `make check` reports one expected residual: the deployed zip also contains `old_main.py`, which nothing imports.
  It is not in the repo; the next deploy from here drops it.
- `python310` is deprecated; a redeploy may be refused or warned (see upload-amazon for the dependency note).
- Open question: Amazon SG is inactive. Should the function be deleted, or kept for possible reuse?
- The file is moved before the BigQuery load, so a failed load leaves the file archived but not loaded.
