# rakuten-update

Loads Rakuten RMS order CSV exports dropped into `rakuten_orders_raw` into `Rakuten.orders`.

| | |
|---|---|
| Status | active (`Rakuten.orders` last modified 2026-10-06, recreated 2025-04-11) |
| Folder | `rakuten/upload_rakuten/` |
| Runtime / entry point | python310 (deprecated runtime) / `upload_rakuten_bq` in `main.py` |
| Trigger | GCS finalize on `rakuten_orders_raw` (trigger location us, trigger SA `cris-cl@`), no retry |
| Reads | the uploaded object; the schema of the destination table |
| Writes | `Rakuten.orders` (WRITE_APPEND). The uploaded file is not moved or archived |

## How it works

1. `df_process.load_rak_orders_df`: reads the CSV as `cp932` (falls back to the default encoding), adds every
   Japanese header from `columns_names.columns_names_translation` that is missing in the file as an empty column,
   renames the columns to English with that map and keeps only the mapped columns, adds `UPDATED_AT_UTC` and
   `FILE_NAME`, then casts each column with `columns_names.dict_col` (default `str`) and blanks `nan` / `null` / `none`.
2. `bq_process.upload_rak_orders`: drops any column that is not in the destination table's schema and appends with an
   explicit schema derived from the dtypes.

File naming: no convention is enforced by the code.

## Dedup / reruns

None. Uploading an overlapping export (or the same file twice) appends duplicate order rows.

## Configuration

Env vars (values in `deploy.sh`): `PROJECT_ID`, `DATASET_ID` (`Rakuten`), `TABLE_ID` (`orders`). No secrets.
Column map and dtypes: `columns_names.py`. The live CPU (0.1666) is not the Gen 2 default for 512Mi, so `deploy.sh`
passes `--cpu 0.1666`. The local prototype notebook `orders_process.ipynb` is excluded by `.gcloudignore`.

## Deploy

```
make check rakuten-update
make deploy rakuten-update      # DRY_RUN=1 prints the command only
make describe rakuten-update
```

## Known issues / open questions

- `python310` is deprecated; a redeploy may be refused or warned.
- The repo was synced on 2026-11-18 with the deployed source (missing translated columns are added as empty, previously
  only in the console-edited version).
- `UPDATED_AT_UTC` is `datetime.now()` in server time, which is UTC on Cloud Functions.
- Related local script (legacy, not a function): `rakuten/rakuten.py` reads local CSVs and appends to
  `Rakuten.rakuten_orders`, a table that no longer exists.
