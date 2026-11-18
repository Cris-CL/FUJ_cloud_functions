# payment-rakuten

Cloud Function (Gen 2, python314, project `test-bigquery-cc`, region `us-central1`) that loads the
two 楽天ペイ CSV files into BigQuery. It is triggered by every file uploaded to bucket
`rakuten_payment_raw` and routes on the file name. A file that loads is then moved to the archive
(`gs://fujiorg-sales-data/Rakuten/`, see "Where files end up"), so **`rakuten_payment_raw` only holds
files that failed**.

| | |
|---|---|
| Status | active |
| Folder | `rakuten/payment_rakuten/` |
| Runtime / entry point | python314 / `update_payment` in `main.py` |
| Trigger | GCS finalize on `rakuten_payment_raw` (trigger location us, runtime and trigger SA `cris-cl@`), no retry |
| Reads | the uploaded object; `Rakuten.statement` (精算書No dedup); `FUJ_scheduled.rakuten_cost_report()` and `free.rakuten_cost_full` (cost rows) |
| Writes | `Rakuten.payment_main`, `Rakuten.payment_coupon`, `Rakuten.statement`, `free.rakuten_cost_full` (append); archive in `gs://fujiorg-sales-data/Rakuten/` |
| Config | env vars `DATASET_ID`, `TABLE_ID_1`, `TABLE_ID_2`, `TABLE_ID_3`, `COST_DATASET_ID`, `COST_TABLE_ID` (values in `deploy.sh`); no secrets |
| Deploy / check | `make check payment-rakuten`, `make deploy payment-rakuten` (see [Deploy](#deploy)) |

| File you download from 楽天ペイ | Name must | Lands in | Then |
|---|---|---|---|
| Payment CSV (deposit detail per order) | anything **not** ending in `_Shop_Billpay.csv`, by convention `FUJ_202400_Rakuten Deals - YYYYMMDD.csv` | `Rakuten.payment_main`, `Rakuten.payment_coupon` | sales chain, unchanged: scheduled query `FUJ_Rakuten_freee` (Saturday) -> `free.rakuten_report_full` |
| Billpay statement (精算書, fees) | end in `_Shop_Billpay.csv` (the name 楽天 gives it: `YYMMDD_<id>_<精算書No>_<店舗別ID>_<id>_Shop_Billpay.csv`) | `Rakuten.statement` | the function appends the freee cost rows of every deposit not yet written to `free.rakuten_cost_full` (built by `FUJ_scheduled.rakuten_cost_report()`) |

## Upload procedure (each deposit)

1. Download both files for the deposit from 楽天ペイ. Do not rename the Billpay file; do not rename a
   payment file to end in `_Shop_Billpay.csv`.
2. Upload both to bucket `rakuten_payment_raw` (console or `gsutil cp <file> gs://rakuten_payment_raw/`).
   Order does not matter: the two files go to different tables. Upload the RMS order export of the
   period as well (other function, see `bigquery_documentation/rakuten.md`).
3. Check the function log in Cloud Logging (function `payment-rakuten`). A statement load ends with
   `statement <精算書No>: <n> lines uploaded with report_id <YYYYMMDD>` followed by
   `<n> cost rows appended to free.rakuten_cost_full`; a payment file ends with `End upload process`.
   Both then log `archived gs://rakuten_payment_raw/<name> -> gs://fujiorg-sales-data/Rakuten/...`.
   A failed load writes nothing and the file stays in `rakuten_payment_raw`; the message says why
   (see "Errors").
4. The cost rows are in `free.rakuten_cost_full` right away (a statement with no 請求 line appends 0
   rows). The sales rows follow the Saturday run of `FUJ_Rakuten_freee` as before.
5. `SELECT * FROM FUJ_test.check_rakuten_cost()` must show `OK` for the deposit
   (`diff_tax_excluded` = 0 and `diff_tax` = 0: the whole-yen cost rows add up to the statement's 請求
   total and 消費税 line exactly). `MISSING NOMENCLATURE` in
   `free.rakuten_cost_full.account` means a new 品目: add it (next section), then rebuild that deposit's
   cost rows (see "Reloading").

## Where files end up

| File | Archived as (bucket `fujiorg-sales-data`) |
|---|---|
| Payment CSV `FUJ_YYYY00_Rakuten Deals - YYYYMMDD.csv` | `Rakuten/<YYYY>/deals/Rakuten Deals - YYYYMMDD.csv` (the `FUJ_YYYY00_` prefix is dropped; the year is the one of the date at the end of the name, not of the prefix) |
| Billpay statement `..._Shop_Billpay.csv` | `Rakuten/<YYYY>/billpay/Billpay_<report_id>.csv` (report_id = deposit date, below) |

- The file is moved (copied, then deleted from `rakuten_payment_raw`) only after everything succeeded:
  for a statement that includes the cost-row append.
- An archive object is never overwritten: if the destination already exists the file stays in
  `rakuten_payment_raw` and the log says so.
- A payment file whose name has no ` - YYYYMMDD.csv` date is loaded but cannot be archived: it stays
  in `rakuten_payment_raw` with a log line. Do not upload it again (payment files are not
  deduplicated); rename it in the archive by hand.
- Anything in `rakuten_payment_raw` therefore needs attention: read its log line in Cloud Logging.
- Files loaded before the archive existed were moved once by `backfill_archive.py` (dry run by
  default, `--apply` to move; it only moves files whose data is in BigQuery). The 楽天 file
  `FUJ_202400_Rakuten Deals - 20241005.csv` was misnamed (Deals settle on the 10th and 25th; its rows
  are `report_id` 20241010) and is archived as `Rakuten/2024/deals/Rakuten Deals - 20241010.csv`.

### report_id of a statement

`report_id` = 精算対象期間終了日 of the line whose 品目 starts with `楽天ﾍﾟｲ_決済金`, as `YYYYMMDD`.
It is the deposit date and matches `Rakuten.payment_main.report_id` of the payment file.

### Errors (nothing is written)

- header does not match the 17 expected columns: not a Billpay statement, or 楽天 changed the layout.
- no / more than one `楽天ﾍﾟｲ_決済金*` line, or that line has no 精算対象期間終了日: report_id cannot be
  derived.
- statement `<精算書No>` is already loaded: the same file was uploaded before (see "Reloading").

If the statement landed but the append step failed (BigQuery error in the log), the deposit is picked
up by the next statement upload, or run `queries_bq/rakuten_cost/05_materialise_cost_rows.sql` by hand.
The file stays in `rakuten_payment_raw`, but uploading it again fails with "already loaded": after
the cost rows are in, move it to the archive by hand
(`gcloud storage mv gs://rakuten_payment_raw/<name> gs://fujiorg-sales-data/Rakuten/<YYYY>/billpay/Billpay_<report_id>.csv`).

## Adding a 品目 to the nomenclature

`Rakuten.cost_nomenclature` maps the statement 品目 (`subject_name`, exact match, half-width katakana
and bracket widths as written in the file) to 勘定科目, 税区分, 備考, 品目. When a cost row shows
`MISSING NOMENCLATURE {subject_name:...}`:

```sql
INSERT INTO `Rakuten.cost_nomenclature`
  (subject_name, remarks, item, tax_distinction, account, translation, ADDED_DATETIME)
VALUES ('<品目 exactly as in the statement>', '<備考>', '<品目 freee>', '<税区分>', '<勘定科目>', '<english>', CURRENT_DATETIME());
```

The seed (37 rows) is `queries_bq/rakuten_cost/02_rakuten_cost_nomenclature.sql`; keep it in sync.

## Reloading a statement or a deposit

- To load a corrected statement file: delete its lines first
  (`DELETE FROM Rakuten.statement WHERE statement_number = '<精算書No>'`), delete the deposit's cost
  rows (next point), then upload the file again.
- To rebuild the cost rows of a deposit (after a nomenclature fix):
  `DELETE FROM free.rakuten_cost_full WHERE control_number = '<YYYYMMDD>'`, then run
  `queries_bq/rakuten_cost/05_materialise_cost_rows.sql` (or upload the next statement, which runs
  the same query).

## Deploy

```bash
make check payment-rakuten      # config + source vs live, read-only
make deploy payment-rakuten     # DRY_RUN=1 prints the command only
make describe payment-rakuten
```

`deploy.sh` holds the whole configuration (trigger bucket, service account, memory, timeout, env vars
`DATASET_ID`, `TABLE_ID_1`, `TABLE_ID_2`, `TABLE_ID_3`, `COST_DATASET_ID`, `COST_TABLE_ID`).
`requirements.txt` pins the versions the function runs with; change them only together with a test
deploy. `.gcloudignore` keeps this README, the deploy script and the local `backfill_archive.py` out of the
uploaded source. Until the next deploy, `make check` reports SOURCE DRIFT on `main.py`: the repo only adds a
docstring paragraph about the archive (no code change).

## BigQuery objects (creation scripts in `queries_bq/rakuten_cost/`)

| Object | Script | Role |
|---|---|---|
| `Rakuten.statement` | 01 | landing table, one row per statement line (+ `report_id`, `FILE_NAME`) |
| `Rakuten.cost_nomenclature` | 02 | 品目 -> freee account mapping (seed) |
| `FUJ_scheduled.rakuten_cost_report()` | 03 | builds the freee cost rows (sheet `RAK_PAYMENT` logic) |
| `free.rakuten_cost_full` | 04 | materialised cost rows, one deposit once |
| append query | 05 | the INSERT the function runs after each statement load; for manual re-runs |
| `FUJ_test.check_rakuten_cost()` | 06 | reconciliation per deposit |

## Local test

The parser can be run on local files with the same code:

```python
import os, sys; sys.path.insert(0, "rakuten/payment_rakuten")
import main
df = main.process_statement_df(main.read_statement_csv(path, name), name)   # raises StatementError on a bad file
main.archive_object_name(name, report_id)   # where the file would be archived
```

Runtime python314 with pandas 3: write to BigQuery with `pandas_gbq.to_gbq(df, ...)` (pandas 3 has no
`DataFrame.to_gbq`), and payment CSVs are read with `dtype=str` so long numbers stay strings.
