# payment-rakuten

Cloud Function (Gen 2, python310, project `test-bigquery-cc`, region `us-central1`) that loads the
two 楽天ペイ CSV files into BigQuery. It is triggered by every file uploaded to bucket
`rakuten_payment_raw` and routes on the file name.

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
   `<n> cost rows appended to free.rakuten_cost_full`. A failed load writes nothing; the message says
   why (see "Errors").
4. The cost rows are in `free.rakuten_cost_full` right away (a statement with no 請求 line appends 0
   rows). The sales rows follow the Saturday run of `FUJ_Rakuten_freee` as before.
5. `SELECT * FROM FUJ_test.check_rakuten_cost()` must show `OK` for the deposit
   (`diff_tax_excluded` = 0; `diff_tax` below 1 yen is per-line rounding). `MISSING NOMENCLATURE` in
   `free.rakuten_cost_full.account` means a new 品目: add it (next section), then rebuild that deposit's
   cost rows (see "Reloading").

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
bash rakuten/payment_rakuten/deploy.sh
gcloud functions describe payment-rakuten --region us-central1 --format=yaml   # check
```

`deploy.sh` holds the whole configuration (trigger bucket, service account, memory, timeout, env vars
`DATASET_ID`, `TABLE_ID_1`, `TABLE_ID_2`, `TABLE_ID_3`, `COST_DATASET_ID`, `COST_TABLE_ID`).
`requirements.txt` pins the versions the function runs with; change them only together with a test
deploy. `.gcloudignore` keeps this README and the deploy script out of the uploaded source.

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
```
