# BigQuery lineage: Fuji Organics sales -> freee

Generated 2026-10-16 with generate_lineage.py from the live BigQuery definitions.

Project `test-bigquery-cc`, location US. Scope: datasets `Amazon`, `FUJ_extra`, `FUJ_scheduled`, `FUJ_sheets`, `Rakuten`, `Shopify`, `free`. Schedules are UTC. Dependencies are extracted by regex from FROM/JOIN targets and dataset-qualified routine calls.

## 1. Pipeline map per store

- **Amazon JP**
  - Landing tables: `Amazon.nomenclature`, `Amazon.order_central_master`, `Amazon.transaction_view_master`
    - FUJ_extra helpers: `FUJ_extra.AMA_SUB`, `FUJ_extra.Balance`, `FUJ_extra.DateTypeAma`, `FUJ_extra.MainDateAma`
      - Report function: `FUJ_scheduled.amazon_report()`
        - Scheduled query `FUJ_Amazon_freee`: every day 00:12 UTC -> `free.amazon_report_full` (WRITE_APPEND)
          - `free.amazon_report_full`
            - `FUJ_sheets.amazon_long()` (also reads `FUJ_extra.JanTable` (<- `free.sku_master`))
              - `FUJ_sheets.amazon_short()`
                - Google Sheet FUJ_<YYYY>_AMAZON: AMA_L / AMA_S -> REP_L / REP_S -> monthly tab -> freee CSV import
- **Rakuten**
  - Landing tables: `Rakuten.orders`, `Rakuten.payment_main`
    - FUJ_extra helpers: `FUJ_extra.Rakuten_full`, `FUJ_extra.zipCodeRak`
      - Report function: `FUJ_scheduled.rakuten_report()`
        - Scheduled query `FUJ_Rakuten_freee`: every sat 00:00 UTC -> `free.rakuten_report_full` (WRITE_APPEND)
          - `free.rakuten_report_full`
            - `FUJ_sheets.rakuten_long()` (also reads `FUJ_extra.JanTable` (<- `free.sku_master`))
              - `FUJ_sheets.rakuten_short()`
                - Google Sheet FUJ_<YYYY>_RAKUTEN: RAK_L / RAK_S -> REP_L / REP_S -> monthly tab (+ fee rows from free.rakuten_cost_full, added by hand; before 2026-11 from sheet RAK_PAYMENT) -> freee CSV import
- **Shopify (Shopify Payments, Stripe, Komoju, Amazon Pay)**
  - Flow `FUJ_scheduled.shopify_report()`
    - Landing tables: `Shopify.orders_master`, `Shopify.shopify_payouts_api`
      - FUJ_extra helpers: -
        - Report function: `FUJ_scheduled.shopify_report()`
          - Scheduled query `FUJ_Shopify`: every mon 02:05 UTC -> `free.shopify_freee_full` (WRITE_APPEND)
            - `free.shopify_freee_full`
  - Flow `FUJ_scheduled.stripe_rep()`
    - Landing tables: `Shopify.orders_master`, `Shopify.stripe_api`, `Shopify.stripe_payouts`
      - FUJ_extra helpers: -
        - Report function: `FUJ_scheduled.stripe_rep()`
          - Scheduled query `FUJ_Shopify_Stripe`: every day 23:35 UTC -> `free.stripe_freee_full` (WRITE_APPEND)
            - `free.stripe_freee_full`
  - Flow `FUJ_scheduled.komoju_rep()`
    - Landing tables: `Shopify.komoju_payments`, `Shopify.orders_master`
      - FUJ_extra helpers: -
        - Report function: `FUJ_scheduled.komoju_rep()`
          - No scheduled query: run by hand, INSERT (append) into `free.sh_komoju`
            - `free.sh_komoju`: manual run of FUJ_scheduled.komoju_rep(), not scheduled yet
  - Flow `FUJ_scheduled.SA_report()`
    - Landing tables: `Shopify.amazon_pay`, `Shopify.orders_master`
      - FUJ_extra helpers: -
        - Report function: `FUJ_scheduled.SA_report()`
          - Scheduled query `FUJ_Shopify_Amazon_freee`: every sun,wed,fri 23:15 UTC -> `free.sh_ama_freee_full` (WRITE_APPEND)
            - `free.sh_ama_freee_full`
  - All flows above land in `free.sh_ama_freee_full`, `free.sh_komoju`, `free.shopify_freee_full`, `free.stripe_freee_full`, then:
    - `FUJ_extra.ShopifyCombinedReport()`
      - `FUJ_sheets.shopify_long()` (also reads `FUJ_extra.JanTable` (<- `free.sku_master`))
        - `FUJ_sheets.shopify_short()`
          - Google Sheet FUJ_<YYYY>_SHOPIFY: SH_L / SH_S (+ tab totals from FUJ_sheets.Shopify_totals()) -> REP_L / REP_S -> monthly tab -> freee CSV import

## 2. Routine -> reads (active routines)

| Routine | Type | Reads tables | Calls routines |
|---|---|---|---|
| `FUJ_sheets.amazon_long` | table function | `free.amazon_report_full` | `FUJ_extra.JanTable` |
| `FUJ_sheets.amazon_short` | table function | - | `FUJ_sheets.amazon_long` |
| `FUJ_sheets.rakuten_long` | table function | `free.rakuten_report_full` | `FUJ_extra.JanTable` |
| `FUJ_sheets.rakuten_short` | table function | - | `FUJ_sheets.rakuten_long` |
| `FUJ_sheets.shopify_long` | table function | - | `FUJ_extra.JanTable`, `FUJ_extra.ShopifyCombinedReport` |
| `FUJ_sheets.shopify_short` | table function | - | `FUJ_sheets.shopify_long` |
| `FUJ_sheets.Shopify_totals` | table function | `Shopify.amazon_pay`, `Shopify.komoju_payments`, `Shopify.shopify_payouts_api`, `Shopify.stripe_payouts` | - |
| `FUJ_extra.AMA_SUB` | table function | `Amazon.order_central_master`, `Amazon.transaction_view_master`, `free.amazon_report_full` | `FUJ_extra.DateTypeAma`, `FUJ_extra.MainDateAma` |
| `FUJ_extra.Balance` | scalar function | - | - |
| `FUJ_extra.DateTypeAma` | scalar function | - | - |
| `FUJ_extra.JanTable` | table function | `free.sku_master` | - |
| `FUJ_extra.MainDateAma` | scalar function | - | - |
| `FUJ_extra.MISSING_SKU` | table function | `Amazon.order_central_master`, `free.amazon_report_full`, `free.rakuten_report_full`, `free.sh_ama_freee_full`, `free.shopify_freee_full`, `free.sku_master`, `free.stripe_freee_full` | - |
| `FUJ_extra.Rakuten_full` | table function | `Rakuten.orders` | `FUJ_extra.zipCodeRak` |
| `FUJ_extra.ShopifyCombinedReport` | table function | `free.sh_ama_freee_full`, `free.sh_komoju`, `free.shopify_freee_full`, `free.stripe_freee_full` | - |
| `FUJ_extra.SHP_REP` | table function | - | `FUJ_extra.JanTable`, `FUJ_extra.ShopifyCombinedReport` |
| `FUJ_extra.SKU_COURSES` | table function | `Shopify.orders_master`, `free.sku_master` | - |
| `FUJ_extra.zipCodeRak` | scalar function | - | - |
| `FUJ_scheduled.amazon_report` | table function | `Amazon.nomenclature`, `Amazon.transaction_view_master` | `FUJ_extra.AMA_SUB`, `FUJ_extra.Balance` |
| `FUJ_scheduled.komoju_rep` | table function | `Shopify.komoju_payments`, `Shopify.orders_master` | - |
| `FUJ_scheduled.rakuten_report` | table function | `Rakuten.payment_main`, `free.rakuten_report_full` | `FUJ_extra.Rakuten_full` |
| `FUJ_scheduled.SA_report` | table function | `Shopify.amazon_pay`, `Shopify.orders_master`, `free.sh_ama_freee_full` | - |
| `FUJ_scheduled.samples_shopify` | table function | `Shopify.orders_master`, `free.shopify_freee_full` | - |
| `FUJ_scheduled.shopify_report` | table function | `Shopify.orders_master`, `Shopify.shopify_payouts_api` | - |
| `FUJ_scheduled.stripe_rep` | table function | `Shopify.orders_master`, `Shopify.stripe_api`, `Shopify.stripe_payouts`, `free.stripe_freee_full` | - |

## 3. Reverse index: table/routine -> read by

Readers tagged (legacy) are not part of the active pipeline.

### Tables (Amazon, Rakuten, Shopify, free)

| Table | Status | Written by | Read by |
|---|---|---|---|
| `Amazon.nomenclature` | active (fee-type mapping, single source of truth; new fee types added by hand) | loaded outside BigQuery | `FUJ_extra.amazon_full` (legacy); `FUJ_scheduled.amazon_report` |
| `Amazon.order_central_master` | active | loaded outside BigQuery | `FUJ_extra.AMA_SUB`; `FUJ_extra.MISSING_SKU`; `FUJ_extra.amazon_full` (legacy) |
| `Amazon.transaction_view_master` | active | loaded outside BigQuery | `FUJ_extra.AMA_SUB`; `FUJ_extra.amazon_full` (legacy); `FUJ_scheduled.amazon_report`; sheet FUJ_<YYYY>_AMAZON tab current_set |
| `Rakuten.orders` | active | loaded outside BigQuery | `FUJ_extra.Rakuten_full` |
| `Rakuten.payment_main` | active | loaded outside BigQuery | `FUJ_scheduled.rakuten_report` |
| `Shopify.amazon_pay` | active | loaded outside BigQuery | `FUJ_scheduled.SA_report`; `FUJ_sheets.Shopify_totals` |
| `Shopify.komoju_payments` | active | loaded outside BigQuery | `FUJ_scheduled.komoju_rep`; `FUJ_sheets.Shopify_totals` |
| `Shopify.orders_master` | active | loaded outside BigQuery | `FUJ_extra.SKU_COURSES`; `FUJ_scheduled.SA_report`; `FUJ_scheduled.komoju_rep`; `FUJ_scheduled.samples_shopify`; `FUJ_scheduled.shopify_report`; `FUJ_scheduled.stripe_rep` |
| `Shopify.shopify_payouts_api` | active | loaded outside BigQuery | `FUJ_scheduled.shopify_report`; `FUJ_sheets.Shopify_totals` |
| `Shopify.stripe_api` | active | loaded outside BigQuery | `FUJ_scheduled.stripe_rep` |
| `Shopify.stripe_payouts` | active | loaded outside BigQuery | `FUJ_scheduled.stripe_rep`; `FUJ_sheets.Shopify_totals` |
| `free.amazon_report_full` | active | scheduled query `FUJ_Amazon_freee` | `FUJ_extra.AMA_SUB`; `FUJ_extra.MISSING_SKU`; `FUJ_sheets.amazon_long` |
| `free.rakuten_report_full` | active | scheduled query `FUJ_Rakuten_freee` | `FUJ_extra.MISSING_SKU`; `FUJ_scheduled.rakuten_report`; `FUJ_sheets.rakuten_long`; scheduled query `FUJ_Rakuten_freee` |
| `free.sh_ama_freee_full` | active | scheduled query `FUJ_Shopify_Amazon_freee` | `FUJ_extra.MISSING_SKU`; `FUJ_extra.ShopifyCombinedReport`; `FUJ_scheduled.SA_report` |
| `free.sh_komoju` | active | manual job | `FUJ_extra.ShopifyCombinedReport` |
| `free.shopify_freee_full` | active | scheduled query `FUJ_Shopify` | `FUJ_extra.MISSING_SKU`; `FUJ_extra.ShopifyCombinedReport`; `FUJ_scheduled.samples_shopify`; scheduled query `FUJ_Shopify` |
| `free.sku_master` | active | - | `FUJ_extra.JanTable`; `FUJ_extra.MISSING_SKU`; `FUJ_extra.SKU_COURSES` |
| `free.stripe_freee_full` | active | scheduled query `FUJ_Shopify_Stripe` | `FUJ_extra.MISSING_SKU`; `FUJ_extra.ShopifyCombinedReport`; `FUJ_scheduled.stripe_rep` |

### Routines

| Routine | Status | Read / called by |
|---|---|---|
| `FUJ_sheets.amazon_long` | active | `FUJ_sheets.amazon_short`; sheet FUJ_<YYYY>_AMAZON tab AMA_L |
| `FUJ_sheets.amazon_short` | active | sheet FUJ_<YYYY>_AMAZON tab AMA_S |
| `FUJ_sheets.rakuten_long` | active | `FUJ_sheets.rakuten_short`; sheet FUJ_<YYYY>_RAKUTEN tab RAK_L |
| `FUJ_sheets.rakuten_short` | active | sheet FUJ_<YYYY>_RAKUTEN tab RAK_S |
| `FUJ_sheets.shopify_long` | active | `FUJ_sheets.shopify_short`; sheet FUJ_<YYYY>_SHOPIFY tab SH_L |
| `FUJ_sheets.shopify_short` | active | sheet FUJ_<YYYY>_SHOPIFY tab SH_S |
| `FUJ_sheets.Shopify_totals` | active | sheet FUJ_<YYYY>_SHOPIFY tab totals |
| `FUJ_extra.AMA_SUB` | active | `FUJ_scheduled.amazon_report` |
| `FUJ_extra.amazon_full` | legacy | - |
| `FUJ_extra.Balance` | active | `FUJ_extra.amazon_full` (legacy); `FUJ_scheduled.ASG_report` (legacy); `FUJ_scheduled.amazon_report` |
| `FUJ_extra.DateTypeAma` | active | `FUJ_extra.AMA_SUB`; `FUJ_extra.amazon_full` (legacy); `FUJ_scheduled.ASG_report` (legacy) |
| `FUJ_extra.JanTable` | active | `FUJ_extra.MainShopifySub` (legacy); `FUJ_extra.SHP_REP`; `FUJ_extra.shopify_short_old` (legacy); `FUJ_sheets.ASG_long` (legacy); `FUJ_sheets.amazon_long`; `FUJ_sheets.rakuten_long`; `FUJ_sheets.shopify_long` |
| `FUJ_extra.MainDateAma` | active | `FUJ_extra.AMA_SUB`; `FUJ_extra.amazon_full` (legacy); `FUJ_scheduled.ASG_report` (legacy) |
| `FUJ_extra.MainShopifySub` | legacy | - |
| `FUJ_extra.MISSING_SKU` | active, run ad hoc (no BigQuery caller) | - |
| `FUJ_extra.Rakuten_full` | active | `FUJ_scheduled.rakuten_report` |
| `FUJ_extra.shopify_short_old` | legacy | - |
| `FUJ_extra.ShopifyCombinedReport` | active | `FUJ_extra.MainShopifySub` (legacy); `FUJ_extra.SHP_REP`; `FUJ_extra.shopify_short_old` (legacy); `FUJ_sheets.shopify_long` |
| `FUJ_extra.SHP_REP` | active, run ad hoc (no BigQuery caller) | - |
| `FUJ_extra.SKU_COURSES` | active, run ad hoc (no BigQuery caller) | - |
| `FUJ_extra.template` | legacy | - |
| `FUJ_extra.zipCodeRak` | active | `FUJ_extra.Rakuten_full` |
| `FUJ_scheduled.amazon_report` | active | scheduled query `FUJ_Amazon_freee` |
| `FUJ_scheduled.komoju_rep` | active | manual job -> `free.sh_komoju` |
| `FUJ_scheduled.rakuten_report` | active | scheduled query `FUJ_Rakuten_freee` |
| `FUJ_scheduled.SA_report` | active | scheduled query `FUJ_Shopify_Amazon_freee` |
| `FUJ_scheduled.samples_shopify` | active, run ad hoc (no BigQuery caller) | - |
| `FUJ_scheduled.shopify_report` | active | scheduled query `FUJ_Shopify` |
| `FUJ_scheduled.stripe_rep` | active | scheduled query `FUJ_Shopify_Stripe` |
| `FUJ_scheduled.template` | legacy | - |

## 4. Scheduled queries (BigQuery Data Transfer, location US)

| Name | Schedule (UTC) | Source routine | Destination | Write disposition | Dedup filter |
|---|---|---|---|---|---|
| FUJ_Amazon_freee | every day 00:12 UTC | `FUJ_scheduled.amazon_report()` | `free.amazon_report_full` | WRITE_APPEND | `settlement_id NOT IN free.amazon_report_full` (in FUJ_extra.AMA_SUB) |
| FUJ_Shopify_Amazon_freee | every sun,wed,fri 23:15 UTC | `FUJ_scheduled.SA_report()` | `free.sh_ama_freee_full` | WRITE_APPEND | `control_number NOT IN free.sh_ama_freee_full` (in FUJ_scheduled.SA_report) |
| FUJ_Shopify | every mon 02:05 UTC | `FUJ_scheduled.shopify_report()` | `free.shopify_freee_full` | WRITE_APPEND | `control_number NOT IN free.shopify_freee_full` (scheduled query) |
| FUJ_Rakuten_freee | every sat 00:00 UTC | `FUJ_scheduled.rakuten_report()` | `free.rakuten_report_full` | WRITE_APPEND | `control_number NOT IN free.rakuten_report_full` (scheduled query); `control_number IS NOT NULL AND amount <> 0` (scheduled query); `control_number NOT IN free.rakuten_report_full` (in FUJ_scheduled.rakuten_report) |
| FUJ_Shopify_Stripe | every day 23:35 UTC | `FUJ_scheduled.stripe_rep()` | `free.stripe_freee_full` | WRITE_APPEND | `SETT_NUMBER NOT IN free.stripe_freee_full` (in FUJ_scheduled.stripe_rep) |
| (manual) | not scheduled (run by hand) | `FUJ_scheduled.komoju_rep()` | `free.sh_komoju` | INSERT (append) | none: `FUJ_scheduled.komoju_rep` does not read `free.sh_komoju` |

## 5. Legacy / not part of the active pipeline

- Legacy datasets: `Adjustments`, `Backup`, `FUJ_draft`, `FUJ_other`, `FUJ_test`, `Reports`, `apps_script`, `free_adj`, `reports_free_adjusted`, `temp_nomenclature`, `z_functions`, `z_test_tables`
- Amazon Singapore (inactive): `Amazon.ex_sheet`, `Amazon.exchange_sg`, `Amazon.oc_sg`, `Amazon.oc_sg_new`, `Amazon.tv_sg`, `Amazon.tv_sg_new`, `FUJ_scheduled.ASG_report`, `FUJ_sheets.ASG_long`, `FUJ_sheets.ASG_short`, `free.AMA_SG`
- Legacy routines in active datasets: `FUJ_extra.MainShopifySub`, `FUJ_extra.amazon_full`, `FUJ_extra.shopify_short_old`, `FUJ_extra.template`, `FUJ_scheduled.template`
- Legacy tables in active datasets: `Amazon.nomenclature_COPY`, `Amazon.orders_central_jp`, `Amazon.transaction_view_jp`, `Rakuten.free_draft`, `Rakuten.orders_new_cols`, `Shopify.amazon_pay_2`, `Shopify.komoju_payments_temp`, `Shopify.refunds_master`, `Shopify.ss_api_order_number`, `Shopify.ss_pay_orders`, `free.new_sku`

