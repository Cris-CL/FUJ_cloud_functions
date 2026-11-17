"""payment-rakuten Cloud Function.

Triggered by a file upload (GCS finalize) on bucket rakuten_payment_raw and
routed on the object name:

- ``*_Shop_Billpay.csv``: 楽天ペイ Billpay statement (精算書) -> ``Rakuten.statement``, then the
  freee cost rows of every deposit not yet written are appended to ``free.rakuten_cost_full``
  (built by ``FUJ_scheduled.rakuten_cost_report()``, one deposit written once)
- anything else:          楽天ペイ payment CSV -> ``Rakuten.payment_main`` + ``Rakuten.payment_coupon``

A file that loaded is then moved to ``gs://fujiorg-sales-data/Rakuten/<YYYY>/deals|billpay/``
(``archive_object_name``); a file that failed stays in rakuten_payment_raw.

Pinned dependencies: see requirements.txt. Deploy with deploy.sh.
"""

import os
import re
from datetime import datetime

import functions_framework
import pandas as pd
import numpy as np
import pandas_gbq
from google.api_core.exceptions import NotFound, PreconditionFailed
from google.cloud import bigquery, storage


def get_complete_dataframe(uri):
    complete_df = pd.read_csv(uri, header=3, dtype=str)

    rep_col = list(complete_df.columns)
    rep_col[-1] = "report_id"
    complete_df.columns = rep_col

    complete_df.astype({"report_id": "str"})

    complete_df[complete_df.columns[-1]] = complete_df[complete_df.columns[-1]].map(
        lambda x: None if str(x) in ["nan", np.nan] else str(x).replace(".0", "")
    )
    div_index = []
    ini = 0
    for i in range(len(complete_df)):
        if complete_df["連番"].iloc[i] == "連番":
            ini += i
        if complete_df["連番"].iloc[i] == "合計":
            div_index.append(i)
    return [complete_df, div_index, ini]


def process_main_df(complete_df, div_index):
    df_payment = complete_df.iloc[: div_index[0]].copy()
    df_payment.columns = [
        "number",
        "order_confirmation_date",
        "order_number",
        "payment_number",
        "payment_institution_link_number",
        "onquiry_number",
        "payment_confirmation_date",
        "rakuten_pay_money",
        "payment_method",
        "summary",
        "report_id",
    ]
    for col_name in df_payment.columns:
        df_payment[col_name] = df_payment[col_name].map(
            lambda x: x.replace(",", "").replace(".", "") if isinstance(x, str) else x
        )
    df_payment = df_payment.astype(
        {
            "order_confirmation_date": "datetime64[ns]",
            "rakuten_pay_money": "float64",
            "payment_confirmation_date": "datetime64[ns]",
        }
    )
    df_payment.reset_index(inplace=True, drop=True)
    df_payment.drop(columns=["number"], inplace=True)

    return df_payment


def process_coupon_df(complete_df, ini):
    df_coupon = complete_df.iloc[ini:-1].copy()
    new_col = df_coupon.iloc[0]
    df_coupon.columns = new_col
    df_coupon = df_coupon.iloc[1:].copy()

    df_coupon.reset_index(inplace=True, drop=True)
    coupon_transaltion = {
        "連番": "number",
        "注文確認日": "order_confirmation_date",
        "課税資産譲渡日": "order_confirmation_date",
        "受注番号": "order_number",
        "クーポン利用確定日": "coupon_confirmation_date",
        "クーポン支払額確定日": "coupon_confirmation_date",
        "クーポン利用額": "coupon_amount",
        "クーポン名": "coupon_name",
    }

    df_coupon = df_coupon.rename(
        columns={col: coupon_transaltion.get(col, "error") for col in df_coupon.columns}
    )

    new_col_2 = [
        "number",
        "order_number",
        "order_confirmation_date",
        "coupon_name",
        "coupon_amount",
        "coupon_confirmation_date",
        "report_id",
    ]

    rep_col2 = list(df_coupon.columns)
    rep_col2[-1] = "report_id"
    df_coupon.columns = rep_col2

    df_coupon = df_coupon[new_col_2]

    df_coupon.drop(columns=["number"], inplace=True)

    for col_name in df_coupon.columns:
        df_coupon[col_name] = df_coupon[col_name].map(
            lambda x: x.replace(",", "").replace(".", "") if isinstance(x, str) else x
        )
    df_coupon = df_coupon.astype(
        {
            "report_id": "str",
            "coupon_amount": "float64",
            "order_confirmation_date": "datetime64[ns]",
            "coupon_confirmation_date": "datetime64[ns]",
        }
    )
    return df_coupon


# --------------------------------------------------------------------------- #
# 楽天ペイ Billpay statement (精算書)
# --------------------------------------------------------------------------- #

STATEMENT_SUFFIX = "_Shop_Billpay.csv"
STATEMENT_ENCODING = "cp932"
STATEMENT_EMPTY_VALUES = ("", "-")
# 品目 of the line whose 精算対象期間終了日 is the deposit date (= report_id).
# Older statements say 楽天ﾍﾟｲ_決済金, newer ones 楽天ﾍﾟｲ_決済金等: prefix match.
DEPOSIT_LINE_PREFIX = "楽天ﾍﾟｲ_決済金"
# Table function that builds the freee cost rows from the statement lines (queries_bq/rakuten_cost/03).
COST_REPORT_ROUTINE = "FUJ_scheduled.rakuten_cost_report"

# (CSV header, BigQuery column, BigQuery type), in file order. The header of
# every uploaded file must match this list exactly.
STATEMENT_COLUMNS = [
    ("発行日", "issue_date", "DATE"),
    ("精算書No", "statement_number", "STRING"),
    ("店舗別内訳書No", "store_statement_number", "STRING"),
    ("店舗別ID", "store_id", "STRING"),
    ("ＵＲＬ", "url", "STRING"),
    ("店舗名", "store_name", "STRING"),
    ("支払（税込額）", "payment_amount_tax_included", "FLOAT64"),
    ("請求（税抜額）", "billing_amount_tax_excluded", "FLOAT64"),
    ("請求（税額）", "billing_tax_amount", "FLOAT64"),
    ("支払/請求分類", "payment_billing_category", "STRING"),
    ("集約科目", "aggregate_subject", "STRING"),
    ("品目", "subject_name", "STRING"),  # nomenclature key (集約科目名 in older files)
    ("精算対象期間開始日", "period_start_date", "DATE"),
    ("精算対象期間終了日", "period_end_date", "DATE"),
    ("金額", "amount", "FLOAT64"),
    ("うち消費税", "consumption_tax", "FLOAT64"),
    ("税率", "tax_rate", "FLOAT64"),
]
STATEMENT_HEADER = [jp for jp, _, _ in STATEMENT_COLUMNS]
STATEMENT_RENAME = {jp: en for jp, en, _ in STATEMENT_COLUMNS}
STATEMENT_DATE_COLUMNS = [en for _, en, t in STATEMENT_COLUMNS if t == "DATE"]
STATEMENT_NUMERIC_COLUMNS = [en for _, en, t in STATEMENT_COLUMNS if t == "FLOAT64"]
STATEMENT_SCHEMA = [{"name": en, "type": t} for _, en, t in STATEMENT_COLUMNS] + [
    {"name": "report_id", "type": "STRING"},
    {"name": "FILE_NAME", "type": "STRING"},
]


class StatementError(ValueError):
    """A Billpay statement file does not have the expected shape; nothing is uploaded."""


def is_statement_file(name):
    return name.endswith(STATEMENT_SUFFIX)


def read_statement_csv(uri, file_name):
    """Read a Billpay statement CSV as strings; '-' and '' become None."""
    df = pd.read_csv(uri, encoding=STATEMENT_ENCODING, dtype=str, keep_default_na=False)
    header = list(df.columns)
    if header != STATEMENT_HEADER:
        raise StatementError(
            f"{file_name}: header does not match the {len(STATEMENT_HEADER)} expected "
            f"columns.\n  got:      {header}\n  expected: {STATEMENT_HEADER}"
        )
    df = df.rename(columns=STATEMENT_RENAME)
    df = df.map(lambda x: None if x in STATEMENT_EMPTY_VALUES else x)
    return df


def statement_report_id(df, file_name):
    """report_id = 精算対象期間終了日 (YYYYMMDD) of the single 楽天ﾍﾟｲ_決済金* line."""
    is_deposit = df["subject_name"].fillna("").str.startswith(DEPOSIT_LINE_PREFIX)
    deposit = df[is_deposit]
    if len(deposit) == 0:
        raise StatementError(
            f"{file_name}: no line with 品目 starting with '{DEPOSIT_LINE_PREFIX}'; "
            "cannot derive report_id"
        )
    if len(deposit) > 1:
        raise StatementError(
            f"{file_name}: {len(deposit)} lines with 品目 starting with "
            f"'{DEPOSIT_LINE_PREFIX}' ({deposit['subject_name'].tolist()}); expected exactly one"
        )
    end_date = deposit["period_end_date"].iloc[0]
    if pd.isna(end_date):
        raise StatementError(
            f"{file_name}: the '{DEPOSIT_LINE_PREFIX}' line has no 精算対象期間終了日; "
            "cannot derive report_id"
        )
    return end_date.strftime("%Y%m%d")


def process_statement_df(df, file_name):
    """Type the statement columns and add report_id and FILE_NAME."""
    df = df.copy()
    if df.empty:
        raise StatementError(f"{file_name}: the statement has no lines")
    for col in STATEMENT_NUMERIC_COLUMNS:
        df[col] = pd.to_numeric(df[col].str.replace(",", "", regex=False)).astype("float64")
    for col in STATEMENT_DATE_COLUMNS:
        df[col] = pd.to_datetime(df[col], format="%Y/%m/%d")
    statement_numbers = df["statement_number"].dropna().unique().tolist()
    if len(statement_numbers) != 1:
        raise StatementError(
            f"{file_name}: expected one 精算書No in the file, got {statement_numbers}"
        )
    df["report_id"] = statement_report_id(df, file_name)
    df["FILE_NAME"] = file_name
    return df


def statement_already_loaded(statement_number):
    """True when the destination table already holds lines of this 精算書No."""
    dataset_id = os.environ.get("DATASET_ID")
    table_3 = os.environ.get("TABLE_ID_3")
    client = bigquery.Client()
    job = client.query(
        f"SELECT COUNT(*) AS n FROM `{dataset_id}.{table_3}` "
        "WHERE statement_number = @statement_number",
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("statement_number", "STRING", statement_number)
            ]
        ),
    )
    try:
        rows = list(job.result())
    except NotFound:
        return False  # table not created yet: nothing loaded
    return rows[0]["n"] > 0


def materialise_cost_rows():
    """Append the freee cost rows of every deposit not yet in the cost table.

    Same query as queries_bq/rakuten_cost/05_materialise_cost_rows.sql: a deposit
    (control_number) is written once, so re-running it appends nothing.
    """
    cost_table = f"{os.environ.get('COST_DATASET_ID')}.{os.environ.get('COST_TABLE_ID')}"
    query = f"""
INSERT INTO `{cost_table}`
SELECT
  *
FROM
  {COST_REPORT_ROUTINE}()
WHERE
  control_number IS NOT NULL AND amount <> 0
AND
  control_number NOT IN (
    SELECT DISTINCT
      control_number
    FROM
      `{cost_table}`
    WHERE
      control_number IS NOT NULL
  )"""
    job = bigquery.Client().query(query)
    job.result()
    print(f"{job.num_dml_affected_rows} cost rows appended to {cost_table}")
    return job.num_dml_affected_rows


def update_statement(uri, file_name):
    df_statement = process_statement_df(read_statement_csv(uri, file_name), file_name)
    statement_number = df_statement["statement_number"].iloc[0]
    report_id = df_statement["report_id"].iloc[0]
    if statement_already_loaded(statement_number):
        raise StatementError(
            f"{file_name}: statement {statement_number} is already loaded; "
            "delete its rows from the statement table before loading it again"
        )
    upload_df(df_statement, "statement")
    print(
        f"statement {statement_number}: {len(df_statement)} lines uploaded "
        f"with report_id {report_id}"
    )
    materialise_cost_rows()
    return report_id


def upload_df(df, df_name):

    dataset_id = os.environ.get("DATASET_ID")
    table_1 = os.environ.get("TABLE_ID_1")
    table_2 = os.environ.get("TABLE_ID_2")
    table_3 = os.environ.get("TABLE_ID_3")

    df_destination = {
        "main": f"{dataset_id}.{table_1}",
        "coupon": f"{dataset_id}.{table_2}",
        "statement": f"{dataset_id}.{table_3}",
    }
    table_schema = STATEMENT_SCHEMA if df_name == "statement" else None
    try:
        pandas_gbq.to_gbq(
            df,
            destination_table=df_destination[df_name],
            if_exists="append",
            progress_bar=False,
            table_schema=table_schema,
        )
        print(f"{df_name} dataframe correctly uploaded")
    except Exception as err:
        print(f"Error {err} with type {type(err)} occurred")
        raise TypeError(f"{df_name} dataframe incorrect or missing")
    return


# --------------------------------------------------------------------------- #
# Archive: a file that loaded is moved out of the upload bucket
# --------------------------------------------------------------------------- #

ARCHIVE_BUCKET = "fujiorg-sales-data"
ARCHIVE_PREFIX = "Rakuten"
# "FUJ_202400_Rakuten Deals - 20261010.csv": the FUJ_YYYY00_ prefix is dropped and
# the year comes from the trailing date (the prefix year can be older than the date).
DEALS_NAME = re.compile(r"^(?:FUJ_\d{6}_)?(?P<rest>.* - (?P<date>\d{8})\.csv)$")


class ArchiveError(ValueError):
    """The file loaded but could not be archived; it stays in the upload bucket."""


def _year(yyyymmdd, file_name):
    try:
        return datetime.strptime(yyyymmdd, "%Y%m%d").strftime("%Y")
    except ValueError:
        raise ArchiveError(f"{file_name}: '{yyyymmdd}' is not a YYYYMMDD date")


def archive_object_name(file_name, report_id=None):
    """Object name in ARCHIVE_BUCKET for an uploaded file.

    Deals:   Rakuten/<YYYY>/deals/<name without FUJ_YYYY00_>, YYYY from the date in the name.
    Billpay: Rakuten/<YYYY>/billpay/Billpay_<report_id>.csv, report_id = deposit date.
    """
    if is_statement_file(file_name):
        if not report_id:
            raise ArchiveError(f"{file_name}: a Billpay file needs its report_id to be archived")
        year = _year(report_id, file_name)
        return f"{ARCHIVE_PREFIX}/{year}/billpay/Billpay_{report_id}.csv"
    match = DEALS_NAME.match(file_name)
    if not match:
        raise ArchiveError(
            f"{file_name}: name has no ' - YYYYMMDD.csv' date; cannot choose the archive folder"
        )
    year = _year(match["date"], file_name)
    return f"{ARCHIVE_PREFIX}/{year}/deals/{match['rest']}"


def move_to_archive(client, bucket_name, name, generation, destination_name):
    """Copy the object to ARCHIVE_BUCKET, then delete the source.

    The copy never overwrites an existing archive object (if_generation_match=0), and the
    delete only removes the exact generation that was copied. On a failed copy the
    source is left in place.
    """
    source_bucket = client.bucket(bucket_name)
    source = source_bucket.blob(name, generation=generation)
    destination = client.bucket(ARCHIVE_BUCKET)
    try:
        source_bucket.copy_blob(
            source,
            destination,
            destination_name,
            source_generation=generation,
            if_generation_match=0,
        )
    except PreconditionFailed:
        raise ArchiveError(
            f"{name}: gs://{ARCHIVE_BUCKET}/{destination_name} already exists; "
            "not overwritten, the file stays in the upload bucket"
        )
    source_bucket.delete_blob(name, if_generation_match=generation)
    print(f"archived gs://{bucket_name}/{name} -> gs://{ARCHIVE_BUCKET}/{destination_name}")


def archive(bucket_name, name, generation, report_id=None):
    move_to_archive(
        storage.Client(), bucket_name, name, generation, archive_object_name(name, report_id)
    )


# Triggered by a change in a storage bucket. The file is moved to the archive only
# after it loaded; any failure raises and leaves it in the upload bucket.
@functions_framework.cloud_event
def update_payment(cloud_event):
    data = cloud_event.data
    bucket = data["bucket"]
    name = data["name"]
    generation = int(data["generation"])
    print(name)

    uri = f"gs://{bucket}/{name}"

    if is_statement_file(name):
        report_id = update_statement(uri, name)
        archive(bucket, name, generation, report_id)
        return

    complete_df = get_complete_dataframe(uri)
    df_comp = complete_df[0]
    div_index = complete_df[1]
    ini = complete_df[2]

    try:
        df_main = process_main_df(df_comp, div_index)
    except Exception as err:
        print(f"Error {err} with type {type(err)} occurred")
        raise TypeError("main dataframe incorrect or missing")

    try:
        df_coupon = process_coupon_df(df_comp, ini)
    except Exception as err:
        print(f"Error {err} with type {type(err)} occurred")
        df_coupon = pd.DataFrame()
        # raise TypeError("coupon dataframe incorrect or missing")

    upload_df(df_main, "main")
    upload_df(df_coupon, "coupon")
    print("End upload process")
    archive(bucket, name, generation)
    return
