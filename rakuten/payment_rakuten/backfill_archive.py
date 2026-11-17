"""One-off: move files already loaded from gs://rakuten_payment_raw to the archive.

Uses the same naming/layout as the function (main.archive_object_name). It only moves,
it never reprocesses. A file is moved only when it is already loaded:

- Deals:   the date in its name is a report_id in Rakuten.payment_main
- Billpay: its name is a FILE_NAME in Rakuten.statement (report_id is read from there)

Anything else (not loaded, unparseable name, destination already exists) is listed and
left in place.

Usage (from this folder, with the function's requirements installed):
    python backfill_archive.py            # dry run: prints source -> destination
    python backfill_archive.py --apply    # moves (copies, then deletes the source)
"""

import argparse
import sys

from google.cloud import bigquery, storage

from main import (
    ARCHIVE_BUCKET,
    DEALS_NAME,
    ArchiveError,
    archive_object_name,
    is_statement_file,
    move_to_archive,
)

PROJECT = "test-bigquery-cc"
SOURCE_BUCKET = "rakuten_payment_raw"

# Files uploaded under a wrong date, archived under the corrected name. Deals settle on
# the 10th and 25th; this file holds the 2024-09-26..2024-10-10 settlement, loaded into
# Rakuten.payment_main as report_id 20241010 (its own report_id column says 20241005).
RENAMES = {
    "FUJ_202400_Rakuten Deals - 20241005.csv": "FUJ_202400_Rakuten Deals - 20241010.csv",
}


def loaded_state(bq):
    deals = {
        r["report_id"]
        for r in bq.query("SELECT DISTINCT report_id FROM `Rakuten.payment_main`").result()
    }
    statements = {
        r["FILE_NAME"]: r["report_id"]
        for r in bq.query(
            "SELECT FILE_NAME, ANY_VALUE(report_id) AS report_id "
            "FROM `Rakuten.statement` GROUP BY FILE_NAME"
        ).result()
    }
    return deals, statements


def plan(gcs, bq):
    deals, statements = loaded_state(bq)
    archive = gcs.bucket(ARCHIVE_BUCKET)
    moves, skipped = [], []
    for blob in gcs.list_blobs(SOURCE_BUCKET):
        name = blob.name
        archive_name = RENAMES.get(name, name)
        try:
            if is_statement_file(name):
                report_id = statements.get(name)
                if report_id is None:
                    raise ArchiveError("not in Rakuten.statement (not loaded)")
            else:
                report_id = None
                match = DEALS_NAME.match(archive_name)
                if match and match["date"] not in deals:
                    raise ArchiveError(f"report_id {match['date']} not in Rakuten.payment_main")
            destination = archive_object_name(archive_name, report_id)
            if archive.blob(destination).exists():
                raise ArchiveError(f"gs://{ARCHIVE_BUCKET}/{destination} already exists")
        except ArchiveError as err:
            skipped.append((name, str(err)))
            continue
        moves.append((name, blob.generation, destination))
    return moves, skipped


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--apply", action="store_true", help="move the files (default: dry run)")
    args = parser.parse_args(argv)

    gcs = storage.Client(project=PROJECT)
    bq = bigquery.Client(project=PROJECT)
    moves, skipped = plan(gcs, bq)

    for name, _, destination in moves:
        print(f"MOVE  gs://{SOURCE_BUCKET}/{name} -> gs://{ARCHIVE_BUCKET}/{destination}")
    for name, reason in skipped:
        print(f"SKIP  gs://{SOURCE_BUCKET}/{name}: {reason}")
    print(f"{len(moves)} to move, {len(skipped)} skipped")

    if not args.apply:
        print("dry run: nothing moved (re-run with --apply)")
        return 0
    failed = 0
    for name, generation, destination in moves:
        try:
            move_to_archive(gcs, SOURCE_BUCKET, name, generation, destination)
        except Exception as err:  # keep going; report at the end
            failed += 1
            print(f"FAIL  {name}: {err}")
    print(f"{len(moves) - failed} moved, {failed} failed")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
