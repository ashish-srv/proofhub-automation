"""
recompute_reconciliation.py

Use this instead of run_daily_pipeline.py when you've only edited the
mapping excel (e.g. filled in blank Client Name rows) and want the
Creatives_Dashboard_Data.xlsx updated WITHOUT re-fetching from ProofHub
or the quotations database (which is what takes the long time).

Does:
  1. Downloads the current mapping excel from Drive.
  2. Downloads the last raw proofhub_tasks.csv and Quotation_Required_Data.csv
     from Drive (already fetched by a previous full run — not re-fetched here).
  3. Runs combine_reconcile.py against those files.
  4. Uploads the updated mapping excel + Creatives_Dashboard_Data.xlsx back to Drive.

Typically finishes in seconds, since no ProofHub/API calls happen.
"""

import os
import subprocess
import sys

from drive_sync import get_drive_service, download_file, upload_file

DRIVE_FOLDER_ID = os.environ.get("CREATIVES_DASHBOARD_DRIVE_FOLDER_ID")
if not DRIVE_FOLDER_ID:
    print("❌ CREATIVES_DASHBOARD_DRIVE_FOLDER_ID environment variable not set.")
    sys.exit(1)

MAPPING_EXCEL  = "ProofHub_ProjectName_ClientName_Mapping.xlsx"
OUTPUT_EXCEL   = "Creatives_Dashboard_Data.xlsx"
RAW_PROOFHUB   = "proofhub_tasks.csv"
RAW_QUOTATIONS = "Quotation_Required_Data.csv"
TIMESHEET_CSV  = "All Projects Timesheet.csv"
SALARY_CSV     = "Employee Monthly Rate.csv"

# The timesheet CSV lives in a DIFFERENT Drive folder than the creatives one
TIMESHEET_FOLDER_ID = os.environ.get("GOOGLE_DRIVE_FOLDER_ID")


def require_downloaded(service, filename):
    ok = download_file(service, DRIVE_FOLDER_ID, filename, filename)
    if not ok or not os.path.exists(filename):
        print(f"❌ '{filename}' not found in Drive. Run the full pipeline "
              f"(run_daily_pipeline.py) at least once first to create it.")
        sys.exit(1)


def main():
    service = get_drive_service()

    print("▶ Downloading current mapping + last raw data from Drive (no ProofHub/DB fetch)...")
    require_downloaded(service, MAPPING_EXCEL)
    require_downloaded(service, RAW_PROOFHUB)
    require_downloaded(service, RAW_QUOTATIONS)

    # optional inputs for hours/cost enrichment — warn but continue if absent
    if TIMESHEET_FOLDER_ID:
        download_file(service, TIMESHEET_FOLDER_ID, TIMESHEET_CSV, TIMESHEET_CSV)
    else:
        print("⚠ GOOGLE_DRIVE_FOLDER_ID not set — timesheet download skipped, "
              "hours/employee/cost columns will be 0.")
    download_file(service, DRIVE_FOLDER_ID, SALARY_CSV, SALARY_CSV)

    print("\n▶ Re-running reconciliation...")
    result = subprocess.run("python3 combine_reconcile.py", shell=True)
    if result.returncode != 0:
        print("❌ combine_reconcile.py failed.")
        sys.exit(1)

    print("\n▶ Uploading updated results back to Drive...")
    upload_file(service, DRIVE_FOLDER_ID, MAPPING_EXCEL)   # in case new unmapped rows were added
    upload_file(service, DRIVE_FOLDER_ID, OUTPUT_EXCEL)

    print("\n✅ Recompute complete (ProofHub/DB were not re-fetched).")


if __name__ == "__main__":
    main()
