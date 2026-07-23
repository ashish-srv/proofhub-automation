"""
run_daily_pipeline.py

Orchestrates the full daily pipeline for GitHub Actions:
  1. Download the current mapping excel from the Drive folder
     (skips gracefully on first-ever run if it doesn't exist yet).
  2. Run the ProofHub fetch (proofhub_single_project_fetch.py logic).
  3. Run the quotations fetch (your existing psycopg2 script).
  4. Run combine_reconcile.py (mapping, join, reconciliation).
  5. Upload the updated mapping excel + all output files back to Drive.

Set DRIVE_FOLDER_ID below to the folder ID from your
"Proofhub Automation" shared drive folder.
"""

import os
import subprocess
import sys

from drive_sync import get_drive_service, download_file, upload_file

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
DRIVE_FOLDER_ID = os.environ.get("CREATIVES_DASHBOARD_DRIVE_FOLDER_ID")
if not DRIVE_FOLDER_ID:
    print("❌ CREATIVES_DASHBOARD_DRIVE_FOLDER_ID environment variable not set.")
    sys.exit(1)

MAPPING_EXCEL   = "ProofHub_ProjectName_ClientName_Mapping.xlsx"
OUTPUT_EXCEL    = "Creatives_Dashboard_Data.xlsx"
RAW_PROOFHUB    = "proofhub_tasks.csv"
RAW_QUOTATIONS  = "Quotation_Required_Data.csv"
TIMESHEET_CSV   = "All Projects Timesheet.csv"
SALARY_CSV      = "Employee Monthly Rate.csv"

# The timesheet CSV lives in a DIFFERENT Drive folder than the creatives one
TIMESHEET_FOLDER_ID = os.environ.get("GOOGLE_DRIVE_FOLDER_ID")


def run_step(description, command):
    print(f"\n{'=' * 60}\n▶ {description}\n{'=' * 60}")
    result = subprocess.run(command, shell=True)
    if result.returncode != 0:
        print(f"❌ Step failed: {description}")
        sys.exit(1)


def main():
    service = get_drive_service()

    # ── Step 1: pull the current mapping excel (so we don't lose manual edits) ──
    print("\n▶ Downloading current mapping excel from Drive...")
    download_file(service, DRIVE_FOLDER_ID, MAPPING_EXCEL, MAPPING_EXCEL)
    if not os.path.exists(MAPPING_EXCEL):
        print(f"❌ '{MAPPING_EXCEL}' not found locally or in Drive. "
              f"Upload it manually once to seed the mapping, then re-run.")
        sys.exit(1)

    # ── Step 2: fetch ProofHub tasks (all projects) ──
    run_step("Fetching ProofHub tasks", "python3 proofhub_task_fetch.py")

    # ── Step 3: fetch quotations ──
    run_step("Fetching quotations", "python3 quotations_fetch.py")

    # ── Step 3b: pull timesheet + salary CSVs from Drive (for cost enrichment) ──
    print("\n▶ Downloading timesheet + salary data from Drive...")
    if TIMESHEET_FOLDER_ID:
        download_file(service, TIMESHEET_FOLDER_ID, TIMESHEET_CSV, TIMESHEET_CSV)
    else:
        print("⚠ GOOGLE_DRIVE_FOLDER_ID not set — timesheet download skipped, "
              "hours/employee/cost columns will be 0.")
    download_file(service, DRIVE_FOLDER_ID, SALARY_CSV, SALARY_CSV)
    # both files are optional — combine_reconcile.py handles their absence
    # with warnings and zeroed columns rather than failing.

    # ── Step 4: combine + reconcile ──
    run_step("Combining and reconciling", "python3 combine_reconcile.py")

    # ── Step 5: upload everything back to Drive ──
    print("\n▶ Uploading results back to Drive...")
    upload_file(service, DRIVE_FOLDER_ID, MAPPING_EXCEL)          # updated mapping (new blank rows, if any)
    upload_file(service, DRIVE_FOLDER_ID, OUTPUT_EXCEL)           # 3-sheet reconciled workbook
    upload_file(service, DRIVE_FOLDER_ID, RAW_PROOFHUB)           # raw ProofHub pull, for QA
    upload_file(service, DRIVE_FOLDER_ID, RAW_QUOTATIONS)         # raw quotations pull, for QA

    print("\n✅ Daily pipeline complete.")


if __name__ == "__main__":
    main()
