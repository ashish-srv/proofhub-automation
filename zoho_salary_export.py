"""
zoho_salary_export.py

Exports the 'Employee Monthly Rate' table from Zoho Analytics (India DC)
as CSV and uploads it to the creatives_dashboard Drive folder as
'Employee Monthly Rate.csv'.

Runs as its own GitHub Actions workflow (creatives_dashboard_zoho_export.yml),
separate from the daily sync, per design.

Required environment variables (GitHub secrets):
  ZOHO_CLIENT_ID
  ZOHO_CLIENT_SECRET
  ZOHO_REFRESH_TOKEN
  ZOHO_ORG_ID
  ZOHO_WORKSPACE_ID
  ZOHO_VIEW_ID                          (view ID of Employee Monthly Rate)
  GOOGLE_SERVICE_ACCOUNT_JSON
  CREATIVES_DASHBOARD_DRIVE_FOLDER_ID
"""

import os
import sys
import requests

from drive_sync import get_drive_service, upload_file

ACCOUNTS_URL  = "https://accounts.zoho.in/oauth/v2/token"
ANALYTICS_URL = "https://analyticsapi.zoho.in/restapi/v2"

OUTPUT_CSV = "Employee Monthly Rate.csv"


def require_env(name):
    value = os.environ.get(name)
    if not value:
        print(f"❌ {name} environment variable not set.")
        sys.exit(1)
    return value


def get_access_token(client_id, client_secret, refresh_token):
    print("▶ Refreshing Zoho access token...")
    response = requests.post(ACCOUNTS_URL, params={
        "refresh_token": refresh_token,
        "client_id": client_id,
        "client_secret": client_secret,
        "grant_type": "refresh_token",
    }, timeout=30)

    if response.status_code != 200:
        print(f"❌ Token refresh failed {response.status_code}: {response.text[:300]}")
        sys.exit(1)

    data = response.json()
    token = data.get("access_token")
    if not token:
        print(f"❌ No access_token in response: {data}")
        sys.exit(1)

    print("✅ Access token obtained.")
    return token


def export_view_as_csv(access_token, org_id, workspace_id, view_id, out_path):
    print("▶ Exporting 'Employee Monthly Rate' from Zoho Analytics...")
    url = f"{ANALYTICS_URL}/workspaces/{workspace_id}/views/{view_id}/data"
    headers = {
        "Authorization": f"Zoho-oauthtoken {access_token}",
        "ZANALYTICS-ORGID": org_id,
    }
    params = {"CONFIG": '{"responseFormat":"csv"}'}

    response = requests.get(url, headers=headers, params=params, timeout=120)

    if response.status_code != 200:
        print(f"❌ Export failed {response.status_code}: {response.text[:300]}")
        sys.exit(1)

    with open(out_path, "wb") as f:
        f.write(response.content)

    # basic sanity check on the downloaded content
    with open(out_path, "r", encoding="utf-8", errors="replace") as f:
        first_line = f.readline().strip()
    print(f"✅ Export saved to '{out_path}' (header: {first_line[:120]})")


def main():
    client_id     = require_env("ZOHO_CLIENT_ID")
    client_secret = require_env("ZOHO_CLIENT_SECRET")
    refresh_token = require_env("ZOHO_REFRESH_TOKEN")
    org_id        = require_env("ZOHO_ORG_ID")
    workspace_id  = require_env("ZOHO_WORKSPACE_ID")
    view_id       = require_env("ZOHO_VIEW_ID")
    drive_folder  = require_env("CREATIVES_DASHBOARD_DRIVE_FOLDER_ID")

    access_token = get_access_token(client_id, client_secret, refresh_token)
    export_view_as_csv(access_token, org_id, workspace_id, view_id, OUTPUT_CSV)

    print("▶ Uploading to Drive...")
    service = get_drive_service()
    upload_file(service, drive_folder, OUTPUT_CSV)

    print("\n✅ Zoho salary export complete.")


if __name__ == "__main__":
    main()
