"""
drive_sync.py

Small helper module for reading/writing files in a Google Shared Drive
folder using a service account. Reused pattern from greythr_sync.yml.

Requires:
    pip install google-api-python-client google-auth

Auth:
    Expects the service account JSON content in the environment variable
    GOOGLE_SERVICE_ACCOUNT_JSON (same secret your greythr_sync workflow
    already uses). In GitHub Actions this is passed in via:
        env:
          GOOGLE_SERVICE_ACCOUNT_JSON: ${{ secrets.GOOGLE_SERVICE_ACCOUNT_JSON }}
"""

import io
import json
import os

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload

SCOPES = ["https://www.googleapis.com/auth/drive"]


def get_drive_service():
    creds_json = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")
    if not creds_json:
        raise RuntimeError(
            "GOOGLE_SERVICE_ACCOUNT_JSON environment variable not set. "
            "Set it locally for testing, or as a GitHub Actions secret."
        )

    creds_info = json.loads(creds_json)
    credentials = service_account.Credentials.from_service_account_info(
        creds_info, scopes=SCOPES
    )
    return build("drive", "v3", credentials=credentials)


def find_file_in_folder(service, folder_id, filename):
    """Return the file's Drive ID if a file with this exact name exists in the folder, else None."""
    query = (
        f"'{folder_id}' in parents "
        f"and name = '{filename}' "
        f"and trashed = false"
    )
    response = service.files().list(
        q=query,
        spaces="drive",
        fields="files(id, name)",
        supportsAllDrives=True,
        includeItemsFromAllDrives=True,
    ).execute()

    files = response.get("files", [])
    return files[0]["id"] if files else None


def download_file(service, folder_id, filename, local_path):
    """Download filename from the Drive folder to local_path. Returns True if found and downloaded."""
    file_id = find_file_in_folder(service, folder_id, filename)
    if not file_id:
        print(f"ℹ '{filename}' not found in Drive folder — nothing to download (may be first run).")
        return False

    request = service.files().get_media(fileId=file_id)
    with io.FileIO(local_path, "wb") as fh:
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()

    print(f"⬇ Downloaded '{filename}' from Drive -> {local_path}")
    return True


def upload_file(service, folder_id, local_path, filename=None):
    """
    Upload local_path to the Drive folder.
    If a file with the same name already exists there, update it in place
    (same file ID preserved). Otherwise create a new file.
    """
    filename = filename or os.path.basename(local_path)
    existing_id = find_file_in_folder(service, folder_id, filename)

    mime_map = {
        ".xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        ".csv": "text/csv",
        ".json": "application/json",
    }
    ext = os.path.splitext(filename)[1].lower()
    mimetype = mime_map.get(ext, "application/octet-stream")

    media = MediaFileUpload(local_path, mimetype=mimetype, resumable=True)

    if existing_id:
        service.files().update(
            fileId=existing_id,
            media_body=media,
            supportsAllDrives=True,
        ).execute()
        print(f"⬆ Updated existing '{filename}' in Drive (same file ID kept)")
    else:
        metadata = {"name": filename, "parents": [folder_id]}
        service.files().create(
            body=metadata,
            media_body=media,
            supportsAllDrives=True,
            fields="id",
        ).execute()
        print(f"⬆ Created new '{filename}' in Drive")
