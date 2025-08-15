# gdrive_sync.py
import os, json, io, re
from pathlib import Path
from typing import List, Dict
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]

def get_drive_service():
    raw = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")
    if not raw:
        raise RuntimeError("GOOGLE_SERVICE_ACCOUNT_JSON is not set")
    info = json.loads(raw)
    # Important: fix escaped newlines in the private key
    if "private_key" in info:
        info["private_key"] = info["private_key"].replace("\\n", "\n")
    creds = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
    return build("drive", "v3", credentials=creds, cache_discovery=False)

def list_pdfs_in_folder(svc, folder_id: str) -> List[Dict]:
    q = f"'{folder_id}' in parents and mimeType='application/pdf' and trashed=false"
    files, token = [], None
    while True:
        resp = svc.files().list(
            q=q, pageSize=1000, pageToken=token,
            fields="nextPageToken, files(id,name)"
        ).execute()
        files.extend(resp.get("files", []))
        token = resp.get("nextPageToken")
        if not token:
            break
    return files

def download_pdf(svc, file_id: str, dest: Path):
    dest.parent.mkdir(parents=True, exist_ok=True)
    req = svc.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, req)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    dest.write_bytes(fh.getvalue())

def safe_name(name: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "_", name)
