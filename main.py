# main.py
from __future__ import annotations

import os
import json
import uuid
from pathlib import Path
from typing import Optional, Literal, Dict, Any, List

import httpx
from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from pds_extractor import extract_pds

# --- NEW: Google Drive sync helpers ---
# You already created gdrive_sync.py and installed google-* deps
from gdrive_sync import (
    get_drive_service,
    list_pdfs_in_folder,
    download_pdf,
    safe_name,
)

app = FastAPI(title="Valvoline PDS MVP")

# Folders / files we use locally
DATA_DIR = Path("data");   DATA_DIR.mkdir(exist_ok=True)
PARSED_DIR = Path("parsed"); PARSED_DIR.mkdir(exist_ok=True)
INDEX_PATH = Path("index.json")


# -----------------------
# Simple name -> file index
# -----------------------
def _load_index() -> Dict[str, str]:
    try:
        return json.loads(INDEX_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}

def _save_index(index: Dict[str, str]) -> None:
    INDEX_PATH.write_text(json.dumps(index, ensure_ascii=False, indent=2), encoding="utf-8")

def _save_index_entry(product_name: str, pdf_path: Path) -> None:
    idx = _load_index()
    idx[product_name] = str(pdf_path)
    _save_index(idx)


# -----------------------
# Request models
# -----------------------
class AnswerReq(BaseModel):
    product_a_url: Optional[str] = None
    product_b_url: Optional[str] = None
    product_a_file: Optional[str] = None
    product_b_file: Optional[str] = None
    locale: Literal["no", "en"] = "no"
    expected_output: Literal["summary", "comparison"] = "summary"


# -----------------------
# Health
# -----------------------
@app.get("/health")
def health():
    return {"ok": True}


# -----------------------
# Upload a PDF (binary) -> store on disk
# -----------------------
@app.post("/upload")
async def upload_pdf(file: UploadFile = File(...)):
    if not file.filename.lower().endswith(".pdf"):
        raise HTTPException(400, "Please upload a PDF")
    name = f"{uuid.uuid4().hex}.pdf"
    out = DATA_DIR / name
    out.write_bytes(await file.read())
    return {"stored_as": str(out)}


# -----------------------
# Helper: download a PDF from URL to disk
# -----------------------
async def _download_pdf_to_disk(url: str) -> Path:
    async with httpx.AsyncClient(follow_redirects=True, timeout=60) as client:
        r = await client.get(url)
        if r.status_code != 200 or b"%PDF" not in r.content[:1024]:
            raise HTTPException(400, "URL did not return a valid PDF")
        name = f"{uuid.uuid4().hex}.pdf"
        out = DATA_DIR / name
        out.write_bytes(r.content)
        return out


# -----------------------
# Answer (summary or comparison)
# -----------------------
@app.post("/answer")
async def answer(req: AnswerReq):
    # Resolve A
    if req.product_a_file:
        pA = Path(req.product_a_file)
        if not pA.exists():
            raise HTTPException(400, "product_a_file not found")
    elif req.product_a_url:
        pA = await _download_pdf_to_disk(req.product_a_url)
    else:
        raise HTTPException(400, "Provide product_a_file or product_a_url")
    A = extract_pds(str(pA))

    # Summary path (or only A provided)
    if req.expected_output == "summary" or not (req.product_b_file or req.product_b_url):
        md = (
            f"**Product:** {A.get('product_name_line')}\n\n"
            f"**Revision:** {A.get('version')}\n\n"
            f"**Approvals / Specifications:**\n- "
            + "; ".join(A.get("approvals_and_specs", []))
            + "\n\n"
            f"**Typical properties:**\n"
            + "\n".join(
                [
                    f"- {p['property_name']}: {p['value']} ({p['test_method']})"
                    for p in A.get("typical_properties", [])
                ]
            )
        )
        return JSONResponse({"reply_markdown": md, "productA": A})

    # Resolve B for comparison
    if req.product_b_file:
        pB = Path(req.product_b_file)
        if not pB.exists():
            raise HTTPException(400, "product_b_file not found")
    elif req.product_b_url:
        pB = await _download_pdf_to_disk(req.product_b_url)
    else:
        raise HTTPException(400, "Provide product_b_file or product_b_url")
    B = extract_pds(str(pB))

    # Normalize property names for alignment
    import re
    def norm(s: str) -> str:
        return re.sub(r"\s+", " ", s.strip()).lower()

    mapB = {norm(p["property_name"]): p for p in B.get("typical_properties", [])}
    nameA = A.get("product_name_line") or Path(A["pdf"]).name
    nameB = B.get("product_name_line") or Path(B["pdf"]).name
    verA = A.get("version") or ""
    verB = B.get("version") or ""

    lines = [
        f"**Sammenligning:** {nameA} (Rev. {verA}) vs {nameB} (Rev. {verB})",
        "",
        f"| Egenskap | {nameA} | {nameB} |",
        "|---|---|---|",
    ]
    for p in A.get("typical_properties", []):
        key = norm(p["property_name"])
        q = mapB.get(key)
        valA = p["value"]
        valB = q["value"] if q else "—"
        methA = f" ({p['test_method']})" if p.get("test_method") else ""
        methB = f" ({q['test_method']})" if q and q.get("test_method") else ""
        lines.append(f"| {p['property_name']} | {valA}{methA} | {valB}{methB} |")

    lines += ["", "**Godkjenninger / spesifikasjoner:**"]
    if A.get("approvals_and_specs"):
        lines.append("- " + nameA + ": " + "; ".join(A["approvals_and_specs"]))
    if B.get("approvals_and_specs"):
        lines.append("- " + nameB + ": " + "; ".join(B["approvals_and_specs"]))
    return JSONResponse({"reply_markdown": "\n".join(lines), "productA": A, "productB": B})


# -----------------------
# NEW: Sync Google Drive folder -> local cache, parse, and index
# -----------------------
@app.post("/drive/sync")
def drive_sync(folder_id: Optional[str] = None):
    """
    Downloads all PDFs from the specified Google Drive folder (or env DRIVE_FOLDER_ID),
    parses each once, stores parsed JSON, and updates name->file index.
    """
    fid = folder_id or os.getenv("DRIVE_FOLDER_ID")
    if not fid:
        raise HTTPException(400, "Provide folder_id or set DRIVE_FOLDER_ID")

    # Ensure Google auth is configured
    if "GOOGLE_SERVICE_ACCOUNT_JSON" not in os.environ:
        raise HTTPException(400, "GOOGLE_SERVICE_ACCOUNT_JSON env var is missing")

    svc = get_drive_service()
    files = list_pdfs_in_folder(svc, fid)

    results: List[Dict[str, Any]] = []
    for f in files:
        # Download (always overwrite for simplicity; you can add mtime checks later)
        local = DATA_DIR / safe_name(f["name"])
        download_pdf(svc, f["id"], local)

        # Parse and cache
        parsed = extract_pds(str(local))
        (PARSED_DIR / (local.stem + ".json")).write_text(
            json.dumps(parsed, ensure_ascii=False, indent=2), encoding="utf-8"
        )

        # Choose a display name for index
        name = (parsed.get("product_name_line") or local.stem).strip()
        if name:
            _save_index_entry(name, local)

        results.append({"name": name, "stored_as": str(local)})

    return {"count": len(results), "items": results}
