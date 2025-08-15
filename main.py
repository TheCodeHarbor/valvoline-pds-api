# main.py
import os, json, uuid
from pathlib import Path
from typing import Optional, Literal

import httpx
from fastapi import FastAPI, File, UploadFile, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from pds_extractor import extract_pds
from gdrive_sync import get_drive_service, list_pdfs_in_folder, download_pdf, safe_name

app = FastAPI(title="Valvoline PDS MVP")

# Folders and index
DATA_DIR   = Path("data");    DATA_DIR.mkdir(exist_ok=True)
PARSED_DIR = Path("parsed");  PARSED_DIR.mkdir(exist_ok=True)
INDEX_PATH = Path("index.json")

# ---------- tiny helpers ----------
def _save_index_entry(product_name: str, pdf_path: Path):
    idx = _load_index()
    idx[product_name.strip()] = str(pdf_path)
    INDEX_PATH.write_text(json.dumps(idx, ensure_ascii=False, indent=2), encoding="utf-8")

def _load_index() -> dict:
    if INDEX_PATH.exists():
        return json.loads(INDEX_PATH.read_text(encoding="utf-8"))
    return {}

# ---------- request models ----------
class AnswerReq(BaseModel):
    product_a_url: Optional[str] = None
    product_b_url: Optional[str] = None
    product_a_file: Optional[str] = None
    product_b_file: Optional[str] = None
    locale: Literal["no","en"] = "no"
    expected_output: Literal["summary","comparison"] = "summary"

class NameReq(BaseModel):
    product_a_name: str
    product_b_name: Optional[str] = None
    locale: Literal["no","en"] = "no"

# ---------- basic endpoints ----------
@app.get("/health")
def health():
    return {"ok": True}

@app.post("/upload")
async def upload_pdf(file: UploadFile = File(...)):
    if not file.filename.lower().endswith(".pdf"):
        raise HTTPException(400, "Please upload a PDF")
    name = f"{uuid.uuid4().hex}.pdf"
    out = DATA_DIR / name
    out.write_bytes(await file.read())
    return {"stored_as": str(out)}

async def _download_pdf_to_disk(url: str) -> Path:
    async with httpx.AsyncClient(follow_redirects=True, timeout=60) as client:
        r = await client.get(url)
        if r.status_code != 200 or b"%PDF" not in r.content[:1024]:
            raise HTTPException(400, "URL did not return a valid PDF")
        name = f"{uuid.uuid4().hex}.pdf"
        out = DATA_DIR / name
        out.write_bytes(r.content)
        return out

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

    # Summary only?
    if req.expected_output == "summary" or not (req.product_b_file or req.product_b_url):
        md = (
            f"**Product:** {A.get('product_name_line')}\n\n"
            f"**Revision:** {A.get('version')}\n\n"
            f"**Approvals / Specifications:**\n- " +
            "; ".join(A.get('approvals_and_specs', [])) + "\n\n" +
            "**Typical properties:**\n" +
            "\n".join([f"- {p['property_name']}: {p['value']} ({p['test_method']})"
                       for p in A.get('typical_properties', [])])
        )
        return JSONResponse({"reply_markdown": md, "productA": A})

    # Resolve B (for comparison)
    if req.product_b_file:
        pB = Path(req.product_b_file)
        if not pB.exists():
            raise HTTPException(400, "product_b_file not found")
    elif req.product_b_url:
        pB = await _download_pdf_to_disk(req.product_b_url)
    else:
        raise HTTPException(400, "Provide product_b_file or product_b_url")
    B = extract_pds(str(pB))

    import re
    norm = lambda s: re.sub(r"\s+", " ", s.strip()).lower()
    mapB = { norm(p["property_name"]): p for p in B.get("typical_properties", []) }

    nameA = A.get("product_name_line") or Path(A["pdf"]).name
    nameB = B.get("product_name_line") or Path(B["pdf"]).name
    verA  = A.get("version") or ""; verB = B.get("version") or ""

    lines = [
        f"**Sammenligning:** {nameA} (Rev. {verA}) vs {nameB} (Rev. {verB})", "",
        f"| Egenskap | {nameA} | {nameB} |", "|---|---|---|"
    ]
    for p in A.get("typical_properties", []):
        key  = norm(p["property_name"]); q = mapB.get(key)
        valA = p["value"]; valB = q["value"] if q else "—"
        methA = f" ({p['test_method']})" if p.get("test_method") else ""
        methB = f" ({q['test_method']})" if q and q.get("test_method") else ""
        lines.append(f"| {p['property_name']} | {valA}{methA} | {valB}{methB} |")
    lines += ["", "**Godkjenninger / spesifikasjoner:**"]
    if A.get("approvals_and_specs"): lines.append("- " + nameA + ": " + "; ".join(A["approvals_and_specs"]))
    if B.get("approvals_and_specs"): lines.append("- " + nameB + ": " + "; ".join(B["approvals_and_specs"]))
    return JSONResponse({"reply_markdown": "\n".join(lines), "productA": A, "productB": B})

# ---------- Drive diagnostics ----------
@app.get("/drive/check")
def drive_check():
    try:
        svc = get_drive_service()
        about = svc.about().get(fields="user,kind").execute()
        return {"ok": True, "user": about.get("user")}
    except Exception as e:
        import traceback
        return JSONResponse(status_code=500, content={"ok": False, "error": str(e), "trace": traceback.format_exc()})

@app.get("/drive/list")
def drive_list(folder_id: Optional[str] = None):
    fid = folder_id or os.getenv("DRIVE_FOLDER_ID")
    if not fid:
        raise HTTPException(400, "No folder id; set DRIVE_FOLDER_ID or pass ?folder_id=")
    try:
        svc = get_drive_service()
        files = list_pdfs_in_folder(svc, fid)
        return {"ok": True, "count": len(files), "sample": files[:5]}
    except Exception as e:
        import traceback
        return JSONResponse(status_code=500, content={"ok": False, "error": str(e), "trace": traceback.format_exc()})

@app.post("/drive/sync")
def drive_sync(
    folder_id: Optional[str] = None,
    limit: int = 10,          # <= small batch while testing
    request: Request = None
):
    try:
        fid = folder_id or os.getenv("DRIVE_FOLDER_ID")
        if not fid:
            raise HTTPException(400, "Provide folder_id or set DRIVE_FOLDER_ID")

        token = os.getenv("SYNC_TOKEN")
        if token and request.headers.get("X-Sync-Token") != token:
            raise HTTPException(403, "Forbidden")

        svc = get_drive_service()
        files = list_pdfs_in_folder(svc, fid)
        files = files[: max(1, int(limit))]  # small slice to avoid timeouts

        results = []
        for f in files:
            local = DATA_DIR / safe_name(f["name"])
            download_pdf(svc, f["id"], local)
            parsed = extract_pds(str(local))
            (PARSED_DIR / (local.stem + ".json")).write_text(
                json.dumps(parsed, ensure_ascii=False, indent=2), encoding="utf-8"
            )
            name = (parsed.get("product_name_line") or local.stem).strip()
            if name:
                _save_index_entry(name, local)
            results.append({"name": name, "stored_as": str(local)})

        return {
            "processed": len(results),
            "items": results,
            "note": "Increase ?limit=… later or run nightly to index everything."
        }
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        return JSONResponse(status_code=500, content={"ok": False, "error": str(e), "trace": traceback.format_exc()})

# ---------- Name-based endpoints for Stammer ----------
def _resolve_by_name(name: str) -> Path:
    idx = _load_index()
    if not idx:
        raise HTTPException(404, "Index is empty; run /drive/sync first")
    lower = {k.lower(): v for k, v in idx.items()}
    if name.lower() in lower:
        return Path(lower[name.lower()])
    for k, v in idx.items():
        if name.lower() in k.lower():
            return Path(v)
    raise HTTPException(404, f"Could not find a PDF for '{name}'")

@app.post("/summary/by-name")
def summary_by_name(req: NameReq):
    pA = _resolve_by_name(req.product_a_name)
    A = extract_pds(str(pA))
    md = (
        f"**Product:** {A.get('product_name_line')}\n\n"
        f"**Revision:** {A.get('version')}\n\n"
        f"**Approvals / Specifications:**\n- " +
        "; ".join(A.get('approvals_and_specs', [])) + "\n\n" +
        "**Typical properties:**\n" +
        "\n".join([f"- {p['property_name']}: {p['value']} ({p['test_method']})"
                   for p in A.get('typical_properties', [])])
    )
    return {"reply_markdown": md, "productA": A}

@app.post("/compare/by-name")
def compare_by_name(req: NameReq):
    if not req.product_b_name:
        raise HTTPException(400, "Provide product_b_name for comparison")
    pA = _resolve_by_name(req.product_a_name)
    pB = _resolve_by_name(req.product_b_name)
    A = extract_pds(str(pA)); B = extract_pds(str(pB))

    import re
    norm = lambda s: re.sub(r"\s+", " ", s.strip()).lower()
    mapB = {norm(p["property_name"]): p for p in B.get("typical_properties", [])}
    nameA = A.get("product_name_line") or pA.name
    nameB = B.get("product_name_line") or pB.name
    verA  = A.get("version") or ""; verB = B.get("version") or ""
    lines = [
        f"**Sammenligning:** {nameA} (Rev. {verA}) vs {nameB} (Rev. {verB})", "",
        f"| Egenskap | {nameA} | {nameB} |", "|---|---|---|"
    ]
    for p in A.get("typical_properties", []):
        key = norm(p["property_name"]); q = mapB.get(key)
        valA = p["value"]; valB = q["value"] if q else "—"
        methA = f" ({p['test_method']})" if p.get("test_method") else ""
        methB = f" ({q['test_method']})" if q and q.get("test_method") else ""
        lines.append(f"| {p['property_name']} | {valA}{methA} | {valB}{methB} |")
    lines += ["", "**Godkjenninger / spesifikasjoner:**"]
    if A.get("approvals_and_specs"): lines.append("- " + nameA + ": " + "; ".join(A["approvals_and_specs"]))
    if B.get("approvals_and_specs"): lines.append("- " + nameB + ": " + "; ".join(B["approvals_and_specs"]))
    return {"reply_markdown": "\n".join(lines), "productA": A, "productB": B}
