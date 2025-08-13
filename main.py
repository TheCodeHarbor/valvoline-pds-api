from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import Optional, Literal
import httpx, uuid
from pathlib import Path
from pds_extractor import extract_pds

app = FastAPI(title="Valvoline PDS MVP")
DATA_DIR = Path("data"); DATA_DIR.mkdir(exist_ok=True)

class AnswerReq(BaseModel):
    product_a_url: Optional[str] = None
    product_b_url: Optional[str] = None
    product_a_file: Optional[str] = None
    product_b_file: Optional[str] = None
    locale: Literal["no","en"] = "no"
    expected_output: Literal["summary","comparison"] = "summary"

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

    # Summary
    if req.expected_output == "summary" or not (req.product_b_file or req.product_b_url):
        md = f"**Product:** {A.get('product_name_line')}\n\n" \
             f"**Revision:** {A.get('version')}\n\n" \
             f"**Approvals / Specifications:**\n- " + "; ".join(A.get('approvals_and_specs', [])) + "\n\n" \
             f"**Typical properties:**\n" + "\n".join([f"- {p['property_name']}: {p['value']} ({p['test_method']})" for p in A.get('typical_properties', [])])
        return JSONResponse({"reply_markdown": md, "productA": A})

    # Comparison
    if req.product_b_file:
        pB = Path(req.product_b_file)
        if not pB.exists():
            raise HTTPException(400, "product_b_file not found")
    elif req.product_b_url:
        pB = await _download_pdf_to_disk(req.product_b_url)
    else:
        raise HTTPException(400, "Provide product_b_file or product_b_url")
    B = extract_pds(str(pB))

    def norm(s: str):
        import re
        return re.sub(r"\s+", " ", s.strip()).lower()

    mapB = { norm(p["property_name"]): p for p in B.get("typical_properties", []) }
    nameA = A.get("product_name_line") or Path(A["pdf"]).name
    nameB = B.get("product_name_line") or Path(B["pdf"]).name
    verA  = A.get("version") or ""; verB = B.get("version") or ""

    lines = [f"**Sammenligning:** {nameA} (Rev. {verA}) vs {nameB} (Rev. {verB})", "",
             f"| Egenskap | {nameA} | {nameB} |", "|---|---|---|"]
    for p in A.get("typical_properties", []):
        key = norm(p["property_name"]); q = mapB.get(key)
        valA = p["value"]; valB = q["value"] if q else "—"
        methA = f" ({p['test_method']})" if p.get("test_method") else ""
        methB = f" ({q['test_method']})" if q and q.get("test_method") else ""
        lines.append(f"| {p['property_name']} | {valA}{methA} | {valB}{methB} |")

    lines += ["", "**Godkjenninger / spesifikasjoner:**"]
    if A.get("approvals_and_specs"): lines.append("- " + nameA + ": " + "; ".join(A["approvals_and_specs"]))
    if B.get("approvals_and_specs"): lines.append("- " + nameB + ": " + "; ".join(B["approvals_and_specs"]))
    return JSONResponse({"reply_markdown": "\n".join(lines), "productA": A, "productB": B})
