# pds_extractor.py
import re
from pathlib import Path
from typing import List, Dict, Any, Optional
from PyPDF2 import PdfReader

# ---------- helpers ----------

def _read_text(pdf_path: str) -> str:
    reader = PdfReader(pdf_path)
    texts = []
    for page in reader.pages:
        try:
            texts.append(page.extract_text() or "")
        except Exception:
            texts.append("")
    # Normalize whitespace & some symbols so regex is simpler
    raw = "\n".join(texts)
    raw = raw.replace("\r", "\n")
    raw = re.sub(r"[ \t]+", " ", raw)
    raw = re.sub(r"\n{3,}", "\n\n", raw)
    # unify various minus/degree/superscripts and units
    raw = raw.replace("º", "°").replace("–", "-").replace("—", "-").replace("²", "2")
    return raw

def _first(lines: List[str], pattern: re.Pattern, default: Optional[str] = None) -> Optional[str]:
    for ln in lines:
        m = pattern.search(ln)
        if m:
            return m.group(1).strip()
    return default

def _norm(s: str) -> str:
    s = (s or "").strip()
    s = re.sub(r"\s+", " ", s)
    return s

def _section_after(text: str, anchors: List[re.Pattern], stop_markers: List[re.Pattern], max_chars: int = 1500) -> Optional[str]:
    """
    Find a section that starts at the first anchor match and ends at the first stop marker or after max_chars.
    """
    start = None
    for pat in anchors:
        m = pat.search(text)
        if m:
            start = m.end()
            break
    if start is None:
        return None

    end = len(text)
    snippet = text[start:start + max_chars]
    # stop at next header-like marker or named section
    for pat in stop_markers:
        m2 = pat.search(snippet)
        if m2:
            end = start + m2.start()
            break
    return text[start:end]

def _split_items(blob: str) -> List[str]:
    # split by newlines, bullets, semicolons, commas – keep compact items
    parts = re.split(r"[;\n•\-]\s*", blob)
    items = []
    for p in parts:
        p = _norm(p)
        if p:
            items.append(p)
    return items

# ---------- extractors ----------

def _extract_product_name(text: str, pdf_path: str) -> Optional[str]:
    """
    Try multiple strategies:
      1) Look for a line starting with 'Valvoline' within the first ~1000 chars.
      2) PDF metadata title (if present).
      3) Fallback: filename stem.
    """
    head = text[:2000]
    # Lines that look like a product heading
    for ln in head.splitlines():
        s = ln.strip()
        if not s:
            continue
        if s.lower().startswith("valvoline"):
            # trim trailing codes if the next line is 'Typical properties' etc.
            return _norm(s)

    # PDF metadata title
    try:
        reader = PdfReader(pdf_path)
        meta = reader.metadata or {}
        title = (meta.get("/Title") or meta.get("Title") or "").strip()
    except Exception:
        title = ""
    if title:
        return _norm(title)

    # Fallback: filename stem
    stem = Path(pdf_path).stem
    return _norm(stem)

def _extract_version(text: str) -> Optional[str]:
    # Common patterns: Revision, Rev., Version, issue codes like 306/06b etc.
    pats = [
        re.compile(r"(?:Revision|Rev\.?|Version)\s*[: ]\s*([A-Za-z0-9./ -]{2,})", re.I),
        re.compile(r"\b(\d{3}/\d+[A-Za-z]?)\b"),
        re.compile(r"\b(\d{2,4}/\d{1,2}[A-Za-z]?)\b"),
    ]
    for p in pats:
        m = p.search(text)
        if m:
            return _norm(m.group(1).replace(" / ", "/").replace("  ", " "))
    return None

def _extract_approvals(text: str) -> List[str]:
    """
    Motorcycle PDS often uses headings like 'Specifications', 'Performance', 'Meets requirements',
    or lists API/JASO directly. We gather from a section starting at any of these anchors.
    """
    anchors = [
        re.compile(r"Approvals?\s*&?\s*/?\s*Specifications?", re.I),
        re.compile(r"Specifications?", re.I),
        re.compile(r"Performance(?: levels?)?", re.I),
        re.compile(r"Meets (?:or exceeds )?the requirements of", re.I),
        re.compile(r"\b(API|ACEA|ILSAC|JASO|VW|MB|BMW|FORD|GM|DEXOS)\b", re.I),
    ]
    stops = [
        re.compile(r"Typical (?:properties|characteristics|values|data)", re.I),
        re.compile(r"Typical", re.I),
        re.compile(r"Health|Safety|Handling", re.I),
        re.compile(r"Storage", re.I),
        re.compile(r"\n[A-Z ]{4,}\n"),  # next all-caps header
    ]
    blob = _section_after(text, anchors, stops, max_chars=1500)
    if not blob:
        # fallback: scan entire text for obvious approvals tokens
        blob = text[:3000]

    # Keep only lines that look like approvals/specs (contain known tokens)
    keep_tokens = re.compile(r"\b(API|ACEA|ILSAC|JASO|VW|MB|BMW|FORD|GM|DEXOS|PSA|FIAT|RENAULT|VOLVO|MAN|CUMMINS|ALLISON)\b", re.I)
    items = []
    for line in _split_items(blob):
        if keep_tokens.search(line):
            items.append(line)

    # unique while preserving order
    seen = set()
    out = []
    for it in items:
        key = it.lower()
        if key not in seen:
            seen.add(key)
            out.append(it)
    return out

def _extract_typical_properties(text: str) -> List[Dict[str, Any]]:
    """
    Extract rows like 'Viscosity , mm2/s @ 100 ºC.: 17,5 (ASTM D-445)'
    Keep the original value string (commas or dots).
    """
    # Narrow to typical properties section if we can find it
    anchors = [
        re.compile(r"Typical (?:properties|characteristics|values|data)", re.I),
    ]
    stops = [
        re.compile(r"Approvals?|Specifications?|Performance", re.I),
        re.compile(r"Health|Safety|Handling|Storage", re.I),
        re.compile(r"\n[A-Z ]{4,}\n"),
    ]
    section = _section_after(text, anchors, stops, max_chars=3000) or text

    lines = [ln.strip() for ln in section.splitlines() if ln.strip()]
    props: List[Dict[str, Any]] = []
    ordinal = 1

    # Regex that captures: name ... : value (ASTM D-xxx)
    row_re = re.compile(
        r"""^
            (?P<name>[A-Za-z].*?)            # property name
            \s*[:\.]\s*
            (?P<value>[<>]?\s*[\d\.,]+(?:\s*[A-Za-zµ%°/.\-\s]+)?) # value w/ units
            (?:\s*\(\s*(?P<method>ASTM[^)]*)\))? # optional (ASTM ...)
        $""",
        re.X
    )

    for ln in lines:
        m = row_re.match(ln)
        if not m:
            continue
        name = _norm(m.group("name"))
        val = _norm(m.group("value"))
        method = _norm(m.group("method") or "")
        # small cleanups
        name = name.replace("mm2/s", "mm2/s").replace("mm²/s", "mm2/s")
        if method and not method.upper().startswith("ASTM"):
            method = method  # keep as-is; rarely other standards
        props.append({
            "ordinal": ordinal,
            "property_name": name,
            "test_method": method if method else None,
            "value": val
        })
        ordinal += 1

    # If nothing parsed, try a looser grab for a few well-known keys
    if not props:
        loose = [
            ("Viscosity", re.compile(r"(Viscosity[^:\n]+)[:\.]\s*([^\n]+)")),
            ("Viscosity Index", re.compile(r"(Viscosity Index)[:\.]\s*([^\n]+)")),
            ("Pour Point", re.compile(r"(Pour Point)[^:\n]*[:\.]\s*([^\n]+)")),
            ("Flash Point", re.compile(r"(Flash Point)[^:\n]*[:\.]\s*([^\n]+)")),
            ("Specific Gravity", re.compile(r"(Specific Gravity[^:\n]*)[:\.]\s*([^\n]+)")),
            ("TBN", re.compile(r"(TBN[^:\n]*)[:\.]\s*([^\n]+)")),
        ]
        for label, rx in loose:
            m = rx.search(section)
            if m:
                props.append({
                    "ordinal": ordinal,
                    "property_name": _norm(m.group(1)),
                    "test_method": None,
                    "value": _norm(m.group(2)),
                })
                ordinal += 1

    return props

# ---------- public API ----------

def extract_pds(pdf_path: str) -> Dict[str, Any]:
    text = _read_text(pdf_path)
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]

    product = _extract_product_name(text, pdf_path)
    version = _extract_version(text)
    approvals = _extract_approvals(text)
    props = _extract_typical_properties(text)

    # normalize ordering indexes
    for i, row in enumerate(props, 1):
        row["ordinal"] = i

    return {
        "pdf": pdf_path,
        "product_name_line": product,
        "version": version,
        "approvals_and_specs": approvals,
        "typical_properties": props,
    }
