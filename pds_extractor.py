import re
from pathlib import Path
from typing import Dict, List
import PyPDF2  # type: ignore

HEADING_APPROVALS = r"Approvals\s*&\s*Specifications"
HEADING_TYPICAL = r"Typical\s+propert(?:y|ies)"
SECTION_HEADINGS = [
    HEADING_APPROVALS,
    HEADING_TYPICAL,
    r"Applications",
    r"Features\s*&\s*Benefits",
    r"Health\s*&\s*Safety",
    r"Storage",
    r"Protect the environment",
    r"Keeping the world moving since 1866",
    r"This information only applies",
    r"Replaces\s*[–-]",
    r"Version:",
]

PROP_NAME_PAT = re.compile(
    r"(Viscosity|TBN|Pour Point|Specific Gravity|Flash Point|Viscosity Index|Noack|Sulfated Ash|HTHS|CCS|MRV|Density|Kinematic Viscosity|Brookfield)",
    re.I
)
METHOD_PAT = re.compile(r"\b(ASTM|DIN|ISO|CEC|SAE|IP)\b", re.I)
NORMATIVE_PAT = re.compile(
    r"\b(ACEA|API|ILSAC|JASO|MB|Mercedes|VW|VAG|BMW|Ford|GM|dexos|Renault|RN0?\d{3,4}|PSA|Fiat|Chrysler|Volvo|Porsche|Opel|Vauxhall|Peugeot|Citro[eë]n)\b",
    re.I
)
METHOD_VALUE_RE = re.compile(
    r"^(?P<method>(?:ASTM|DIN|ISO|CEC|SAE|IP)\s*[A-Za-z]*\s*-?\d+(?:\.\d+)?(?:\/-?\d+(?:\.\d+)?)?)\s*(?P<value>.+)$",
    re.I
)

def _extract_pages_text(pdf_path: str) -> List[str]:
    reader = PyPDF2.PdfReader(pdf_path)
    pages = []
    for page in reader.pages:
        txt = page.extract_text() or ""
        txt = re.sub(r"[ \t]+", " ", txt).replace("\u00A0", " ")
        txt = "\n".join(line.strip() for line in txt.splitlines())
        pages.append(txt)
    return pages

def _find_section(full_text: str, heading_regex: str, stop_regexes: List[str]) -> str | None:
    m = re.search(heading_regex, full_text, flags=re.IGNORECASE)
    if not m:
        return None
    start = m.end()
    ends = []
    for h in stop_regexes:
        mm = re.search(h, full_text[start:], flags=re.IGNORECASE)
        if mm:
            ends.append(start + mm.start())
    end = min(ends) if ends else len(full_text)
    return full_text[start:end].strip()

def _parse_approvals(section_text: str | None) -> List[str]:
    if not section_text:
        return []
    lines = [ln.strip(" -*•\t") for ln in section_text.splitlines() if ln.strip()]
    keepers = []
    for ln in lines:
        if re.search(r"^Meets requirements of", ln, re.I): continue
        if re.search(r"^Recommended for use", ln, re.I):
            keepers.append(ln); continue
        if NORMATIVE_PAT.search(ln):
            keepers.append(ln); continue
        if re.search(r"\b\d{3}\.\d+\b", ln) or re.search(r"\b\d{3}\s?0{2}(?:/\d{3}\s?0{2})?\b", ln):
            keepers.append(ln); continue
    return [ln for ln in keepers if ln.lower() != "is specified"]

def _split_method_value(line: str) -> tuple[str, str | None]:
    m = METHOD_VALUE_RE.match(line.strip())
    if not m:
        return line.strip(), None
    method = re.sub(r"\s*-\s*", "-", m.group("method"))
    value = m.group("value").strip()
    return method, value

def _parse_typical_properties(section_text: str | None):
    if not section_text:
        return []
    text = re.sub(r"Typical property characteristics.*?may occur\.\s*", "", section_text, flags=re.I | re.S)
    lines = [re.sub(r"\s+", " ", ln.strip()) for ln in text.splitlines() if ln.strip()]
    lines = [ln for ln in lines if not ln.lower().startswith(("synpower", "synpower env", "sae viscosity grade"))]
    props, i, ordinal = [], 0, 1
    while i < len(lines) - 1:
        name, nxt = lines[i], lines[i + 1]
        if PROP_NAME_PAT.search(name) and METHOD_PAT.search(nxt):
            method, value = _split_method_value(nxt)
            if value:
                props.append({"ordinal": ordinal, "property_name": name, "test_method": method, "value": value})
                ordinal += 1; i += 2; continue
        i += 1
    return props

def extract_pds(pdf_path: str) -> Dict:
    pages = _extract_pages_text(pdf_path)
    full_text = "\n".join(pages)
    approvals_text = _find_section(full_text, HEADING_APPROVALS, [h for h in SECTION_HEADINGS if h != HEADING_APPROVALS])
    typical_text   = _find_section(full_text, HEADING_TYPICAL,    [h for h in SECTION_HEADINGS if h != HEADING_TYPICAL])
    approvals = _parse_approvals(approvals_text)
    properties = _parse_typical_properties(typical_text)
    mver = re.search(r"Version:\s*([^\n]+)", full_text, re.I)
    version = mver.group(1).strip() if mver else None
    product_line = None
    for ln in (pages[0].splitlines() if pages else []):
        if "Valvoline" in ln and "SynPower" in ln:
            product_line = ln.strip(); break
    return {
        "pdf": pdf_path,
        "product_name_line": product_line,
        "version": version,
        "approvals_and_specs": approvals,
        "typical_properties": properties,
    }
