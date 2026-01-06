
# Step 5 (Complete): Create a reusable CCDA parser package in the Notebook filesystem

from pathlib import Path

pkg_dir = Path("ccda_parser")
pkg_dir.mkdir(exist_ok=True)

# --------------------------
# __init__.py
# --------------------------
(pkg_dir / "__init__.py").write_text(
    '''"""
Reusable C-CDA Parser

Parses CCDA XML (urn:hl7-org:v3) into tidy pandas DataFrames for Medications,
Diagnostic Results, Problems, Procedures, Encounters, Vital Signs, Immunizations,
and Functional Status.
"""
__version__ = "0.1.0"
''',
    encoding="utf-8"
)

# --------------------------
# parser.py — core utilities + extractors
# --------------------------
parser_py = r'''
import xml.etree.ElementTree as ET
from datetime import datetime
import pandas as pd

# HL7 namespaces
ns = {"cda": "urn:hl7-org:v3", "sdtc": "urn:hl7-org:sdtc"}

# -------- Utilities --------
def parse_hl7_ts(ts: str):
    """
    Convert HL7 TS (YYYYMMDDHHMMSS...) to Python datetime or None.
    Keeps only the first 14 chars YYYYMMDDHHMMSS if longer.
    """
    if not ts:
        return None
    ts14 = ts[:14]
    try:
        return datetime.strptime(ts14, "%Y%m%d%H%M%S")
    except Exception:
        return None

def to_iso(ts: str) -> str:
    """Return ISO 8601 string or '' if ts invalid."""
    dt = parse_hl7_ts(ts)
    return dt.isoformat() if dt else ""

def _gettext(el):
    """Safe text extraction."""
    return (el.text or "").strip() if el is not None else ""

def validate_ccda_xml(text: str):
    """
    Lightweight validation: well-formed XML and the root element is ClinicalDocument.
    Returns (ok: bool, message: str).
    """
    try:
        root = ET.fromstring(text)
    except Exception as e:
        return False, f"XML parse failed: {e}"
    if not root.tag.endswith("ClinicalDocument"):
        return False, "Root element is not ClinicalDocument."
    return True, "OK"

def discover_sections(root):
    """Return list of dicts with title, code, codeSystem, and the section element."""
    sections = []
    for sec in root.findall(".//cda:structuredBody/cda:component/cda:section", ns):
        title = _gettext(sec.find("cda:title", ns))
        code_el = sec.find("cda:code", ns)
        sec_code = code_el.get("code") if code_el is not None else None
        sec_code_system = code_el.get("codeSystem") if code_el is not None else None
        sections.append({"title": title, "code": sec_code, "codeSystem": sec_code_system, "el": sec})
    return sections

# -------- Extractors --------
def extract_medications(sections):
    """
    Returns DataFrame columns:
    file_name (to be added upstream), start_raw, stop_raw, start_iso, stop_iso,
    description, code_system, code
    """
    rows = []
    for s in sections:
        if s["title"].lower().startswith("medication"):
            sec = s["el"]
            for entry in sec.findall(".//cda:entry", ns):
                sa = entry.find(".//cda:substanceAdministration", ns)
                if sa is None:
                    continue
                start = stop = ""
                low = sa.find(".//cda:low", ns)
                high = sa.find(".//cda:high", ns)
                if low is not None:  start = low.get("value","")
                if high is not None: stop  = high.get("value","")
                desc, code_system, code_val = "", "", ""
                prod_code = sa.find(".//cda:consumable//cda:manufacturedProduct//cda:manufacturedMaterial//cda:code", ns)
                if prod_code is not None:
                    desc = prod_code.get("displayName","") or _gettext(prod_code.find("cda:originalText", ns))
                    code_system = prod_code.get("codeSystemName","") or prod_code.get("codeSystem","")
                    code_val = prod_code.get("code","")
                else:
                    name_el = sa.find(".//cda:consumable//cda:manufacturedProduct//cda:manufacturedMaterial//cda:name", ns)
                    desc = _gettext(name_el)
                rows.append({
                    "start_raw": start, "stop_raw": stop,
                    "start_iso": to_iso(start), "stop_iso": to_iso(stop),
                    "description": desc, "code_system": code_system, "code": code_val
                })
    return pd.DataFrame(rows)

def extract_results(sections):
    """Diagnostic Results: start_raw/start_iso, description, LOINC code, value, unit"""
    rows = []
    for s in sections:
        if "result" in s["title"].lower():
            sec = s["el"]
            for obs in sec.findall(".//cda:observation", ns):
                code = obs.find("cda:code", ns)
                desc = code.get("displayName","") if code is not None else ""
                loinc = code.get("code","") if code is not None else ""
                start = ""
                eff = obs.find("cda:effectiveTime", ns)
                if eff is not None:
                    low = eff.find("cda:low", ns)
                    if low is not None: start = low.get("value","")
                val_el = obs.find("cda:value", ns)
                value = ""; unit = ""
                if val_el is not None:
                    value = val_el.get("value",""); unit = val_el.get("unit","")
                rows.append({
                    "start_raw": start, "start_iso": to_iso(start),
                    "description": desc, "code_system": "LOINC", "code": loinc,
                    "value": value, "unit": unit
                })
    return pd.DataFrame(rows)

def extract_problems(sections):
    """Problems: start/stop raw+iso, description, code_system (SNOMED), code"""
    rows = []
    for s in sections:
        if "problem" in s["title"].lower():
            sec = s["el"]
            for obs in sec.findall(".//cda:observation", ns):
                start = stop = ""
                eff = obs.find("cda:effectiveTime", ns)
                if eff is not None:
                    low = eff.find("cda:low", ns); high = eff.find("cda:high", ns)
                    start = low.get("value","") if low is not None else ""
                    stop  = high.get("value","") if high is not None else ""
                val_code = obs.find("cda:value", ns)
                desc = val_code.get("displayName","") if val_code is not None else ""
                code_val = val_code.get("code","") if val_code is not None else ""
                code_system = val_code.get("codeSystemName","") if val_code is not None else ""
                rows.append({
                    "start_raw": start, "stop_raw": stop,
                    "start_iso": to_iso(start), "stop_iso": to_iso(stop),
                    "description": desc, "code_system": code_system, "code": code_val
                })
    return pd.DataFrame(rows)

def extract_procedures(sections):
    """Procedures/Surgeries: start/stop raw+iso, description, code_system, code"""
    rows = []
    for s in sections:
        if "procedure" in s["title"].lower() or "surger" in s["title"].lower():
            sec = s["el"]
            for proc in sec.findall(".//cda:procedure", ns):
                start = stop = ""
                eff = proc.find("cda:effectiveTime", ns)
                if eff is not None:
                    low = eff.find("cda:low", ns); high = eff.find("cda:high", ns)
                    start = low.get("value","") if low is not None else ""
                    stop  = high.get("value","") if high is not None else ""
                code_el = proc.find("cda:code", ns)
                desc = code_el.get("displayName","") if code_el is not None else ""
                code_val = code_el.get("code","") if code_el is not None else ""
                code_system = code_el.get("codeSystemName","") if code_el is not None else ""
                rows.append({
                    "start_raw": start, "stop_raw": stop,
                    "start_iso": to_iso(start), "stop_iso": to_iso(stop),
                    "description": desc, "code_system": code_system, "code": code_val
                })
    return pd.DataFrame(rows)

def extract_encounters(sections):
    """Encounters: start/stop raw+iso, description, code_system, code"""
    rows = []
    for s in sections:
        if "encounter" in s["title"].lower():
            sec = s["el"]
            for enc in sec.findall(".//cda:encounter", ns):
                start = stop = ""
                eff = enc.find("cda:effectiveTime", ns)
                if eff is not None:
                    low = eff.find("cda:low", ns); high = eff.find("cda:high", ns)
                    start = low.get("value","") if low is not None else ""
                    stop  = high.get("value","") if high is not None else ""
                code_el = enc.find("cda:code", ns)
                desc = code_el.get("displayName","") if code_el is not None else ""
                code_val = code_el.get("code","") if code_el is not None else ""
                code_system = code_el.get("codeSystemName","") if code_el is not None else ""
                rows.append({
                    "start_raw": start, "stop_raw": stop,
                    "start_iso": to_iso(start), "stop_iso": to_iso(stop),
                    "description": desc, "code_system": code_system, "code": code_val
                })
    return pd.DataFrame(rows)

def extract_vitals(sections):
    """Vitals: start_raw/start_iso, description, LOINC code, value, unit"""
    rows = []
    for s in sections:
        if "vital" in s["title"].lower():
            sec = s["el"]
            for obs in sec.findall(".//cda:observation", ns):
                code_el = obs.find("cda:code", ns)
                desc = code_el.get("displayName","") if code_el is not None else ""
                loinc = code_el.get("code","") if code_el is not None else ""
                start = ""
                eff = obs.find("cda:effectiveTime", ns)
                if eff is not None:
                    low = eff.find("cda:low", ns)
                    start = low.get("value","") if low is not None else ""
                val_el = obs.find("cda:value", ns)
                value = ""; unit = ""
                if val_el is not None:
                    value = val_el.get("value",""); unit = val_el.get("unit","")
                rows.append({
                    "start_raw": start, "start_iso": to_iso(start),
                    "description": desc, "code_system": "LOINC", "code": loinc,
                    "value": value, "unit": unit
                })
    return pd.DataFrame(rows)

def extract_immunizations(sections):
    """Immunizations: start_raw/start_iso, description, code_system (CVX), code"""
    rows = []
    for s in sections:
        if "immunization" in s["title"].lower():
            sec = s["el"]
            for sa in sec.findall(".//cda:substanceAdministration", ns):
                start = ""
                eff = sa.find("cda:effectiveTime", ns)
                if eff is not None:
                    low = eff.find("cda:low", ns)
                    start = low.get("value","") if low is not None else ""
                prod_code = sa.find(".//cda:consumable//cda:manufacturedProduct//cda:manufacturedMaterial//cda:code", ns)
                desc = prod_code.get("displayName","") if prod_code is not None else ""
                code_val = prod_code.get("code","") if prod_code is not None else ""
                code_system = prod_code.get("codeSystemName","") if prod_code is not None else ""
                rows.append({
                    "start_raw": start, "start_iso": to_iso(start),
                    "description": desc, "code_system": code_system, "code": code_val
                })
    return pd.DataFrame(rows)

def extract_functional_status(sections):
    """Functional Status: start_raw/start_iso, description, LOINC code, value, unit"""
    rows = []
    for s in sections:
        if "functional" in s["title"].lower():
            sec = s["el"]
            for obs in sec.findall(".//cda:observation", ns):
                code_el = obs.find("cda:code", ns)
                desc = code_el.get("displayName","") if code_el is not None else ""
                code_val = code_el.get("code","") if code_el is not None else ""
                value_el = obs.find("cda:value", ns)
                value = ""; unit = ""
                if value_el is not None:
                    value = value_el.get("value",""); unit = value_el.get("unit","")
                start = ""
                eff = obs.find("cda:effectiveTime", ns)
                if eff is not None:
                    low = eff.find("cda:low", ns)
                    start = low.get("value","") if low is not None else ""
                rows.append({
                    "start_raw": start, "start_iso": to_iso(start),
                    "description": desc, "code_system": "LOINC", "code": code_val,
                    "value": value, "unit": unit
                })
    return pd.DataFrame(rows)
'''
(pkg_dir / "parser.py").write_text(parser_py, encoding="utf-8")

# --------------------------
# parse_text.py — entrypoint to parse raw XML text
# --------------------------
parse_text_py = r'''
import xml.etree.ElementTree as ET
import pandas as pd
from .parser import (
    validate_ccda_xml, discover_sections,
    extract_medications, extract_results, extract_problems,
    extract_procedures, extract_encounters, extract_vitals,
    extract_immunizations, extract_functional_status
)

def parse_ccda_text(text: str):
    """
    Parse CCDA XML from a text string.
    Returns (dfs: dict[str, DataFrame], meta: dict with status/reason/counts).
    """
    ok, msg = validate_ccda_xml(text)
    if not ok:
        return {}, {"status": "rejected", "reason": msg}

    try:
        root = ET.fromstring(text)
    except Exception as e:
        return {}, {"status": "rejected", "reason": f"XML parse error: {e}"}

    sections = discover_sections(root)
    if not sections:
        return {}, {"status": "rejected", "reason": "No sections found in structuredBody."}

    dfs = {
        "medications":       extract_medications(sections),
        "results":           extract_results(sections),
        "problems":          extract_problems(sections),
        "procedures":        extract_procedures(sections),
        "encounters":        extract_encounters(sections),
        "vitals":            extract_vitals(sections),
        "immunizations":     extract_immunizations(sections),
        "functional_status": extract_functional_status(sections),
    }
    meta = {
        "status": "parsed",
        "reason": "OK",
        "sections_found": [s["title"] for s in sections],
        "counts": {k: len(v) for k, v in dfs.items()},
    }
    return dfs, meta
'''
(pkg_dir / "parse_text.py").write_text(parse_text_py, encoding="utf-8")

print("✅ Parser package created at:", pkg_dir.resolve())

from ccda_parser import __version__