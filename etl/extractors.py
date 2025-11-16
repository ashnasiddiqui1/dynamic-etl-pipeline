import re
import chardet

EMAIL_RE = re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b")
PHONE_RE = re.compile(r"(?:\+?\d[\d\s\-()]{7,}\d)")
DATE_RE = re.compile(r"\b\d{1,2}[-/]\d{1,2}[-/]\d{2,4}\b")
NUMBER_RE = re.compile(r"-?\d+\.?\d*")

def extract_patterns(text):
    if not isinstance(text, str):
        return {"emails": [], "phones": [], "dates": [], "numbers": []}
    return {
        "emails": EMAIL_RE.findall(text),
        "phones": PHONE_RE.findall(text),
        "dates": DATE_RE.findall(text),
        "numbers": NUMBER_RE.findall(text),
    }

def flatten_json(data, prefix=""):
    out = {}
    if isinstance(data, dict):
        for k, v in data.items():
            out.update(flatten_json(v, f"{prefix}.{k}" if prefix else k))
    elif isinstance(data, list):
        for i, v in enumerate(data):
            out.update(flatten_json(v, f"{prefix}[{i}]"))
    else:
        out[prefix] = data
    return out

def safe_read_text(file):
    data = file.read()
    if isinstance(data, bytes):
        try:
            return data.decode("utf-8").splitlines()
        except:
            enc = chardet.detect(data)["encoding"] or "utf-8"
            return data.decode(enc, errors="replace").splitlines()
    return data.splitlines()
