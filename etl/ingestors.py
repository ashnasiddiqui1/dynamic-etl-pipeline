import pandas as pd
import json
from docx import Document
import PyPDF2
from .utils import safe_read_text

def ingest_csv(file):
    try:
        file.seek(0)
        return pd.read_csv(file).to_dict(orient="records")
    except Exception as e:
        return [{"_ingest_error": str(e)}]

def ingest_json(file):
    try:
        file.seek(0)
        return json.load(file)
    except Exception as e:
        return [{"_ingest_error": str(e)}]

def ingest_pdf(file):
    try:
        file.seek(0)
        reader = PyPDF2.PdfReader(file)
        return [{"content": p.extract_text() or ""} for p in reader.pages]
    except Exception as e:
        return [{"_ingest_error": str(e)}]

def ingest_docx(file):
    try:
        doc = Document(file)
        return [{"content": p.text} for p in doc.paragraphs if p.text.strip()]
    except Exception as e:
        return [{"_ingest_error": str(e)}]

def ingest_xml(file):
    try:
        import xml.etree.ElementTree as ET
        file.seek(0)
        tree = ET.parse(file)
        root = tree.getroot()
        return [{**node.attrib, "_text": node.text or ""} for node in root]
    except Exception as e:
        return [{"_ingest_error": str(e)}]

def ingest_txt(file):
    try:
        file.seek(0)
        lines = safe_read_text(file)
        return [{"content": line} for line in lines if line.strip()]
    except Exception as e:
        return [{"_ingest_error": str(e)}]

def ingest_file(file, filename):
    ext = filename.split(".")[-1].lower()
    if ext == "csv": return ingest_csv(file)
    if ext == "json": return ingest_json(file)
    if ext == "pdf": return ingest_pdf(file)
    if ext == "docx": return ingest_docx(file)
    if ext == "xml": return ingest_xml(file)
    if ext == "txt": return ingest_txt(file)
    return [{"_ingest_error": "Unsupported file"}]
