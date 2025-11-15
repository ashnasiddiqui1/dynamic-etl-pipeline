# app.py

import streamlit as st
import pandas as pd
import json
import sqlite3
import os
import re
import datetime
from pathlib import Path
from genson import SchemaBuilder
from docx import Document
import PyPDF2

import chardet

st.set_page_config(
    page_title="Unified Dynamic ETL Pipeline",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Optional: CSS for slicker dark look and centered content
st.markdown("""
    <style>
        .main {background-color: #16181d;}
        section[data-testid="stFileUploader"] {margin-bottom: 1.5rem;}
        div[data-testid="stToolbar"] {display: none;}
        h1 {text-align: center;}
    </style>
""", unsafe_allow_html=True)


def safe_read_text(file):
    """Try to decode a file as UTF-8, else detect encoding or fallback; returns list of lines as strings."""
    try:
        text = file.read()
        if isinstance(text, str):
            # Already string
            return text.splitlines()
        elif isinstance(text, bytes):
            try:
                # Try UTF-8 first
                return text.decode('utf-8').splitlines()
            except UnicodeDecodeError:
                # Fall back to chardet detection
                encoding = chardet.detect(text)['encoding'] or 'utf-8'
                try:
                    return text.decode(encoding, errors='replace').splitlines()
                except Exception:
                    # Fallback: decode as ascii ignoring errors
                    return text.decode('ascii', errors='ignore').splitlines()
        else:
            return []
    except Exception as e:
        return []


# --- SQLite DB Helpers ---

DB_FILE = "etl_data.db"


def init_db():
    with sqlite3.connect(DB_FILE) as conn:
        c = conn.cursor()
        c.execute("""
            CREATE TABLE IF NOT EXISTS records (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                data TEXT NOT NULL,
                schema_version INTEGER NOT NULL,
                ingested_at TEXT NOT NULL,
                quality_issues TEXT
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS schema_history (
                version INTEGER PRIMARY KEY AUTOINCREMENT,
                schema TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS schema_changes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                old_version INTEGER,
                new_version INTEGER,
                added_fields TEXT,
                removed_fields TEXT,
                created_at TEXT NOT NULL
            )
        """)
        conn.commit()


def store_schema(schema):
    now = datetime.datetime.utcnow().isoformat()
    with sqlite3.connect(DB_FILE) as conn:
        c = conn.cursor()
        c.execute("SELECT MAX(version) FROM schema_history")
        row = c.fetchone()
        current_version = row[0] if row[0] else 0

        # Get old schema as dict
        if current_version > 0:
            c.execute("SELECT schema FROM schema_history WHERE version=?", (current_version,))
            old_schema_json = c.fetchone()[0]
            old_schema = json.loads(old_schema_json)
        else:
            old_schema = {}

        # Detect changes
        old_fields = set(old_schema.get("properties", {}).keys())
        new_fields = set(schema.get("properties", {}).keys())
        added = list(new_fields - old_fields)
        removed = list(old_fields - new_fields)

        new_version = current_version + 1
        c.execute(
            "INSERT INTO schema_history (version, schema, created_at) VALUES (?, ?, ?)",
            (new_version, json.dumps(schema), now)
        )

        if current_version > 0 and (added or removed):
            c.execute(
                "INSERT INTO schema_changes (old_version, new_version, added_fields, removed_fields, created_at) VALUES (?, ?, ?, ?, ?)",
                (current_version, new_version, json.dumps(added), json.dumps(removed), now)
            )
        conn.commit()
    return new_version


def store_record(record_json, schema_version, quality_issues):
    now = datetime.datetime.utcnow().isoformat()
    with sqlite3.connect(DB_FILE) as conn:
        c = conn.cursor()
        c.execute(
            "INSERT INTO records (data, schema_version, ingested_at, quality_issues) VALUES (?, ?, ?, ?)",
            (record_json, schema_version, now, json.dumps(quality_issues) if quality_issues else None)
        )
        conn.commit()


def get_latest_schema_version():
    with sqlite3.connect(DB_FILE) as conn:
        c = conn.cursor()
        c.execute("SELECT version, schema FROM schema_history ORDER BY version DESC LIMIT 1")
        row = c.fetchone()
        if row:
            return row[0], json.loads(row[1])
        else:
            return 0, {}


def get_schemas():
    with sqlite3.connect(DB_FILE) as conn:
        c = conn.cursor()
        c.execute("SELECT version, schema, created_at FROM schema_history ORDER BY version ASC")
        return c.fetchall()


def get_schema_changes():
    with sqlite3.connect(DB_FILE) as conn:
        c = conn.cursor()
        c.execute("SELECT old_version, new_version, added_fields, removed_fields, created_at FROM schema_changes ORDER BY created_at DESC")
        return c.fetchall()


def get_records(limit=50):
    with sqlite3.connect(DB_FILE) as conn:
        c = conn.cursor()
        c.execute("SELECT id, data, schema_version, ingested_at, quality_issues FROM records ORDER BY id DESC LIMIT ?", (limit,))
        return c.fetchall()


# --- Ingestors ---

def ingest_csv(file):
    return pd.read_csv(file).to_dict(orient='records')


def ingest_json(file):
    return json.load(file)


def ingest_pdf(file):
    # Extract text page by page
    reader = PyPDF2.PdfReader(file)
    texts = []
    for page in reader.pages:
        texts.append({"content": page.extract_text()})
    return texts


def ingest_docx(file):
    doc = Document(file)
    paragraphs = [{"content": p.text} for p in doc.paragraphs if p.text.strip()]
    return paragraphs


def ingest_xml(file):
    import xml.etree.ElementTree as ET
    tree = ET.parse(file)
    root = tree.getroot()
    records = []
    for child in root:
        record = {**child.attrib}
        record["_text"] = child.text.strip() if child.text else ""
        records.append(record)
    return records


def ingest_txt(file):
    """Return list of dicts (one per non-empty line), robust to any encoding."""
    try:
        # Move to start (in case file pointer is not at 0)
        file.seek(0)
        lines = safe_read_text(file)
        return [{"content": line} for line in lines if line.strip()]
    except Exception as e:
        # Return record with error info
        return [{"content": "", "_ingest_error": str(e)}]




def extract_patterns(text):
    # Convert bytes to string if needed
    if isinstance(text, bytes):
        text = text.decode('utf-8', errors='ignore')
    
    # Handle None or non-string types
    if not isinstance(text, str):
        return {"emails": [], "phones": [], "dates": [], "numbers": []}
    
    emails = re.findall(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b', text)
    phones = re.findall(r'(\+?\d{1,3}[-.\s]?)?(\(?\d{3}\)?[-.\s]?)\d{3}[-.\s]?\d{4}', text)
    phones = [''.join(p) for p in phones]
    dates = re.findall(r'\d{1,2}[-/]\d{1,2}[-/]\d{2,4}', text)
    numbers = re.findall(r'-?\d+\.?\d*', text)
    return {
        "emails": emails,
        "phones": phones,
        "dates": dates,
        "numbers": numbers
    }



def infer_schema(records):
    builder = SchemaBuilder()
    for rec in records:
        # Convert bytes to strings for schema inference
        cleaned_rec = {}
        for key, value in rec.items():
            if isinstance(value, bytes):
                cleaned_rec[key] = value.decode('utf-8', errors='ignore')
            elif value is None:
                cleaned_rec[key] = ""
            else:
                cleaned_rec[key] = str(value)
        builder.add_object(cleaned_rec)
    return builder.to_schema()



def validate_record(record, schema):
    quality_issues = []
    properties = schema.get("properties", {})
    for field in record.keys():
        if field not in properties:
            quality_issues.append(f"Field '{field}' not in schema")
    for field in properties.keys():
        if field not in record:
            quality_issues.append(f"Missing field: '{field}'")
    return quality_issues


# --- Streamlit UI ---

st.markdown("<h1>Unified Dynamic ETL Pipeline</h1>", unsafe_allow_html=True)
st.caption("**Upload file (CSV, JSON, PDF, DOCX, XML, TXT)**")

col1, col2 = st.columns([3, 1])
with col1:
    uploaded_file = st.file_uploader(
        "", type=['csv', 'json', 'pdf', 'docx', 'xml', 'txt'],
        accept_multiple_files=False,
        help="CSV, JSON, PDF, DOCX, XML, TXT files up to 200MB"
    )
with col2:
    st.markdown("##")
    st.markdown('<div style="text-align:right;font-size:smaller;">data persists in SQLite<br>across app restarts</div>', unsafe_allow_html=True)


if uploaded_file:

    suffix = Path(uploaded_file.name).suffix.lower()

    # Ingest data based on file type
    if suffix == '.csv':
        records = ingest_csv(uploaded_file)
    elif suffix == '.json':
        records = ingest_json(uploaded_file)
    elif suffix == '.pdf':
        records = ingest_pdf(uploaded_file)
    elif suffix == '.docx':
        records = ingest_docx(uploaded_file)
    elif suffix == '.xml':
        records = ingest_xml(uploaded_file)
    elif suffix == '.txt':
        records = ingest_txt(uploaded_file)
    else:
        st.error("Unsupported file type")
        st.stop()

    # Basic extraction on text fields
    for rec in records:
        if "content" in rec and rec["content"]:
            rec["_extracted_patterns"] = extract_patterns(rec["content"])
        else:
            rec["_extracted_patterns"] = {"emails": [], "phones": [], "dates": [], "numbers": []}

                # Preview data as a table for CSV/JSON/TXT (first 10 rows)
    if suffix in ('.csv', '.json', '.txt'):
        try:
            df_preview = pd.DataFrame(records)
            if not df_preview.empty:
                st.markdown("#### Data preview (first 10 rows)")
                st.dataframe(df_preview.head(10), use_container_width=True)
        except Exception:
            pass


    # Schema inference & evolution
    latest_version, latest_schema = get_latest_schema_version()
    new_schema = infer_schema(records)

    if latest_schema:
        old_fields = set(latest_schema.get('properties', {}).keys())
        new_fields = set(new_schema.get('properties', {}).keys())
        if old_fields != new_fields:
            new_version = store_schema(new_schema)
            st.info(f"Schema updated from v{latest_version} to v{new_version}")
        else:
            new_version = latest_version
            st.info(f"Schema unchanged at v{new_version}")
    else:
        new_version = store_schema(new_schema)
        st.info(f"New schema version {new_version} created")

    # Validate and store records
    count_good = 0
    count_issues = 0
    for rec in records:
        quality_issues = validate_record(rec, new_schema)
        if quality_issues:
            count_issues += 1
        else:
            count_good += 1
        rec["_schema_version"] = new_version
        rec["_ingested_at"] = datetime.datetime.utcnow().isoformat()
        rec["_quality_issues"] = quality_issues
        store_record(json.dumps(rec), new_version, quality_issues)

    st.success(f"{len(records)} records processed: {count_good} valid, {count_issues} with issues.")

    # Basic display
    if st.checkbox("Show raw records"):
        for rec in records[:5]:
            st.json(rec)

# Display schema evolution history
if st.checkbox("Show schema history"):
    schemas = get_schemas()
    for version, schema_json, created_at in schemas:
        st.write(f"Version {version} at {created_at}")
        st.json(json.loads(schema_json))

# Display schema change logs
if st.checkbox("Show schema changes"):
    changes = get_schema_changes()
    for old_v, new_v, added, removed, created_at in changes:
        st.write(f"{created_at}: v{old_v} → v{new_v}")
        st.write(f"  Added fields: {json.loads(added)}")
        st.write(f"  Removed fields: {json.loads(removed)}")

# Browse stored records
if st.checkbox("Browse last records"):
    recs = get_records()
    for rec_id, data, schema_v, ingested_at, quality in recs:
        st.write(f"Record {rec_id} - Schema v{schema_v} - Ingested at {ingested_at}")
        st.json(json.loads(data))
        if quality:
            st.warning(f"Quality issues: {quality}")
