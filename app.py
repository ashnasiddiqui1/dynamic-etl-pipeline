import streamlit as st
import pandas as pd
import json
import datetime
from etl import (
    init_db, ingest_file, extract_patterns, infer_schema, compare_schemas,
    store_schema, get_latest_schema_version, store_record,
    get_schemas, get_schema_changes, get_records
)

init_db()

st.title("Dynamic ETL Pipeline")

uploaded = st.file_uploader("Upload a file", type=["csv","json","pdf","docx","xml","txt"])

if uploaded:
    records = ingest_file(uploaded, uploaded.name)

    for r in records:
        if "content" in r:
            r["_patterns"] = extract_patterns(r["content"])

    st.subheader("Preview")
    st.dataframe(pd.DataFrame(records).head(10))

    latest_v, latest_schema = get_latest_schema_version()
    new_schema = infer_schema(records)

    diff = compare_schemas(latest_schema, new_schema)

    if diff["added"] or diff["removed"]:
        version = store_schema(new_schema)
        st.info(f"Schema updated to v{version}")
    else:
        version = latest_v or store_schema(new_schema)

    good = 0
    bad = 0
    for r in records:
        issues = []
        props = new_schema.get("properties", {})

        for key in props:
            if key not in r:
                issues.append(f"Missing: {key}")

        for key in r:
            if key not in props:
                issues.append(f"Extra: {key}")

        if issues:
            bad += 1
        else:
            good += 1

        r["_schema_version"] = version
        r["_ingested_at"] = datetime.datetime.utcnow().isoformat()

        store_record(json.dumps(r), version, issues)

    st.success(f"{len(records)} records processed • {good} OK • {bad} Issues")

if st.checkbox("Show schema history"):
    for v, s, t in get_schemas():
        st.write(f"Version {v} at {t}")
        st.json(json.loads(s))

if st.checkbox("Show schema changes"):
    for old, new, add, rem, t in get_schema_changes():
        st.write(f"{t}: v{old} → v{new}")
        st.json({"added": add, "removed": rem})

if st.checkbox("Browse latest records"):
    for rid, data, v, t, q in get_records():
        st.write(f"Record {rid} (v{v}) at {t}")
        st.json(json.loads(data))
