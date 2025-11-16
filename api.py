from fastapi import FastAPI, File, UploadFile
from io import BytesIO
import json
import datetime
from etl import (
    init_db, ingest_file, infer_schema, store_schema,
    store_record, extract_patterns
)

app = FastAPI()
init_db()

@app.post("/ingest")
async def ingest_api(file: UploadFile = File(...)):
    raw = await file.read()
    buf = BytesIO(raw)

    records = ingest_file(buf, file.filename)

    for r in records:
        if "content" in r:
            r["_patterns"] = extract_patterns(r["content"])

    schema = infer_schema(records)
    version = store_schema(schema)

    for r in records:
        r["_schema_version"] = version
        r["_ingested_at"] = datetime.datetime.utcnow().isoformat()
        store_record(json.dumps(r), version, [])

    return {"ingested": len(records), "schema_version": version}
