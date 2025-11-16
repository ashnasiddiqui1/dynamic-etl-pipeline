import sqlite3
import json
import datetime
from pathlib import Path

DB_FILE = Path("etl_data.db")

def init_db():
    DB_FILE.parent.mkdir(parents=True, exist_ok=True)
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
        c.execute("CREATE INDEX IF NOT EXISTS idx_records_schema ON records(schema_version)")
        conn.commit()


def store_schema(schema):
    now = datetime.datetime.utcnow().isoformat()
    with sqlite3.connect(DB_FILE) as conn:
        c = conn.cursor()
        c.execute("SELECT MAX(version) FROM schema_history")
        row = c.fetchone()
        current_version = row[0] if row[0] else 0

        if current_version > 0:
            c.execute("SELECT schema FROM schema_history WHERE version=?", (current_version,))
            old_schema = json.loads(c.fetchone()[0])
        else:
            old_schema = {}

        old_fields = set(old_schema.get("properties", {}).keys())
        new_fields = set(schema.get("properties", {}).keys())
        added = list(new_fields - old_fields)
        removed = list(old_fields - new_fields)

        new_version = current_version + 1
        c.execute(
            "INSERT INTO schema_history (version, schema, created_at) VALUES (?, ?, ?)",
            (new_version, json.dumps(schema), now),
        )

        if (added or removed) and current_version > 0:
            c.execute(
                "INSERT INTO schema_changes (old_version, new_version, added_fields, removed_fields, created_at) VALUES (?, ?, ?, ?, ?)",
                (current_version, new_version, json.dumps(added), json.dumps(removed), now),
            )

        conn.commit()

    return new_version


def store_record(record_json, version, issues):
    now = datetime.datetime.utcnow().isoformat()
    with sqlite3.connect(DB_FILE) as conn:
        c = conn.cursor()
        c.execute(
            "INSERT INTO records (data, schema_version, ingested_at, quality_issues) VALUES (?, ?, ?, ?)",
            (record_json, version, now, json.dumps(issues) if issues else None),
        )
        conn.commit()


def get_latest_schema_version():
    with sqlite3.connect(DB_FILE) as conn:
        c = conn.cursor()
        c.execute("SELECT version, schema FROM schema_history ORDER BY version DESC LIMIT 1")
        row = c.fetchone()
        if row:
            return row[0], json.loads(row[1])
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
