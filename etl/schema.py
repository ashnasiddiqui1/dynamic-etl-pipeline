from genson import SchemaBuilder

def infer_schema(records):
    builder = SchemaBuilder()
    for row in records:
        builder.add_object(row if isinstance(row, dict) else {"value": row})
    return builder.to_schema()

def compare_schemas(old, new):
    old_fields = set(old.get("properties", {}).keys())
    new_fields = set(new.get("properties", {}).keys())
    return {
        "added": list(new_fields - old_fields),
        "removed": list(old_fields - new_fields),
    }
