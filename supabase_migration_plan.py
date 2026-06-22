#!/usr/bin/env python3
# SUPABASE MIGRATION PLAN - Priority 9
from pathlib import Path

MIGRATION_DIR = Path("data_output/migration")
MIGRATION_DIR.mkdir(parents=True, exist_ok=True)

def generate_migration_files():
    schema_path = MIGRATION_DIR / "schema.sql"
    guide_path  = MIGRATION_DIR / "migration_guide.md"
    if schema_path.exists():
        print(f"Schema exists: {schema_path}")
    else:
        print(f"Schema missing: {schema_path}")
    if guide_path.exists():
        print(f"Guide exists:  {guide_path}")
    else:
        print(f"Guide missing: {guide_path}")

if __name__ == "__main__":
    generate_migration_files()
    print("MIGRATION_FILES_DONE")
