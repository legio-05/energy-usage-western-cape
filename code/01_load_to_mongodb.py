"""
01 - Ingest raw CSV files and the metadata sheet into MongoDB.

For each of the 53 schools we read the raw_data/<sid>.csv file and write
one document per 30-minute interval into the readings_raw collection.
The general_information sheet of metadata.xlsx is loaded into the
schools_metadata collection (one document per school).

Run once before the rest of the pipeline.  Re-running is safe; the script
clears the two collections first to avoid duplicates.
"""
from datetime import datetime
import csv
import sys

import pandas as pd
from pymongo import MongoClient, ASCENDING

from config import DATA_DIR, MONGO_URI, MONGO_DB, COL_RAW, COL_META


def connect():
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    client.admin.command("ping")              # fail fast if mongod is down
    return client[MONGO_DB]


def load_metadata(db):
    """Load the general_information sheet, normalise column names."""
    print(f"Loading {COL_META}.xlsx ...")
    md = pd.read_excel(DATA_DIR / f"{COL_META}.xlsx", sheet_name="general_information")
    md.columns = [c.strip() for c in md.columns]              # tidy headers
    md = md.rename(columns={"type": "school_type"})
    docs = md.to_dict(orient="records")

    db[COL_META].drop()
    db[COL_META].insert_many(docs)
    db[COL_META].create_index([("school_id", ASCENDING)], unique=True)
    print(f"  -> {len(docs)} schools written to '{COL_META}'")


def load_readings(db):
    """Stream every raw_data/S*.csv file in batches, skip the combined one."""
    print("Loading raw_data/*.csv ...")
    db[COL_RAW].drop()

    raw_dir = DATA_DIR / "raw_data"
    files = sorted(p for p in raw_dir.glob("S*.csv") if p.name != f"{COL_RAW}.csv")
    if not files:
        sys.exit(f"No school CSV files found in {raw_dir}")

    BATCH = 5000
    total = 0
    for path in files:
        school_id = path.stem                                  # 'S1', 'S10', ...
        batch = []
        with path.open(newline="") as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                # timestamps are 'YYYY-MM-DD HH:MM:SS' in the source files
                try:
                    ts = datetime.strptime(row["timestamp"], "%Y-%m-%d %H:%M:%S")
                except ValueError:
                    continue                                   # malformed line, skip
                # kWh may be empty for missing intervals; keep as None for now
                kwh = row["kWh"].strip()
                kwh = float(kwh) if kwh else None

                batch.append({"school_id": school_id, "timestamp": ts, "kWh": kwh})
                if len(batch) >= BATCH:
                    db[COL_RAW].insert_many(batch, ordered=False)
                    total += len(batch)
                    batch = []
        if batch:
            db[COL_RAW].insert_many(batch, ordered=False)
            total += len(batch)
        print(f"  {school_id}: cumulative rows = {total:,}")

    # compound index for the queries that filter by (school, timestamp)
    db[COL_RAW].create_index([("school_id", ASCENDING), ("timestamp", ASCENDING)])
    print(f"Done. {total:,} rows in '{COL_RAW}'.")


def main():
    db = connect()
    load_metadata(db)
    load_readings(db)
    print("Ingest complete.")


if __name__ == "__main__":
    main()
