"""
04 - Export Tableau-friendly CSVs.

Spark writes folders with part-NNNNN files; Tableau is happiest with a
single named CSV per dashboard.  This script flattens each folder under
output/aggregates/ into output/tableau/<query>.csv.
"""
import shutil
from pathlib import Path

import pandas as pd

from config import AGG_DIR, TABLEAU_DIR


def flatten(folder: Path) -> Path:
    """Take a Spark-written folder of part-files and return a single DataFrame
       (handles either one or several part files)."""
    parts = sorted(folder.glob("part-*.csv"))
    if not parts:
        raise FileNotFoundError(f"No part files in {folder}")
    return pd.concat([pd.read_csv(p) for p in parts], ignore_index=True)


def main():
    if not AGG_DIR.exists():
        raise SystemExit(f"{AGG_DIR} does not exist - run 03_pyspark_analysis first")

    if TABLEAU_DIR.exists():
        shutil.rmtree(TABLEAU_DIR)
    TABLEAU_DIR.mkdir(parents=True, exist_ok=True)

    for folder in sorted(AGG_DIR.iterdir()):
        if not folder.is_dir():
            continue
        df = flatten(folder)
        out = TABLEAU_DIR / f"{folder.name}.csv"
        df.to_csv(out, index=False)
        print(f"  {folder.name:<40} {len(df):>6} rows  ->  {out.name}")

    print(f"Done.  Open the files in {TABLEAU_DIR} from Tableau.")


if __name__ == "__main__":
    main()
