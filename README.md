# Energy Usage of Public Schools in Western Cape, South Africa

Big-data pipeline (MongoDB + PySpark + Tableau) analysing 30-minute electricity readings from 53 public schools. Course project for **PGR108 Big Data**.

![Python](https://img.shields.io/badge/Python-3.10%2B-blue) ![PySpark](https://img.shields.io/badge/PySpark-3.5-orange) ![MongoDB](https://img.shields.io/badge/MongoDB-7.x-green) ![Tableau](https://img.shields.io/badge/Tableau-dashboards-1f77b4)

## Pipeline

```
raw CSVs ──▶ MongoDB (readings_raw, schools_metadata)
         ──▶ PySpark cleaning + enrichment ──▶ MongoDB (readings_clean)
         ──▶ 10 analytical queries ──▶ output/aggregates/
         ──▶ Tableau-friendly CSVs ──▶ output/tableau/
```

| Step | Script | What it does |
|------|--------|--------------|
| 1 | `code/01_load_to_mongodb.py` | Loads raw CSVs and the metadata sheet into MongoDB |
| 2 | `code/02_pyspark_preprocessing.py` | Gap filling, outlier capping, season / school-term features |
| 3 | `code/03_pyspark_analysis.py` | Runs the ten analytical queries |
| 4 | `code/04_export_for_tableau.py` | Flattens Spark output into single CSVs for Tableau |

Shared settings (paths, MongoDB URI, analysis parameters) live in `code/config.py`.

## Analytical queries

Q1 annual consumption per school · Q2 quintile summary · Q3 hourly load profile · Q4 weekend vs weekday (per school and per quintile) · Q5 seasonal · Q6 term vs holiday · Q7 peak intervals · Q8 baseload · Q9 top-10 consumers · Q10 intervention impact (overall and by quintile). Results from our run are in [`output/`](output).

## Getting started

**Requirements:** Python 3.10+, JDK 17, MongoDB 7.x on `localhost:27017`, Tableau Desktop/Public 2024.x (dashboards only). On Windows, PySpark also needs `winutils.exe` in `C:\hadoop\bin`.

```bash
conda create -n spark_env python=3.12 numpy=1.26.4 pyspark=3.5.5 pymongo openjdk=17 openpyxl -c conda-forge
conda activate spark_env
```

1. Download the dataset (see [`data/README.md`](data/README.md)).
2. Start MongoDB (`mongod --dbpath C:\data\db` on Windows, or use MongoDB Compass).
3. Run the pipeline in order. On first run you are asked for the dataset path and collection names.

```bash
cd code
python 01_load_to_mongodb.py
python 02_pyspark_preprocessing.py
python 03_pyspark_analysis.py
python 04_export_for_tableau.py
```

Each script can be re-run on its own. The Spark ↔ MongoDB connector is fetched automatically as a Maven coordinate.

## MongoDB layout

Database `energy_schools` with collections `readings_raw` (one document per school and interval), `schools_metadata` (one per school) and `readings_clean` (cleaned and enriched copy used by the analysis).

## Repository structure

```
code/      pipeline scripts, config, requirements
data/      dataset instructions (data itself is not committed)
output/    aggregates/ (Spark results) and tableau/ (dashboard CSVs)
docs/      project report (PDF) and presentation (PPTX)
```

## Data source

*Energy Usage Measurements for Public Schools in Western Cape, South Africa*, Mendeley Data / Stellenbosch University, 2024.

## Authors

PGR108 Big Data project group.
