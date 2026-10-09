# Dataset

The raw data is not stored in this repository (about 260 MB).

Download **"Energy Usage Measurements for Public Schools in Western Cape, South Africa"** (Mendeley Data / Stellenbosch University, 2024) and unpack it so the folder contains:

```
Energy Usage Measurements for Public Schools in Western Cape, South Africa/
├── raw_data/        53 school CSVs + combined_raw.csv
├── clean_data/      53 school CSVs + combined_clean.csv (gap-filled by the publisher)
├── enhanced_data/   53 school CSVs + combined_enhanced.csv (season / term / schoolDay)
└── metadata.xlsx
```

On first run, `code/config.py` asks for the path of this folder and stores it in `code/local_settings.json` (git-ignored).
