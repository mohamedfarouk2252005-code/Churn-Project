# Data Directory

This folder holds raw input data for the pipeline. **All files here are excluded from Git.**

## Setup

Place the raw dataset file at:

```
data/raw/churn_raw.csv
```

The file is ~55 MB (200,000 rows). You can get it from the shared team drive or unzip `churn_raw.zip` from the project root:

```bash
unzip churn_raw.zip -d data/raw/
```

## Folder layout

```
data/
└── raw/
    └── churn_raw.csv    ← the main dataset (not committed to git)
```
