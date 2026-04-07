# Data Prep Dashboard Agent

Data Prep Dashboard Agent is a flagship open-source analytics product that combines:

- interactive data cleaning workbench
- automatic dashboard generation
- saved run artifacts for repeatable analysis
- deploy-ready packaging with Docker

The app takes a messy CSV or Excel file, profiles its issues, applies both global and column-level cleaning rules, and produces a dashboard-ready dataset with exportable reports.

## Why this project

This project is designed as a flagship portfolio piece for data engineering, analytics engineering, and BI-oriented roles. It sits between lightweight data utilities and a production-grade analytics product.

## Current features

- Upload `.csv` or `.xlsx` data
- Use a built-in sample dataset for quick demos
- Profile rows, columns, missing values, duplicates, data types, outliers, and quality score
- Apply rule-based cleaning:
  - trim whitespace
  - normalize missing markers
  - convert numeric-like columns
  - convert datetime-like columns
  - drop empty rows
  - drop duplicates
  - lowercase text globally or per column
  - optionally fill missing values with mean, median, mode, zero, or custom values
  - rename or drop individual columns
- Review raw vs cleaned previews
- Generate KPI cards, drill-down charts, and DuckDB-powered summary tables
- Save run artifacts locally in `reports/`
- Download cleaned CSV and markdown summary report
- Run unit tests
- Launch with Docker

## Project structure

```text
data-prep-dashboard-agent/
  app/
    __init__.py
    dashboard.py
    data_processing.py
    final_dashboard.py
    final_engine.py
    persistence.py
    ui.py
  data/
    sample_sales_messy.csv
  reports/
  tests/
    test_final_engine.py
  Dockerfile
  streamlit_app.py
  requirements.txt
  README.md
```

## Getting started

```bash
cd data-prep-dashboard-agent
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
streamlit run streamlit_app.py
```

## Run tests

```bash
python -m unittest discover -s tests
```

## Run with Docker

```bash
docker build -t data-prep-dashboard-agent .
docker run -p 8501:8501 data-prep-dashboard-agent
```

## Sample workflow

1. Upload a raw dataset or use the included sample file.
2. Review profiling results and recommended next steps.
3. Turn cleaning rules on or off from the sidebar.
4. Inspect the cleaned preview and generated charts.
5. Export the cleaned CSV and markdown report.

## Next production upgrades

- Add Pandera or Great Expectations validations
- Persist runs in SQLite or Postgres
- Add authentication and project workspaces
- Export dashboard configs for Superset or Metabase
- Add background jobs for scheduled profiling and alerting

## Good next extensions

- LLM explanations for detected data issues
- natural-language chart generation
- anomaly detection
- schema drift alerts
- dbt-ready model export
