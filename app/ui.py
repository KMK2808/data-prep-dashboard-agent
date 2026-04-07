from __future__ import annotations

from pathlib import Path

import pandas as pd
import streamlit as st

from app.final_dashboard import build_dashboard_bundle, build_kpi_summary, default_dashboard_selections
from app.final_engine import (
    CleaningOptions,
    ColumnCleaningConfig,
    apply_cleaning,
    build_recommendations,
    create_markdown_report,
    dataframe_to_csv_bytes,
    default_column_configs,
    load_dataframe,
    profile_dataframe,
    report_payload,
)
from app.persistence import load_saved_runs, save_run_artifacts

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SAMPLE_DATASET = PROJECT_ROOT / "data" / "sample_sales_messy.csv"
REPORTS_DIR = PROJECT_ROOT / "reports"


def apply_app_theme() -> None:
    st.set_page_config(page_title="Data Prep Dashboard Agent", layout="wide")
    st.markdown(
        """
        <style>
        :root {
            --bg-top: #f3efe7;
            --bg-mid: #fbfaf7;
            --bg-bottom: #e7f0ee;
            --ink: #17323b;
            --muted: #5d7477;
            --brand-deep: #0f4c5c;
            --brand: #1f6f78;
            --brand-soft: #d7ebe7;
            --accent: #e59560;
            --accent-soft: #f6e2d3;
            --card: rgba(255, 252, 248, 0.9);
            --line: rgba(23, 50, 59, 0.12);
        }
        .stApp {
            background:
                radial-gradient(circle at top left, rgba(229,149,96,0.18), transparent 28%),
                radial-gradient(circle at top right, rgba(31,111,120,0.12), transparent 30%),
                linear-gradient(180deg, var(--bg-top) 0%, var(--bg-mid) 44%, var(--bg-bottom) 100%);
            color: var(--ink);
        }
        [data-testid="stSidebar"] {
            background: linear-gradient(180deg, #163640 0%, #1f5561 100%);
            border-right: 1px solid rgba(255,255,255,0.08);
        }
        [data-testid="stSidebar"] * {
            color: #f7f6f2;
        }
        [data-testid="stSidebar"] .stCheckbox label,
        [data-testid="stSidebar"] .stRadio label,
        [data-testid="stSidebar"] .stSelectbox label,
        [data-testid="stSidebar"] .stFileUploader label,
        [data-testid="stSidebar"] .stTextInput label {
            color: #f7f6f2 !important;
        }
        .hero {
            position: relative;
            overflow: hidden;
            padding: 1.4rem 1.6rem;
            border-radius: 24px;
            background: linear-gradient(135deg, #0f4c5c 0%, #1f6f78 56%, #3da5a8 100%);
            color: white;
            margin-bottom: 1rem;
            box-shadow: 0 22px 42px rgba(15, 76, 92, 0.22);
            border: 1px solid rgba(255,255,255,0.08);
        }
        .hero::after {
            content: "";
            position: absolute;
            width: 240px;
            height: 240px;
            border-radius: 999px;
            background: rgba(255,255,255,0.08);
            top: -90px;
            right: -40px;
        }
        .metric-card {
            padding: 0.95rem 1rem;
            border-radius: 16px;
            background: linear-gradient(180deg, rgba(255,255,255,0.95), rgba(247,244,239,0.88));
            border: 1px solid var(--line);
            box-shadow: 0 12px 24px rgba(20, 54, 66, 0.08);
        }
        .metric-card div {
            color: var(--muted);
            font-size: 0.86rem;
            text-transform: uppercase;
            letter-spacing: 0.04em;
        }
        .metric-card h3 {
            color: var(--brand-deep);
            margin: 0.25rem 0 0 0;
            font-size: 1.7rem;
        }
        .stTabs [data-baseweb="tab-list"] {
            gap: 0.4rem;
            background: rgba(255,255,255,0.45);
            border-radius: 18px;
            padding: 0.25rem;
            border: 1px solid var(--line);
        }
        .stTabs [data-baseweb="tab"] {
            border-radius: 14px;
            color: var(--muted);
            padding: 0.55rem 0.9rem;
        }
        .stTabs [aria-selected="true"] {
            background: linear-gradient(135deg, var(--accent-soft), #fff7ef) !important;
            color: var(--brand-deep) !important;
            font-weight: 600;
        }
        .stButton > button, .stDownloadButton > button {
            border-radius: 14px;
            border: 1px solid rgba(15, 76, 92, 0.12);
            background: linear-gradient(135deg, var(--brand-deep), var(--brand));
            color: white;
            box-shadow: 0 12px 24px rgba(15, 76, 92, 0.16);
        }
        .stButton > button:hover, .stDownloadButton > button:hover {
            border-color: rgba(15, 76, 92, 0.16);
            background: linear-gradient(135deg, #0d4553, #255e67);
            color: white;
        }
        .stDataFrame, .stPlotlyChart, .stAlert, .stCodeBlock {
            border-radius: 18px;
        }
        h1, h2, h3 {
            color: var(--ink);
        }
        .stCaption, p, label {
            color: var(--muted);
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def render_metric_cards(kpis: dict[str, int]) -> None:
    cols = st.columns(5)
    labels = [
        ("Rows", f"{kpis['rows']:,}"),
        ("Columns", f"{kpis['columns']:,}"),
        ("Missing Cells", f"{kpis['missing_cells']:,}"),
        ("Duplicates Removed", f"{kpis['duplicates_removed']:,}"),
        ("Quality Score", f"{kpis['quality_score']}/100"),
    ]
    for col, (label, value) in zip(cols, labels):
        col.markdown(f"<div class='metric-card'><div>{label}</div><h3>{value}</h3></div>", unsafe_allow_html=True)


def get_source_and_options() -> tuple[CleaningOptions, pd.DataFrame | None, str | None]:
    with st.sidebar:
        st.header("Dataset")
        uploaded_file = st.file_uploader("Upload CSV or XLSX", type=["csv", "xlsx"])
        use_sample = st.checkbox("Use built-in sample dataset", value=uploaded_file is None)

        st.header("Global Cleaning Rules")
        options = CleaningOptions(
            drop_duplicates=st.checkbox("Drop duplicate rows", value=True),
            trim_whitespace=st.checkbox("Trim text whitespace", value=True),
            standardize_missing_markers=st.checkbox("Normalize missing markers", value=True),
            convert_numeric_like=st.checkbox("Auto-convert numeric-like columns", value=True),
            convert_datetime_like=st.checkbox("Auto-convert datetime-like columns", value=True),
            drop_empty_rows=st.checkbox("Drop fully empty rows", value=True),
            fill_numeric_missing=st.checkbox("Fill numeric nulls with median", value=False),
            fill_categorical_missing=st.checkbox("Fill categorical nulls with mode", value=False),
            lowercase_text=st.checkbox("Lowercase text globally", value=False),
        )

    dataset_name = uploaded_file.name if uploaded_file is not None else SAMPLE_DATASET.name if use_sample else None
    raw_df = load_dataframe(uploaded_file=uploaded_file, sample_path=SAMPLE_DATASET if use_sample else None)
    return options, raw_df, dataset_name


def build_column_configs(raw_df: pd.DataFrame) -> dict[str, ColumnCleaningConfig]:
    default_configs = default_column_configs(raw_df)
    configs: dict[str, ColumnCleaningConfig] = {}
    for column in raw_df.columns:
        default = default_configs[column]
        with st.expander(column, expanded=False):
            target_name = st.text_input("Column name", value=default.target_name, key=f"name_{column}")
            target_type = st.selectbox(
                "Target type",
                options=["keep", "numeric", "datetime", "string"],
                index=["keep", "numeric", "datetime", "string"].index(default.target_type),
                key=f"type_{column}",
            )
            fill_strategy = st.selectbox(
                "Missing value strategy",
                options=["none", "mode", "median", "mean", "zero", "custom"],
                index=0,
                key=f"fill_{column}",
            )
            custom_fill = st.text_input("Custom fill value", value="", key=f"custom_{column}")
            lowercase_text = st.checkbox("Lowercase this column", value=False, key=f"lower_{column}")
            drop_column = st.checkbox("Drop this column", value=False, key=f"drop_{column}")
        configs[column] = ColumnCleaningConfig(
            target_name=target_name,
            target_type=target_type,
            fill_strategy=fill_strategy,
            custom_fill_value=custom_fill,
            drop_column=drop_column,
            lowercase_text=lowercase_text,
        )
    return configs


def main() -> None:
    apply_app_theme()
    st.markdown(
        """
        <div class="hero">
            <h1>Data Prep Dashboard Agent</h1>
            <p>Clean raw tabular data, apply column-level transformation rules, and generate dashboard-ready analytics in one workflow.</p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    try:
        options, raw_df, dataset_name = get_source_and_options()
    except ValueError as exc:
        st.error(str(exc))
        return
    if raw_df is None or dataset_name is None:
        st.info("Upload a file or enable the sample dataset to begin.")
        return

    tabs = st.tabs(["Overview", "Health", "Prepare", "Dashboard Studio", "Export", "Run History"])

    with tabs[2]:
        st.subheader("Column-level cleaning controls")
        st.caption("Use these settings to rename fields, coerce data types, fill nulls, or drop low-value columns.")
        column_configs = build_column_configs(raw_df)

    cleaned_df, operations = apply_cleaning(raw_df, options, column_configs)
    raw_profile = profile_dataframe(raw_df)
    cleaned_profile = profile_dataframe(cleaned_df)
    recommendations = build_recommendations(cleaned_profile)
    kpis = build_kpi_summary(raw_df, cleaned_df, cleaned_profile)
    selections = default_dashboard_selections(cleaned_profile)
    report_text = create_markdown_report(dataset_name, raw_profile, cleaned_profile, operations)
    payload = report_payload(dataset_name, raw_profile, cleaned_profile, operations, options, column_configs)

    st.markdown("### Delivery Snapshot")
    render_metric_cards(kpis)

    with tabs[0]:
        st.subheader("Executive summary")
        st.caption(f"Dataset: `{dataset_name}`")
        for recommendation in recommendations:
            st.write(f"- {recommendation}")
        left, right = st.columns(2)
        left.markdown("#### Raw data preview")
        left.dataframe(raw_df.head(12), use_container_width=True)
        right.markdown("#### Cleaned data preview")
        right.dataframe(cleaned_df.head(12), use_container_width=True)

    with tabs[1]:
        st.subheader("Data health diagnostics")
        col1, col2 = st.columns([1, 2])
        col1.metric("Raw Quality Score", f"{raw_profile['quality_score']}/100")
        col1.metric("Cleaned Quality Score", f"{cleaned_profile['quality_score']}/100")
        col1.metric("Dataset Memory", f"{cleaned_profile['memory_mb']} MB")
        profile_df = pd.DataFrame(cleaned_profile["column_profiles"])
        col2.dataframe(profile_df, use_container_width=True)
        if cleaned_profile["missing_by_column"]:
            missing_df = pd.DataFrame(
                [{"column": column, "missing": count} for column, count in cleaned_profile["missing_by_column"].items()]
            )
            st.bar_chart(missing_df.set_index("column"))

    with tabs[2]:
        st.markdown("#### Applied cleaning operations")
        if operations:
            for operation in operations:
                st.write(f"- {operation}")
        else:
            st.write("- No cleaning rules changed the dataset.")
        st.dataframe(
            pd.DataFrame(
                [
                    {"stage": "Raw", "rows": raw_profile["rows"], "missing_cells": raw_profile["missing_cells"], "quality_score": raw_profile["quality_score"]},
                    {"stage": "Cleaned", "rows": cleaned_profile["rows"], "missing_cells": cleaned_profile["missing_cells"], "quality_score": cleaned_profile["quality_score"]},
                ]
            ),
            use_container_width=True,
        )

    with tabs[3]:
        st.subheader("Dashboard studio")
        metric_col, dimension_col, date_col, agg_col, chart_col = st.columns(5)
        metric = metric_col.selectbox("Metric", options=[None] + cleaned_profile["numeric_columns"], index=1 if cleaned_profile["numeric_columns"] else 0)
        dimension = dimension_col.selectbox("Dimension", options=[None] + cleaned_profile["categorical_columns"], index=1 if cleaned_profile["categorical_columns"] else 0)
        date_column = date_col.selectbox("Date", options=[None] + cleaned_profile["datetime_columns"], index=1 if cleaned_profile["datetime_columns"] else 0)
        aggregation = agg_col.selectbox("Aggregation", options=["sum", "avg", "count", "max", "min"], index=0)
        chart_type = chart_col.selectbox("Primary chart", options=["Bar", "Treemap", "Pie"], index=0)
        bundle = build_dashboard_bundle(
            cleaned_df,
            metric=metric or selections["metric"],
            dimension=dimension or selections["dimension"],
            date_column=date_column or selections["date_column"],
            aggregation=aggregation,
            chart_type=chart_type,
        )
        for figure in bundle["figures"]:
            st.plotly_chart(figure, use_container_width=True)
        for title, table in bundle["tables"].items():
            st.markdown(f"#### {title.replace('_', ' ').title()}")
            st.dataframe(table, use_container_width=True)

    with tabs[4]:
        st.subheader("Exports and persistence")
        save_col, csv_col, md_col = st.columns(3)
        if save_col.button("Save run artifacts", use_container_width=True):
            run_dir = save_run_artifacts(REPORTS_DIR, dataset_name, cleaned_df, report_text, payload)
            st.success(f"Saved run to {run_dir}")
        csv_col.download_button("Download cleaned CSV", data=dataframe_to_csv_bytes(cleaned_df), file_name="cleaned_dataset.csv", mime="text/csv", use_container_width=True)
        md_col.download_button("Download markdown report", data=report_text.encode("utf-8"), file_name="data_prep_report.md", mime="text/markdown", use_container_width=True)
        st.code(report_text, language="markdown")

    with tabs[5]:
        st.subheader("Saved runs")
        runs = load_saved_runs(REPORTS_DIR)
        if runs:
            st.dataframe(pd.DataFrame(runs), use_container_width=True)
        else:
            st.info("No saved runs yet. Save the current run from the Export tab to create one.")
