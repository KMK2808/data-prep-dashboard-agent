from __future__ import annotations

from dataclasses import asdict, dataclass
from io import BytesIO
from pathlib import Path
from typing import Any

import pandas as pd

MISSING_MARKERS = {"", "na", "n/a", "null", "none", "unknown", "missing", "nan"}


@dataclass
class CleaningOptions:
    drop_duplicates: bool = True
    trim_whitespace: bool = True
    standardize_missing_markers: bool = True
    convert_numeric_like: bool = True
    convert_datetime_like: bool = True
    drop_empty_rows: bool = True
    fill_numeric_missing: bool = False
    fill_categorical_missing: bool = False
    lowercase_text: bool = False


@dataclass
class ColumnCleaningConfig:
    target_name: str
    target_type: str = "keep"
    fill_strategy: str = "none"
    custom_fill_value: str = ""
    drop_column: bool = False
    lowercase_text: bool = False


def load_dataframe(uploaded_file: Any | None, sample_path: str | Path | None = None) -> pd.DataFrame | None:
    if uploaded_file is None and sample_path is None:
        return None
    if uploaded_file is not None:
        file_name = uploaded_file.name.lower()
        buffer = BytesIO(uploaded_file.getvalue())
        if file_name.endswith(".csv"):
            return pd.read_csv(buffer)
        if file_name.endswith(".xlsx"):
            return pd.read_excel(buffer)
        raise ValueError("Only CSV and XLSX files are supported.")
    sample = Path(sample_path)
    if sample.suffix.lower() == ".csv":
        return pd.read_csv(sample)
    if sample.suffix.lower() == ".xlsx":
        return pd.read_excel(sample)
    raise ValueError("Sample file must be CSV or XLSX.")


def detect_numeric_like_columns(df: pd.DataFrame, threshold: float = 0.8) -> list[str]:
    columns: list[str] = []
    for column in df.columns:
        series = df[column]
        if pd.api.types.is_numeric_dtype(series) or pd.api.types.is_datetime64_any_dtype(series):
            continue
        cleaned = series.dropna().astype(str).str.replace(",", "", regex=False).str.strip()
        if not cleaned.empty and pd.to_numeric(cleaned, errors="coerce").notna().mean() >= threshold:
            columns.append(column)
    return columns


def detect_datetime_candidates(df: pd.DataFrame, threshold: float = 0.7) -> list[str]:
    columns: list[str] = []
    for column in df.columns:
        series = df[column]
        if pd.api.types.is_datetime64_any_dtype(series):
            columns.append(column)
            continue
        if pd.api.types.is_numeric_dtype(series):
            continue
        cleaned = series.dropna().astype(str).str.strip()
        if not cleaned.empty and pd.to_datetime(cleaned, errors="coerce", format="mixed").notna().mean() >= threshold:
            columns.append(column)
    return columns


def detect_outlier_summary(df: pd.DataFrame) -> dict[str, int]:
    summary: dict[str, int] = {}
    for column in df.select_dtypes(include="number").columns:
        series = df[column].dropna()
        if len(series) < 4:
            continue
        q1 = series.quantile(0.25)
        q3 = series.quantile(0.75)
        iqr = q3 - q1
        if iqr == 0:
            continue
        lower = q1 - 1.5 * iqr
        upper = q3 + 1.5 * iqr
        summary[column] = int(((series < lower) | (series > upper)).sum())
    return summary


def compute_quality_score(rows: int, columns: int, missing_cells: int, duplicate_rows: int, high_missing_columns: int) -> int:
    total_cells = max(rows * max(columns, 1), 1)
    penalty = (missing_cells / total_cells) * 45 + (duplicate_rows / max(rows, 1)) * 30 + min(high_missing_columns * 6, 18)
    return max(0, min(100, round(100 - penalty)))


def profile_dataframe(df: pd.DataFrame) -> dict[str, Any]:
    missing_counts = df.isna().sum().sort_values(ascending=False)
    duplicate_rows = int(df.duplicated().sum())
    numeric_like = detect_numeric_like_columns(df)
    datetime_like = detect_datetime_candidates(df)
    outliers = detect_outlier_summary(df)
    profiles: list[dict[str, Any]] = []
    for column in df.columns:
        series = df[column]
        values = series.dropna().astype(str).value_counts().head(3).index.tolist()
        profiles.append(
            {
                "column": column,
                "dtype": str(series.dtype),
                "missing_pct": round(float(series.isna().mean() * 100), 2),
                "unique_values": int(series.nunique(dropna=True)),
                "sample_values": ", ".join(values),
                "suggested_numeric": column in numeric_like,
                "suggested_datetime": column in datetime_like,
                "outlier_rows": outliers.get(column, 0),
            }
        )
    return {
        "rows": int(df.shape[0]),
        "columns": int(df.shape[1]),
        "missing_cells": int(df.isna().sum().sum()),
        "duplicate_rows": duplicate_rows,
        "numeric_columns": df.select_dtypes(include="number").columns.tolist(),
        "categorical_columns": df.select_dtypes(exclude=["number", "datetime", "datetimetz"]).columns.tolist(),
        "datetime_columns": df.select_dtypes(include=["datetime", "datetimetz"]).columns.tolist(),
        "numeric_like_columns": numeric_like,
        "datetime_candidates": datetime_like,
        "missing_by_column": missing_counts[missing_counts > 0].to_dict(),
        "outlier_summary": outliers,
        "quality_score": compute_quality_score(
            rows=len(df),
            columns=len(df.columns),
            missing_cells=int(df.isna().sum().sum()),
            duplicate_rows=duplicate_rows,
            high_missing_columns=sum(count > (0.4 * len(df)) for count in missing_counts.values) if len(df) else 0,
        ),
        "memory_mb": round(float(df.memory_usage(deep=True).sum() / (1024 * 1024)), 2),
        "column_profiles": profiles,
    }


def default_column_configs(df: pd.DataFrame) -> dict[str, ColumnCleaningConfig]:
    numeric_like = set(detect_numeric_like_columns(df))
    datetime_like = set(detect_datetime_candidates(df))
    configs: dict[str, ColumnCleaningConfig] = {}
    for column in df.columns:
        target_type = "keep"
        if column in numeric_like:
            target_type = "numeric"
        elif column in datetime_like:
            target_type = "datetime"
        configs[column] = ColumnCleaningConfig(target_name=column, target_type=target_type)
    return configs


def resolve_fill_value(series: pd.Series, config: ColumnCleaningConfig) -> Any | None:
    if config.fill_strategy == "median" and pd.api.types.is_numeric_dtype(series):
        return series.median()
    if config.fill_strategy == "mean" and pd.api.types.is_numeric_dtype(series):
        return series.mean()
    if config.fill_strategy == "mode":
        mode = series.dropna().mode()
        return mode.iloc[0] if not mode.empty else None
    if config.fill_strategy == "zero" and pd.api.types.is_numeric_dtype(series):
        return 0
    if config.fill_strategy == "custom":
        return config.custom_fill_value
    return None


def apply_column_configs(df: pd.DataFrame, column_configs: dict[str, ColumnCleaningConfig]) -> tuple[pd.DataFrame, list[str]]:
    cleaned = df.copy()
    operations: list[str] = []
    for column in list(cleaned.columns):
        config = column_configs.get(column)
        if config is None:
            continue
        if config.drop_column:
            cleaned = cleaned.drop(columns=[column])
            operations.append(f"Dropped column `{column}`.")
            continue
        if config.lowercase_text:
            cleaned[column] = cleaned[column].apply(lambda value: value.lower() if isinstance(value, str) else value)
            operations.append(f"Lowercased values in `{column}`.")
        if config.target_type == "numeric":
            converted = pd.to_numeric(cleaned[column].astype(str).str.replace(",", "", regex=False), errors="coerce")
            if converted.notna().sum() > 0:
                cleaned[column] = converted
                operations.append(f"Converted `{column}` to numeric.")
        elif config.target_type == "datetime":
            parsed = pd.to_datetime(cleaned[column], errors="coerce", format="mixed")
            if parsed.notna().sum() > 0:
                cleaned[column] = parsed
                operations.append(f"Converted `{column}` to datetime.")
        elif config.target_type == "string":
            cleaned[column] = cleaned[column].astype("string")
            operations.append(f"Converted `{column}` to string.")
        if config.fill_strategy != "none" and cleaned[column].isna().sum() > 0:
            fill_value = resolve_fill_value(cleaned[column], config)
            if fill_value is not None:
                cleaned[column] = cleaned[column].fillna(fill_value)
                operations.append(f"Filled missing values in `{column}` using {config.fill_strategy}.")
    rename_map = {
        column: config.target_name
        for column, config in column_configs.items()
        if column in cleaned.columns and config.target_name and config.target_name != column
    }
    if rename_map:
        cleaned = cleaned.rename(columns=rename_map)
        operations.append(
            "Renamed columns: " + ", ".join(f"`{old}` to `{new}`" for old, new in rename_map.items()) + "."
        )
    return cleaned, list(dict.fromkeys(operations))


def apply_cleaning(
    df: pd.DataFrame,
    options: CleaningOptions,
    column_configs: dict[str, ColumnCleaningConfig] | None = None,
) -> tuple[pd.DataFrame, list[str]]:
    cleaned = df.copy()
    operations: list[str] = []
    column_configs = column_configs or default_column_configs(cleaned)
    if options.trim_whitespace:
        for column in cleaned.select_dtypes(include=["object", "string"]).columns:
            cleaned[column] = cleaned[column].apply(lambda value: value.strip() if isinstance(value, str) else value)
        operations.append("Trimmed leading and trailing whitespace from text columns.")
    if options.standardize_missing_markers:
        cleaned = cleaned.replace(to_replace=r"^\s*$", value=pd.NA, regex=True)
        for marker in MISSING_MARKERS:
            cleaned = cleaned.replace(marker, pd.NA)
            cleaned = cleaned.replace(marker.upper(), pd.NA)
            cleaned = cleaned.replace(marker.title(), pd.NA)
        operations.append("Normalized common missing-value markers into nulls.")
    if options.lowercase_text:
        for column in cleaned.select_dtypes(include=["object", "string"]).columns:
            cleaned[column] = cleaned[column].apply(lambda value: value.lower() if isinstance(value, str) else value)
        operations.append("Lowercased text columns globally.")
    if options.convert_numeric_like:
        for column in detect_numeric_like_columns(cleaned):
            cleaned[column] = pd.to_numeric(cleaned[column].astype(str).str.replace(",", "", regex=False), errors="coerce")
        operations.append("Auto-converted numeric-like columns where possible.")
    if options.convert_datetime_like:
        for column in detect_datetime_candidates(cleaned):
            cleaned[column] = pd.to_datetime(cleaned[column], errors="coerce", format="mixed")
        operations.append("Auto-converted datetime-like columns where possible.")
    if options.drop_empty_rows:
        before = len(cleaned)
        cleaned = cleaned.dropna(how="all")
        if before - len(cleaned):
            operations.append(f"Dropped {before - len(cleaned)} fully empty rows.")
    if options.drop_duplicates:
        before = len(cleaned)
        cleaned = cleaned.drop_duplicates()
        if before - len(cleaned):
            operations.append(f"Removed {before - len(cleaned)} duplicate rows.")
    cleaned, column_ops = apply_column_configs(cleaned, column_configs)
    operations.extend(column_ops)
    if options.fill_numeric_missing:
        filled = []
        for column in cleaned.select_dtypes(include="number").columns:
            if cleaned[column].isna().any():
                cleaned[column] = cleaned[column].fillna(cleaned[column].median())
                filled.append(column)
        if filled:
            operations.append("Filled numeric missing values with medians for: " + ", ".join(filled) + ".")
    if options.fill_categorical_missing:
        filled = []
        for column in cleaned.select_dtypes(exclude="number").columns:
            if cleaned[column].isna().any():
                mode = cleaned[column].dropna().mode()
                if not mode.empty:
                    cleaned[column] = cleaned[column].fillna(mode.iloc[0])
                    filled.append(column)
        if filled:
            operations.append("Filled categorical missing values with modes for: " + ", ".join(filled) + ".")
    return cleaned, list(dict.fromkeys(operations))


def build_recommendations(profile: dict[str, Any]) -> list[str]:
    recommendations: list[str] = []
    if profile["quality_score"] < 80:
        recommendations.append(f"Data health score is {profile['quality_score']}/100; clean high-missing and duplicate-heavy fields first.")
    if profile["missing_cells"] > 0:
        recommendations.append(f"Missing values remain in {len(profile['missing_by_column'])} columns; choose fill or retention rules explicitly.")
    if profile["duplicate_rows"] > 0:
        recommendations.append("Duplicate rows were detected, so deduplication should stay enabled before publishing insights.")
    if profile["numeric_like_columns"]:
        recommendations.append("These text columns should likely be numeric: " + ", ".join(profile["numeric_like_columns"]) + ".")
    if profile["datetime_candidates"]:
        recommendations.append("These columns are suitable for time-series dashboards: " + ", ".join(profile["datetime_candidates"]) + ".")
    outlier_columns = [column for column, count in profile["outlier_summary"].items() if count > 0]
    if outlier_columns:
        recommendations.append("Potential numeric outliers were flagged in: " + ", ".join(outlier_columns[:4]) + ".")
    if not recommendations:
        recommendations.append("Dataset health looks strong; focus next on metric design and dashboard storytelling.")
    return recommendations


def dataframe_to_csv_bytes(df: pd.DataFrame) -> bytes:
    return df.to_csv(index=False).encode("utf-8")


def report_payload(
    dataset_name: str,
    raw_profile: dict[str, Any],
    cleaned_profile: dict[str, Any],
    operations: list[str],
    global_options: CleaningOptions,
    column_configs: dict[str, ColumnCleaningConfig],
) -> dict[str, Any]:
    return {
        "dataset_name": dataset_name,
        "raw_profile": raw_profile,
        "cleaned_profile": cleaned_profile,
        "operations": operations,
        "global_options": asdict(global_options),
        "column_configs": {column: asdict(config) for column, config in column_configs.items()},
        "recommendations": build_recommendations(cleaned_profile),
    }


def create_markdown_report(dataset_name: str, raw_profile: dict[str, Any], cleaned_profile: dict[str, Any], operations: list[str]) -> str:
    lines = [
        f"# Data Prep Dashboard Report: {dataset_name}",
        "",
        "## Raw dataset summary",
        f"- Rows: {raw_profile['rows']}",
        f"- Columns: {raw_profile['columns']}",
        f"- Missing cells: {raw_profile['missing_cells']}",
        f"- Duplicate rows: {raw_profile['duplicate_rows']}",
        f"- Quality score: {raw_profile['quality_score']}/100",
        "",
        "## Cleaning operations applied",
    ]
    lines.extend(f"- {item}" for item in operations) if operations else lines.append("- No cleaning operations were applied.")
    lines.extend(
        [
            "",
            "## Cleaned dataset summary",
            f"- Rows: {cleaned_profile['rows']}",
            f"- Columns: {cleaned_profile['columns']}",
            f"- Missing cells: {cleaned_profile['missing_cells']}",
            f"- Duplicate rows: {cleaned_profile['duplicate_rows']}",
            f"- Quality score: {cleaned_profile['quality_score']}/100",
            "",
            "## Recommendations",
        ]
    )
    lines.extend(f"- {item}" for item in build_recommendations(cleaned_profile))
    return "\n".join(lines)
