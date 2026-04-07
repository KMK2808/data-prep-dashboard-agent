from __future__ import annotations

from dataclasses import dataclass
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


def load_dataframe(uploaded_file: Any | None, sample_path: str | Path | None = None) -> pd.DataFrame | None:
    if uploaded_file is None and sample_path is None:
        return None

    if uploaded_file is not None:
        file_name = uploaded_file.name.lower()
        file_bytes = uploaded_file.getvalue()
        buffer = BytesIO(file_bytes)
        if file_name.endswith(".csv"):
            return pd.read_csv(buffer)
        if file_name.endswith(".xlsx"):
            return pd.read_excel(buffer)
        raise ValueError("Only CSV and XLSX files are supported in this prototype.")

    sample_path = Path(sample_path)
    if sample_path.suffix.lower() == ".csv":
        return pd.read_csv(sample_path)
    if sample_path.suffix.lower() == ".xlsx":
        return pd.read_excel(sample_path)
    raise ValueError("Sample file must be CSV or XLSX.")


def profile_dataframe(df: pd.DataFrame) -> dict[str, Any]:
    missing_counts = df.isna().sum().sort_values(ascending=False)
    duplicate_rows = int(df.duplicated().sum())
    numeric_columns = df.select_dtypes(include="number").columns.tolist()
    categorical_columns = df.select_dtypes(exclude="number").columns.tolist()
    datetime_candidates = detect_datetime_candidates(df)
    numeric_like_columns = detect_numeric_like_columns(df)

    column_profiles: list[dict[str, Any]] = []
    for column in df.columns:
        series = df[column]
        top_values = (
            series.astype(str)
            .replace("nan", pd.NA)
            .dropna()
            .value_counts()
            .head(3)
            .to_dict()
        )
        column_profiles.append(
            {
                "column": column,
                "dtype": str(series.dtype),
                "missing": int(series.isna().sum()),
                "missing_pct": round(float(series.isna().mean() * 100), 2),
                "unique_values": int(series.nunique(dropna=True)),
                "suggested_numeric": column in numeric_like_columns,
                "suggested_datetime": column in datetime_candidates,
                "top_values": top_values,
            }
        )

    return {
        "rows": int(df.shape[0]),
        "columns": int(df.shape[1]),
        "missing_cells": int(df.isna().sum().sum()),
        "duplicate_rows": duplicate_rows,
        "numeric_columns": numeric_columns,
        "categorical_columns": categorical_columns,
        "datetime_candidates": datetime_candidates,
        "numeric_like_columns": numeric_like_columns,
        "column_profiles": column_profiles,
        "missing_by_column": missing_counts[missing_counts > 0].to_dict(),
    }


def detect_numeric_like_columns(df: pd.DataFrame, threshold: float = 0.8) -> list[str]:
    numeric_like: list[str] = []
    for column in df.columns:
        series = df[column]
        if pd.api.types.is_numeric_dtype(series):
            continue
        cleaned = series.dropna().astype(str).str.replace(",", "", regex=False).str.strip()
        if cleaned.empty:
            continue
        converted = pd.to_numeric(cleaned, errors="coerce")
        if (converted.notna().mean()) >= threshold:
            numeric_like.append(column)
    return numeric_like


def detect_datetime_candidates(df: pd.DataFrame, threshold: float = 0.7) -> list[str]:
    candidates: list[str] = []
    for column in df.columns:
        series = df[column]
        if pd.api.types.is_datetime64_any_dtype(series):
            candidates.append(column)
            continue
        if pd.api.types.is_numeric_dtype(series):
            continue
        cleaned = series.dropna().astype(str).str.strip()
        if cleaned.empty:
            continue
        parsed = pd.to_datetime(cleaned, errors="coerce")
        if (parsed.notna().mean()) >= threshold:
            candidates.append(column)
    return candidates


def apply_cleaning(df: pd.DataFrame, options: CleaningOptions) -> tuple[pd.DataFrame, list[str]]:
    cleaned = df.copy()
    operations: list[str] = []

    if options.trim_whitespace:
        object_columns = cleaned.select_dtypes(include=["object", "string"]).columns
        for column in object_columns:
            cleaned[column] = cleaned[column].apply(
                lambda value: value.strip() if isinstance(value, str) else value
            )
        operations.append("Trimmed leading and trailing whitespace from text columns.")

    if options.standardize_missing_markers:
        cleaned = cleaned.replace(
            to_replace=r"^\s*$",
            value=pd.NA,
            regex=True,
        )
        for marker in MISSING_MARKERS:
            cleaned = cleaned.replace(marker, pd.NA)
            cleaned = cleaned.replace(marker.upper(), pd.NA)
            cleaned = cleaned.replace(marker.title(), pd.NA)
        operations.append("Standardized common missing-value markers to nulls.")

    if options.convert_numeric_like:
        converted_columns = []
        for column in detect_numeric_like_columns(cleaned):
            converted = pd.to_numeric(
                cleaned[column].astype(str).str.replace(",", "", regex=False),
                errors="coerce",
            )
            if converted.notna().sum() > 0:
                cleaned[column] = converted
                converted_columns.append(column)
        if converted_columns:
            operations.append(
                "Converted numeric-like columns: " + ", ".join(converted_columns) + "."
            )

    if options.convert_datetime_like:
        parsed_columns = []
        for column in detect_datetime_candidates(cleaned):
            parsed = pd.to_datetime(cleaned[column], errors="coerce")
            if parsed.notna().sum() > 0:
                cleaned[column] = parsed
                parsed_columns.append(column)
        if parsed_columns:
            operations.append(
                "Parsed datetime-like columns: " + ", ".join(parsed_columns) + "."
            )

    if options.drop_empty_rows:
        before = len(cleaned)
        cleaned = cleaned.dropna(how="all")
        removed = before - len(cleaned)
        if removed:
            operations.append(f"Dropped {removed} fully empty rows.")

    if options.drop_duplicates:
        before = len(cleaned)
        cleaned = cleaned.drop_duplicates()
        removed = before - len(cleaned)
        if removed:
            operations.append(f"Removed {removed} duplicate rows.")

    if options.fill_numeric_missing:
        filled_columns = []
        for column in cleaned.select_dtypes(include="number").columns:
            if cleaned[column].isna().any():
                median_value = cleaned[column].median()
                cleaned[column] = cleaned[column].fillna(median_value)
                filled_columns.append(column)
        if filled_columns:
            operations.append(
                "Filled numeric missing values with the median for: "
                + ", ".join(filled_columns)
                + "."
            )

    if options.fill_categorical_missing:
        filled_columns = []
        for column in cleaned.select_dtypes(exclude="number").columns:
            if cleaned[column].isna().any():
                non_null = cleaned[column].dropna()
                if non_null.empty:
                    continue
                mode_value = non_null.mode().iloc[0]
                cleaned[column] = cleaned[column].fillna(mode_value)
                filled_columns.append(column)
        if filled_columns:
            operations.append(
                "Filled categorical missing values with the mode for: "
                + ", ".join(filled_columns)
                + "."
            )

    return cleaned, operations


def build_recommendations(profile: dict[str, Any]) -> list[str]:
    recommendations: list[str] = []
    if profile["missing_cells"] > 0:
        recommendations.append(
            f"Missing values detected in {len(profile['missing_by_column'])} columns; review imputation or null handling."
        )
    if profile["duplicate_rows"] > 0:
        recommendations.append("Duplicate rows found; removing duplicates is recommended.")
    if profile["numeric_like_columns"]:
        recommendations.append(
            "Some text columns look numeric and should be coerced before analysis: "
            + ", ".join(profile["numeric_like_columns"])
            + "."
        )
    if profile["datetime_candidates"]:
        recommendations.append(
            "Date-like columns detected for trend analysis: "
            + ", ".join(profile["datetime_candidates"])
            + "."
        )
    if not recommendations:
        recommendations.append("Dataset looks fairly clean; focus on exploratory metrics and dashboard design.")
    return recommendations


def dataframe_to_csv_bytes(df: pd.DataFrame) -> bytes:
    return df.to_csv(index=False).encode("utf-8")


def create_markdown_report(
    dataset_name: str,
    raw_profile: dict[str, Any],
    cleaned_profile: dict[str, Any],
    operations: list[str],
) -> str:
    lines = [
        f"# Data Prep Dashboard Report: {dataset_name}",
        "",
        "## Raw dataset summary",
        f"- Rows: {raw_profile['rows']}",
        f"- Columns: {raw_profile['columns']}",
        f"- Missing cells: {raw_profile['missing_cells']}",
        f"- Duplicate rows: {raw_profile['duplicate_rows']}",
        "",
        "## Cleaning operations applied",
    ]
    if operations:
        lines.extend(f"- {item}" for item in operations)
    else:
        lines.append("- No cleaning operations were applied.")

    lines.extend(
        [
            "",
            "## Cleaned dataset summary",
            f"- Rows: {cleaned_profile['rows']}",
            f"- Columns: {cleaned_profile['columns']}",
            f"- Missing cells: {cleaned_profile['missing_cells']}",
            f"- Duplicate rows: {cleaned_profile['duplicate_rows']}",
            "",
            "## Recommendations",
        ]
    )
    lines.extend(f"- {item}" for item in build_recommendations(cleaned_profile))
    return "\n".join(lines)
