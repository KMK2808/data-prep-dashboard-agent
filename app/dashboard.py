from __future__ import annotations

from typing import Any

import pandas as pd
import plotly.express as px


def build_dashboard_figures(df: pd.DataFrame) -> dict[str, Any]:
    figures: dict[str, Any] = {}

    numeric_columns = df.select_dtypes(include="number").columns.tolist()
    categorical_columns = df.select_dtypes(exclude="number").columns.tolist()
    datetime_columns = df.select_dtypes(include=["datetime", "datetimetz"]).columns.tolist()

    if numeric_columns:
        histograms = []
        for column in numeric_columns[:3]:
            fig = px.histogram(
                df,
                x=column,
                nbins=20,
                title=f"Distribution of {column}",
                template="plotly_white",
            )
            histograms.append(fig)
        figures["histograms"] = histograms

    if categorical_columns:
        column = categorical_columns[0]
        top_values = (
            df[column]
            .astype(str)
            .replace("nan", pd.NA)
            .dropna()
            .value_counts()
            .head(10)
            .reset_index()
        )
        if not top_values.empty:
            top_values.columns = [column, "count"]
            figures["top_categories"] = px.bar(
                top_values,
                x=column,
                y="count",
                title=f"Top categories in {column}",
                template="plotly_white",
            )

    if datetime_columns and numeric_columns:
        date_column = datetime_columns[0]
        value_column = numeric_columns[0]
        trend_df = (
            df[[date_column, value_column]]
            .dropna()
            .assign(date=lambda data: data[date_column].dt.date)
            .groupby("date", as_index=False)[value_column]
            .sum()
        )
        if not trend_df.empty:
            figures["time_series"] = px.line(
                trend_df,
                x="date",
                y=value_column,
                markers=True,
                title=f"{value_column} over time",
                template="plotly_white",
            )

    if len(numeric_columns) >= 2:
        figures["scatter"] = px.scatter(
            df,
            x=numeric_columns[0],
            y=numeric_columns[1],
            color=categorical_columns[0] if categorical_columns else None,
            title=f"{numeric_columns[0]} vs {numeric_columns[1]}",
            template="plotly_white",
        )

    return figures


def build_kpi_summary(raw_df: pd.DataFrame, cleaned_df: pd.DataFrame) -> dict[str, Any]:
    duplicate_delta = int(raw_df.duplicated().sum() - cleaned_df.duplicated().sum())
    return {
        "rows": int(cleaned_df.shape[0]),
        "columns": int(cleaned_df.shape[1]),
        "missing_cells": int(cleaned_df.isna().sum().sum()),
        "duplicates_removed": max(duplicate_delta, 0),
    }
