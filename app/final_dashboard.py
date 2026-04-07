from __future__ import annotations

from typing import Any

import duckdb
import pandas as pd
import plotly.express as px

COLOR_SEQUENCE = ["#1f6f78", "#e59560", "#6a994e", "#bc4749", "#3d5a80", "#c97b63"]


def q(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def style_figure(fig):
    fig.update_layout(
        template="plotly_white",
        colorway=COLOR_SEQUENCE,
        paper_bgcolor="rgba(255,255,255,0.0)",
        plot_bgcolor="rgba(255,255,255,0.92)",
        font={"color": "#17323b"},
        title={"font": {"size": 18, "color": "#0f4c5c"}},
        margin={"l": 30, "r": 20, "t": 56, "b": 30},
    )
    return fig


def build_kpi_summary(raw_df: pd.DataFrame, cleaned_df: pd.DataFrame, cleaned_profile: dict[str, Any]) -> dict[str, Any]:
    return {
        "rows": int(cleaned_df.shape[0]),
        "columns": int(cleaned_df.shape[1]),
        "missing_cells": int(cleaned_df.isna().sum().sum()),
        "duplicates_removed": max(int(raw_df.duplicated().sum() - cleaned_df.duplicated().sum()), 0),
        "quality_score": int(cleaned_profile["quality_score"]),
    }


def default_dashboard_selections(profile: dict[str, Any]) -> dict[str, str | None]:
    metric = profile["numeric_columns"][0] if profile["numeric_columns"] else None
    dimension = profile["categorical_columns"][0] if profile["categorical_columns"] else None
    date_column = profile["datetime_columns"][0] if profile["datetime_columns"] else None
    return {"metric": metric, "dimension": dimension, "date_column": date_column}


def build_dashboard_bundle(
    df: pd.DataFrame,
    metric: str | None,
    dimension: str | None,
    date_column: str | None,
    aggregation: str,
    chart_type: str,
) -> dict[str, Any]:
    bundle: dict[str, Any] = {"figures": [], "tables": {}}
    if df.empty:
        return bundle

    con = duckdb.connect(database=":memory:")
    con.register("cleaned_df", df)

    if metric and dimension:
        grouped = con.execute(
            f"""
            SELECT {q(dimension)} AS dimension_value,
                   {aggregation}({q(metric)}) AS metric_value
            FROM cleaned_df
            WHERE {q(dimension)} IS NOT NULL AND {q(metric)} IS NOT NULL
            GROUP BY 1
            ORDER BY metric_value DESC
            LIMIT 10
            """
        ).df()
        bundle["tables"]["top_segments"] = grouped
        if not grouped.empty:
            if chart_type == "Bar":
                fig = px.bar(grouped, x="dimension_value", y="metric_value", title=f"{aggregation.title()} of {metric} by {dimension}", color_discrete_sequence=COLOR_SEQUENCE)
            elif chart_type == "Treemap":
                fig = px.treemap(grouped, path=["dimension_value"], values="metric_value", title=f"{aggregation.title()} of {metric} by {dimension}", color_discrete_sequence=COLOR_SEQUENCE)
            else:
                fig = px.pie(grouped, names="dimension_value", values="metric_value", title=f"Share of {metric} by {dimension}", color_discrete_sequence=COLOR_SEQUENCE)
            bundle["figures"].append(style_figure(fig))

    if metric and date_column:
        time_df = con.execute(
            f"""
            SELECT CAST(date_trunc('day', {q(date_column)}) AS DATE) AS day_value,
                   {aggregation}({q(metric)}) AS metric_value
            FROM cleaned_df
            WHERE {q(date_column)} IS NOT NULL AND {q(metric)} IS NOT NULL
            GROUP BY 1
            ORDER BY 1
            """
        ).df()
        bundle["tables"]["time_series"] = time_df
        if not time_df.empty:
            bundle["figures"].append(style_figure(px.line(time_df, x="day_value", y="metric_value", markers=True, title=f"{aggregation.title()} of {metric} over time", color_discrete_sequence=COLOR_SEQUENCE)))

    if metric:
        bundle["figures"].append(style_figure(px.histogram(df, x=metric, nbins=25, title=f"Distribution of {metric}", color_discrete_sequence=COLOR_SEQUENCE)))

    numeric_columns = df.select_dtypes(include="number").columns.tolist()
    if len(numeric_columns) >= 2:
        bundle["figures"].append(style_figure(px.scatter(df, x=numeric_columns[0], y=numeric_columns[1], color=dimension if dimension in df.columns else None, title=f"{numeric_columns[0]} vs {numeric_columns[1]}", color_discrete_sequence=COLOR_SEQUENCE)))

    con.close()
    return bundle
