from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd


def slugify(value: str) -> str:
    cleaned = "".join(char.lower() if char.isalnum() else "-" for char in value)
    while "--" in cleaned:
        cleaned = cleaned.replace("--", "-")
    return cleaned.strip("-") or "dataset"


def save_run_artifacts(
    reports_dir: str | Path,
    dataset_name: str,
    cleaned_df: pd.DataFrame,
    markdown_report: str,
    payload: dict[str, Any],
) -> Path:
    reports_path = Path(reports_dir)
    run_dir = reports_path / f"{datetime.now().strftime('%Y%m%d-%H%M%S')}-{slugify(dataset_name)}"
    run_dir.mkdir(parents=True, exist_ok=True)
    cleaned_df.to_csv(run_dir / "cleaned_dataset.csv", index=False)
    (run_dir / "report.md").write_text(markdown_report, encoding="utf-8")
    (run_dir / "run_summary.json").write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    return run_dir


def load_saved_runs(reports_dir: str | Path) -> list[dict[str, Any]]:
    reports_path = Path(reports_dir)
    if not reports_path.exists():
        return []
    runs: list[dict[str, Any]] = []
    for run_dir in sorted(reports_path.iterdir(), reverse=True):
        summary_file = run_dir / "run_summary.json"
        if not run_dir.is_dir() or not summary_file.exists():
            continue
        payload = json.loads(summary_file.read_text(encoding="utf-8"))
        runs.append(
            {
                "run_dir": str(run_dir),
                "dataset_name": payload.get("dataset_name", run_dir.name),
                "quality_score": payload.get("cleaned_profile", {}).get("quality_score"),
                "rows": payload.get("cleaned_profile", {}).get("rows"),
            }
        )
    return runs
