"""Nextier Merged Copper Stage Labeling v1.

Reads row-level Bronze labels (``merged_bronze_labels_v2.bronze_continuous``) and
runs the canonical nextier-dash Copper layer APIs to produce the three stage
ordinal columns, written to ``merged_copper_labels_v1``:

- ``copper_provisional``  — eager, confirmed at ``MID_TRIAL`` (live visibility).
- ``copper_confirmed``    — strict/sticky, confirmed at ``MID``.
- ``copper_continuous``   — canonical final, closed-only contextual expansion.

The Copper APIs are pure consumers of Bronze labels; each ``apply_*_column``
helper groups by ``name`` and sorts by ``datetime_fmt`` internally.
"""

from __future__ import annotations

from datetime import datetime, timezone
from hashlib import sha256
from typing import Any
from uuid import uuid4

import pandas as pd
import polars as pl
from prefect import flow, get_run_logger

from nextier_core.assign_copper_provisional import apply_provisional_column
from nextier_core.assign_copper_confirmed import apply_confirmed_column
from nextier_core.expand_copper_contextual import apply_continuous_column
from nixdlt.workflow_sdk.platform_tasks import (
    delete_featurestore_records,
    query_store,
    write_featurestore,
)


BRONZE_KEY = "merged_bronze_labels_v2"
COPPER_KEY = "merged_copper_labels_v1"
MANIFEST_KEY = "merged_copper_processing_manifest_v1"
STAGE_NAME = "copper_labels"

DATETIME_COL = "datetime_fmt"


def _now() -> str:
    return datetime.now(timezone.utc).replace(tzinfo=None).isoformat(sep=" ")


def _fmt(value: Any) -> str | None:
    if value is None or pd.isna(value):
        return None
    ts = pd.Timestamp(value)
    if ts.tzinfo is not None:
        ts = ts.tz_convert(None)
    return ts.strftime("%Y-%m-%d %H:%M:%S.%f")


def _to_pandas(result: Any) -> pd.DataFrame:
    if result is None:
        return pd.DataFrame()
    if isinstance(result, pl.DataFrame):
        return result.to_pandas()
    if isinstance(result, pd.DataFrame):
        return result
    return pd.DataFrame(result)


def _signature(well_df: pd.DataFrame, algorithm_version: str) -> str:
    payload = "|".join(
        str(v)
        for v in (
            algorithm_version,
            len(well_df),
            _fmt(well_df[DATETIME_COL].min()),
            _fmt(well_df[DATETIME_COL].max()),
        )
    )
    return sha256(payload.encode()).hexdigest()


@flow(name="nextier-merged-copper-stage-labeling-v1")
async def nextier_merged_copper_stage_labeling_v1_flow(
    mode: str = "incremental",
    max_wells: int = 25,
    well_name: str | None = None,
    algorithm_version: str = "copper-v1",
    dry_run: bool = False,
    workspace_id: int | None = None,
) -> dict[str, Any]:
    logger = get_run_logger()
    if mode not in {"live", "incremental", "historical", "rebuild"}:
        raise ValueError("mode must be live, incremental, historical, or rebuild")

    bronze = _to_pandas(
        await query_store(
            sql=f"""
                SELECT name, datetime_fmt, bronze_continuous, fleet_name, pad_name
                FROM featurestore:{BRONZE_KEY}
                WHERE (:well_name IS NULL OR name = :well_name)
                ORDER BY name, datetime_fmt
            """,
            workspace_id=workspace_id,
            params={"well_name": well_name},
        )
    )
    if bronze.empty:
        logger.info("Copper labeling: no bronze rows for mode=%s", mode)
        return {"mode": mode, "wells_processed": 0, "rows_written": 0}

    bronze[DATETIME_COL] = pd.to_datetime(bronze[DATETIME_COL], errors="coerce")
    bronze = bronze.dropna(subset=["name", DATETIME_COL])
    if "bronze_continuous" not in bronze.columns:
        raise ValueError("bronze_continuous column missing from merged_bronze_labels_v2")

    wells = list(bronze["name"].astype(str).unique())[: int(max_wells)]
    now = _now()
    results: list[dict[str, Any]] = []

    for name in wells:
        well_df = bronze[bronze["name"].astype(str) == name].copy()
        # Copper apply_* helpers expect columns: name, bronze_continuous, datetime_fmt.
        labeled = apply_provisional_column(well_df)
        labeled = apply_confirmed_column(labeled)
        labeled = apply_continuous_column(labeled)

        run_id = str(uuid4())
        out = pd.DataFrame(
            {
                "name": labeled["name"].astype(str).to_numpy(),
                "datetime_fmt": labeled[DATETIME_COL].map(_fmt).to_numpy(),
                "copper_provisional": pd.to_numeric(
                    labeled["copper_provisional"], errors="coerce"
                ).fillna(0).astype(int).to_numpy(),
                "copper_confirmed": pd.to_numeric(
                    labeled["copper_confirmed"], errors="coerce"
                ).fillna(0).astype(int).to_numpy(),
                "copper_continuous": pd.to_numeric(
                    labeled["copper_continuous"], errors="coerce"
                ).fillna(0).astype(int).to_numpy(),
                "fleet_name": labeled.get("fleet_name"),
                "pad_name": labeled.get("pad_name"),
                "algorithm_version": algorithm_version,
                "processing_run_id": run_id,
                "created_at": now,
            }
        )
        rows_written = int(len(out))

        if not dry_run and rows_written:
            await delete_featurestore_records(
                featurestore_key=COPPER_KEY,
                workspace_id=workspace_id,
                filters=[{"field": "name", "op": "eq", "value": name}],
            )
            await write_featurestore(
                featurestore_key=COPPER_KEY,
                workspace_id=workspace_id,
                df=pl.from_pandas(out),
                upsert=True,
            )
            manifest = pd.DataFrame(
                [
                    {
                        "fleet_name": well_df["fleet_name"].iloc[0]
                        if "fleet_name" in well_df and not well_df.empty
                        else None,
                        "pad_name": well_df["pad_name"].iloc[0]
                        if "pad_name" in well_df and not well_df.empty
                        else None,
                        "name": name,
                        "stage_name": STAGE_NAME,
                        "mode": mode,
                        "algorithm_version": algorithm_version,
                        "active_run_id": run_id,
                        "source_signature": _signature(well_df, algorithm_version),
                        "source_rows": int(len(well_df)),
                        "lookback_start_ts": None,
                        "first_source_ts": _fmt(well_df[DATETIME_COL].min()),
                        "last_source_ts": _fmt(well_df[DATETIME_COL].max()),
                        "latest_source_updated_at": now,
                        "processed_at": now,
                        "status": "active",
                    }
                ]
            )
            await write_featurestore(
                featurestore_key=MANIFEST_KEY,
                workspace_id=workspace_id,
                df=pl.from_pandas(manifest),
                upsert=True,
            )
        results.append({"name": name, "rows": rows_written})
        logger.info("Copper labeled name=%s rows=%s dry_run=%s", name, rows_written, dry_run)

    total_rows = sum(r["rows"] for r in results)
    logger.info(
        "Copper labeling completed mode=%s wells=%s rows=%s",
        mode,
        len(results),
        total_rows,
    )
    return {
        "mode": mode,
        "wells_processed": len(results),
        "rows_written": total_rows,
        "has_more": len(wells) == int(max_wells),
    }
