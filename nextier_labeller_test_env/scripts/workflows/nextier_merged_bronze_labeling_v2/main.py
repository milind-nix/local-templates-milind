"""Nextier Merged Bronze Labeling v2.

Runs the canonical nextier-dash Bronze substage FSM
(``nextier_core.bronze_substage_labeling.label_bronze_substages_in_window``)
over merged fleet-stream telemetry and writes row-level ``bronze_continuous``
into ``merged_bronze_labels_v2``.

Windows for the FSM are cut with the same time-differential gap rule the rest of
the pipeline uses (a gap >= ``td_gap_seconds`` between consecutive rows starts a
new window). Copper re-counts stages downstream from ``bronze_continuous``; these
windows only bound the FSM.

Modes:
- ``live``        : only wells with telemetry in the last ``lookback_hours`` * small overlap.
- ``incremental`` : rolling lookback window (default 72h) to absorb late-arriving points.
- ``historical``  : wide backfill for selected wells (no time bound).
- ``rebuild``     : historical + force overwrite even if the source signature is unchanged.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from hashlib import sha256
from typing import Any
from uuid import uuid4

import pandas as pd
import polars as pl
from prefect import flow, get_run_logger

from nextier_core.bronze_substage_labeling import label_bronze_substages_in_window
from nixdlt.workflow_sdk.platform_tasks import (
    delete_featurestore_records,
    query_store,
    write_featurestore,
)


SOURCE_KEY = "merged_fleet_stream_customer_full_v2"
BRONZE_KEY = "merged_bronze_labels_v2"
MANIFEST_KEY = "merged_copper_processing_manifest_v1"
STAGE_NAME = "bronze"
BRONZE_MODEL_NAME = "nextier_dash_bronze"

RATE_COL = "rate_slurry"
PRESSURE_COL = "press_mainline"
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


def _assign_gap_windows(well_df: pd.DataFrame, gap_seconds: float) -> pd.Series:
    """Ordinal window id per row: a gap >= gap_seconds starts a new window."""
    ts = well_df[DATETIME_COL]
    diff = ts.diff().dt.total_seconds()
    breaks = (diff.isna()) | (diff >= gap_seconds)
    return breaks.cumsum().astype(int)


def _label_well(well_df: pd.DataFrame, algorithm_version: str, run_id: str) -> pd.DataFrame:
    """Segment one well into gap-bounded windows and label each with the Bronze FSM."""
    well_df = well_df.sort_values(DATETIME_COL, kind="mergesort").reset_index(drop=True)
    well_df["__window__"] = _assign_gap_windows(well_df, gap_seconds=3600.0)
    now = _now()
    frames: list[pd.DataFrame] = []
    for _, window_df in well_df.groupby("__window__", sort=True):
        window_df = window_df.reset_index(drop=True)
        if window_df.empty:
            continue
        labels_out = label_bronze_substages_in_window(
            window_df,
            start_pos=0,
            end_pos=len(window_df) - 1,
            rate_col=RATE_COL,
            pressure_col=PRESSURE_COL,
            datetime_col=DATETIME_COL,
        )
        labels = labels_out.get("labels") if isinstance(labels_out, dict) else None
        if labels is None:
            continue
        frames.append(
            pd.DataFrame(
                {
                    "name": window_df["name"].astype(str).to_numpy(),
                    "datetime_fmt": window_df[DATETIME_COL].map(_fmt).to_numpy(),
                    "bronze_continuous": pd.Series(labels)
                    .fillna("NA")
                    .astype(str)
                    .to_numpy(),
                    "rate_slurry": pd.to_numeric(
                        window_df.get(RATE_COL), errors="coerce"
                    ).to_numpy(),
                    "press_mainline": pd.to_numeric(
                        window_df.get(PRESSURE_COL), errors="coerce"
                    ).to_numpy(),
                    "prop_conc_blend_denso": pd.to_numeric(
                        window_df.get("prop_conc_blend_denso"), errors="coerce"
                    ).to_numpy(),
                    "fleet_name": window_df.get("fleet_name"),
                    "pad_name": window_df.get("pad_name"),
                    "stage_seq": int(window_df["__window__"].iloc[0]),
                    "algorithm_version": algorithm_version,
                    "processing_run_id": run_id,
                    "created_at": now,
                }
            )
        )
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def _time_bound(mode: str, lookback_hours: int) -> str | None:
    """Lower bound on record_ts for the source query, or None for full history."""
    if mode in {"historical", "rebuild"}:
        return None
    hours = 6 if mode == "live" else int(lookback_hours)
    return _fmt(datetime.now(timezone.utc) - timedelta(hours=hours))


@flow(name="nextier-merged-bronze-labeling-v2")
async def nextier_merged_bronze_labeling_v2_flow(
    mode: str = "incremental",
    max_wells: int = 25,
    well_name: str | None = None,
    algorithm_version: str = "bronze-v2",
    lookback_hours: int = 72,
    td_gap_seconds: int = 3600,
    dry_run: bool = False,
    workspace_id: int | None = None,
) -> dict[str, Any]:
    logger = get_run_logger()
    if mode not in {"live", "incremental", "historical", "rebuild"}:
        raise ValueError("mode must be live, incremental, historical, or rebuild")

    lower_ts = _time_bound(mode, lookback_hours)
    telemetry = _to_pandas(
        await query_store(
            sql=f"""
                SELECT name, record_ts, rate_slurry, press_mainline,
                       prop_conc_blend_denso, fleet_name, pad_name
                FROM datastore:{SOURCE_KEY}
                WHERE (:well_name IS NULL OR name = :well_name)
                  AND (:lower_ts IS NULL OR record_ts >= CAST(:lower_ts AS timestamp))
                ORDER BY name, record_ts
            """,
            workspace_id=workspace_id,
            params={"well_name": well_name, "lower_ts": lower_ts},
        )
    )
    if telemetry.empty:
        logger.info("Bronze labeling: no source telemetry for mode=%s", mode)
        return {"mode": mode, "wells_processed": 0, "rows_written": 0}

    telemetry = telemetry.rename(columns={"record_ts": DATETIME_COL})
    telemetry[DATETIME_COL] = pd.to_datetime(telemetry[DATETIME_COL], errors="coerce")
    telemetry = telemetry.dropna(subset=["name", DATETIME_COL])

    wells = list(telemetry["name"].astype(str).unique())[: int(max_wells)]

    results: list[dict[str, Any]] = []
    for name in wells:
        well_df = telemetry[telemetry["name"].astype(str) == name].copy()
        run_id = str(uuid4())
        labeled = _label_well(well_df, algorithm_version, run_id)
        rows_written = int(len(labeled))
        if not dry_run and rows_written:
            # Overwrite this well's window so late data corrections replace cleanly.
            await delete_featurestore_records(
                featurestore_key=BRONZE_KEY,
                workspace_id=workspace_id,
                filters=[{"field": "name", "op": "eq", "value": name}],
            )
            await write_featurestore(
                featurestore_key=BRONZE_KEY,
                workspace_id=workspace_id,
                df=pl.from_pandas(labeled),
                upsert=True,
            )
            manifest = pd.DataFrame(
                [
                    {
                        "fleet_name": (well_df.get("fleet_name") or pd.Series([None])).iloc[0]
                        if "fleet_name" in well_df
                        else None,
                        "pad_name": (well_df.get("pad_name") or pd.Series([None])).iloc[0]
                        if "pad_name" in well_df
                        else None,
                        "name": name,
                        "stage_name": STAGE_NAME,
                        "mode": mode,
                        "algorithm_version": algorithm_version,
                        "active_run_id": run_id,
                        "source_signature": _signature(well_df, algorithm_version),
                        "source_rows": int(len(well_df)),
                        "lookback_start_ts": lower_ts,
                        "first_source_ts": _fmt(well_df[DATETIME_COL].min()),
                        "last_source_ts": _fmt(well_df[DATETIME_COL].max()),
                        "latest_source_updated_at": _now(),
                        "processed_at": _now(),
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
        logger.info("Bronze labeled name=%s rows=%s dry_run=%s", name, rows_written, dry_run)

    total_rows = sum(r["rows"] for r in results)
    logger.info(
        "Bronze labeling completed mode=%s wells=%s rows=%s",
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
