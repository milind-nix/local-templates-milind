"""Nextier Merged Copper Stage Index v1.

Collapses row-level Copper stage ordinals (``merged_copper_labels_v1``) into
compact stage windows — one row per well + Copper stage — in
``merged_copper_stage_index_v1``.

By default it indexes ``copper_continuous`` (the canonical, closed-only-expanded
final layer). Rows with stage ordinal 0 (unassigned / rest) are ignored, so every
indexed stage is a real, closed Copper stage. Set ``stage_layer=provisional`` to
index the eager live layer instead (useful for a live/provisional dashboard).
"""

from __future__ import annotations

from datetime import datetime, timezone
from hashlib import sha256
from typing import Any
from uuid import uuid4

import pandas as pd
import polars as pl
from prefect import flow, get_run_logger

from nixdlt.workflow_sdk.platform_tasks import (
    delete_featurestore_records,
    query_store,
    write_featurestore,
)


COPPER_KEY = "merged_copper_labels_v1"
SOURCE_KEY = "merged_fleet_stream_customer_full_v2"
INDEX_KEY = "merged_copper_stage_index_v1"
MANIFEST_KEY = "merged_copper_processing_manifest_v1"
STAGE_NAME = "copper_stage_index"

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


def _build_windows(
    well_df: pd.DataFrame,
    telem: pd.DataFrame,
    stage_col: str,
    stage_layer: str,
    algorithm_version: str,
    run_id: str,
    now: str,
) -> pd.DataFrame:
    """Group one well's rows by stage ordinal into stage windows."""
    stages = pd.to_numeric(well_df[stage_col], errors="coerce").fillna(0).astype(int)
    well_df = well_df.assign(__stage__=stages)
    well_df = well_df[well_df["__stage__"] > 0]
    if well_df.empty:
        return pd.DataFrame()

    # Join telemetry aggregates by time within each stage window.
    frames = []
    max_stage = int(well_df["__stage__"].max())
    for stage_num, g in well_df.groupby("__stage__", sort=True):
        g = g.sort_values(DATETIME_COL)
        start_ts = g[DATETIME_COL].min()
        end_ts = g[DATETIME_COL].max()
        window_telem = telem[
            (telem[DATETIME_COL] >= start_ts) & (telem[DATETIME_COL] <= end_ts)
        ]
        frames.append(
            {
                "name": str(g["name"].iloc[0]),
                "copper_stage_num": int(stage_num),
                "fleet_name": g["fleet_name"].iloc[0] if "fleet_name" in g else None,
                "pad_name": g["pad_name"].iloc[0] if "pad_name" in g else None,
                "well_id": None,
                "api_num": None,
                "stage_start_ts": _fmt(start_ts),
                "stage_end_ts": _fmt(end_ts),
                "is_closed": True if stage_layer == "continuous" else int(stage_num) < max_stage,
                "sample_count": int(len(g)),
                "avg_rate_slurry": float(pd.to_numeric(window_telem.get("rate_slurry"), errors="coerce").mean())
                if not window_telem.empty
                else None,
                "max_rate_slurry": float(pd.to_numeric(window_telem.get("rate_slurry"), errors="coerce").max())
                if not window_telem.empty
                else None,
                "avg_press_mainline": float(pd.to_numeric(window_telem.get("press_mainline"), errors="coerce").mean())
                if not window_telem.empty
                else None,
                "max_press_mainline": float(pd.to_numeric(window_telem.get("press_mainline"), errors="coerce").max())
                if not window_telem.empty
                else None,
                "stage_layer": stage_layer,
                "algorithm_version": algorithm_version,
                "processing_run_id": run_id,
                "created_at": now,
            }
        )
    return pd.DataFrame(frames)


@flow(name="nextier-merged-copper-stage-index-v1")
async def nextier_merged_copper_stage_index_v1_flow(
    mode: str = "incremental",
    max_wells: int = 25,
    well_name: str | None = None,
    algorithm_version: str = "copper-v1",
    stage_layer: str = "continuous",
    dry_run: bool = False,
    workspace_id: int | None = None,
    workflow_id: int | None = None,
) -> dict[str, Any]:
    logger = get_run_logger()
    if mode not in {"live", "incremental", "historical", "rebuild"}:
        raise ValueError("mode must be live, incremental, historical, or rebuild")
    if stage_layer not in {"continuous", "provisional"}:
        raise ValueError("stage_layer must be continuous or provisional")
    stage_col = f"copper_{stage_layer}"

    labels = _to_pandas(
        await query_store(
            sql=f"""
                SELECT name, datetime_fmt, copper_provisional, copper_confirmed,
                       copper_continuous, fleet_name, pad_name
                FROM featurestore:{COPPER_KEY}
                WHERE (CAST(:well_name AS VARCHAR) IS NULL OR name = :well_name)
                ORDER BY name, datetime_fmt
            """,
            workspace_id=workspace_id,
            params={"well_name": well_name},
        )
    )
    if labels.empty:
        logger.info("Copper stage index: no copper label rows for mode=%s", mode)
        return {"mode": mode, "wells_processed": 0, "stages_written": 0}

    labels[DATETIME_COL] = pd.to_datetime(labels[DATETIME_COL], errors="coerce")
    labels = labels.dropna(subset=["name", DATETIME_COL])

    telem = _to_pandas(
        await query_store(
            sql=f"""
                SELECT name, record_ts, rate_slurry, press_mainline
                FROM datastore:{SOURCE_KEY}
                WHERE (CAST(:well_name AS VARCHAR) IS NULL OR name = :well_name)
            """,
            workspace_id=workspace_id,
            params={"well_name": well_name},
        )
    )
    if not telem.empty:
        telem = telem.rename(columns={"record_ts": DATETIME_COL})
        telem[DATETIME_COL] = pd.to_datetime(telem[DATETIME_COL], errors="coerce")

    wells = list(labels["name"].astype(str).unique())[: int(max_wells)]
    now = _now()
    results: list[dict[str, Any]] = []

    for name in wells:
        well_df = labels[labels["name"].astype(str) == name].copy()
        well_telem = (
            telem[telem["name"].astype(str) == name] if not telem.empty else pd.DataFrame()
        )
        windows = _build_windows(
            well_df, well_telem, stage_col, stage_layer, algorithm_version, str(uuid4()), now
        )
        stages_written = int(len(windows))

        if not dry_run and stages_written:
            run_id = str(windows["processing_run_id"].iloc[0])
            await delete_featurestore_records(
                featurestore_key=INDEX_KEY,
                workspace_id=workspace_id,
                filters=[{"field": "name", "op": "eq", "value": name}],
            )
            await write_featurestore(
                featurestore_key=INDEX_KEY,
                workspace_id=workspace_id,
                df=pl.from_pandas(windows),
                upsert=True,
            )
            manifest = pd.DataFrame(
                [
                    {
                        "fleet_name": well_df["fleet_name"].iloc[0] if "fleet_name" in well_df and not well_df.empty else None,
                        "pad_name": well_df["pad_name"].iloc[0] if "pad_name" in well_df and not well_df.empty else None,
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
        results.append({"name": name, "stages": stages_written})
        logger.info("Copper stage index name=%s stages=%s dry_run=%s", name, stages_written, dry_run)

    total_stages = sum(r["stages"] for r in results)
    logger.info(
        "Copper stage index completed mode=%s wells=%s stages=%s",
        mode,
        len(results),
        total_stages,
    )
    return {
        "mode": mode,
        "wells_processed": len(results),
        "stages_written": total_stages,
        "has_more": len(wells) == int(max_wells),
    }
