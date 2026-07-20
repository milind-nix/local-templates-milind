from __future__ import annotations

from datetime import datetime, timezone
from hashlib import sha256
from typing import Any
from uuid import uuid4

import pandas as pd
import numpy as np
import polars as pl
from prefect import flow, get_run_logger

from nextier_core.anomaly_detection import (
    detect_design_delta_anomalies,
    detect_mid_stage_shutdowns,
    detect_sweep_markers_around_mid_stage_shutdowns,
    require_anomaly_min_length,
)
from nextier_utils.common.segment_utils import build_contiguous_segments
from nixdlt.workflow_sdk.platform_tasks import query_store, write_featurestore


PLATINUM_KEY = "merged_platinum_labels_v1"
LABELS_KEY = "merged_anomaly_labels_v2"
EVENTS_KEY = "merged_anomaly_events_v2"
STAGE_SUMMARY_KEY = "merged_anomaly_stage_summary_v2"
MANIFEST_KEY = "merged_anomaly_processing_manifest_v2"


def _now() -> str:
    return datetime.now(timezone.utc).replace(tzinfo=None).isoformat(sep=" ")


def _as_pandas(frame: pl.DataFrame) -> pd.DataFrame:
    return frame.to_pandas() if not frame.is_empty() else pd.DataFrame()


def _source_signature(row: dict[str, Any], algorithm_version: str) -> str:
    payload = "|".join(
        str(row.get(key) or "")
        for key in (
            "name",
            "stage_num_td",
            "source_rows",
            "first_source_ts",
            "last_source_ts",
            "latest_source_updated_at",
        )
    )
    return sha256(f"{algorithm_version}|{payload}".encode()).hexdigest()


async def _candidate_stages(
    workspace_id: int,
    algorithm_version: str,
    mode: str,
    max_stages: int,
    well_name: str | None,
    stage_num_td: float | None,
) -> list[dict[str, Any]]:
    force = mode == "rebuild"
    rows = await query_store(
        sql=f"""
            WITH source_stages AS (
              SELECT
                s.name,
                s.stage_num_td,
                COALESCE(
                  NULLIF(MAX(w.fleet_name), ''),
                  NULLIF(MAX(s.fleet_name), ''),
                  'Unknown'
                ) AS fleet_name,
                COALESCE(
                  NULLIF(MAX(w.pad_name), ''),
                  NULLIF(MAX(s.pad_name), ''),
                  'Unknown'
                ) AS pad_name,
                COALESCE(MAX(CAST(s.sample_count AS BIGINT)), 0) AS source_rows,
                MIN(CAST(s.stage_start_ts AS timestamp)) AS first_source_ts,
                MAX(CAST(s.stage_end_ts AS timestamp)) AS last_source_ts,
                MAX(
                  COALESCE(
                    CAST(s.last_created_ts AS timestamp),
                    CAST(s.stage_end_ts AS timestamp)
                  )
                ) AS latest_source_updated_at
              FROM featurestore:merged_td_stage_index_v1 s
              LEFT JOIN featurestore:merged_well_index_v1 w
                ON w.name = s.name
              WHERE s.name IS NOT NULL
                AND s.stage_num_td IS NOT NULL
                AND s.stage_start_ts IS NOT NULL
                AND s.stage_end_ts IS NOT NULL
                AND (:well_name = '' OR s.name = :well_name)
                AND (:stage_num_td < 0 OR s.stage_num_td = :stage_num_td)
              GROUP BY s.name, s.stage_num_td
            )
            SELECT s.*
            FROM source_stages s
            LEFT JOIN featurestore:{MANIFEST_KEY} m
              ON m.name = s.name
             AND m.stage_num_td = s.stage_num_td
             AND m.algorithm_version = :algorithm_version
            WHERE :force_rebuild
               OR m.name IS NULL
               OR m.source_rows IS DISTINCT FROM s.source_rows
               OR CAST(m.latest_source_updated_at AS timestamp)
                    IS DISTINCT FROM s.latest_source_updated_at
            ORDER BY s.name, s.stage_num_td
            LIMIT {int(max_stages)}
        """,
        workspace_id=workspace_id,
        params={
            "algorithm_version": algorithm_version,
            "force_rebuild": force,
            "well_name": (well_name or "").strip(),
            "stage_num_td": float(stage_num_td) if stage_num_td is not None else -1.0,
        },
    )
    return rows.to_dicts() if not rows.is_empty() else []


async def _load_stage(
    workspace_id: int,
    name: str,
    stage_num_td: float,
) -> pl.DataFrame:
    return await query_store(
        sql=f"""
            SELECT
              name,
              stage_num_td,
              CAST(datetime_fmt AS timestamp) AS datetime_fmt,
              substage,
              rate_slurry,
              press_mainline,
              prop_conc_blend_denso
            FROM featurestore:{PLATINUM_KEY}
            WHERE name = :name
              AND stage_num_td = :stage_num_td
            ORDER BY CAST(datetime_fmt AS timestamp)
        """,
        workspace_id=workspace_id,
        params={"name": name, "stage_num_td": float(stage_num_td)},
    )


def _prepare_outputs(
    source: pl.DataFrame,
    candidate: dict[str, Any],
    *,
    algorithm_version: str,
    pressure_threshold_psi: float,
    rate_drop_threshold_bpm: float,
    rolling_points: int,
    max_gap_seconds: float,
) -> dict[str, pl.DataFrame]:
    source_df = _as_pandas(source)
    name = str(candidate["name"])
    stage = float(candidate["stage_num_td"])
    fleet_name = str(candidate.get("fleet_name") or "Unknown")
    pad_name = str(candidate.get("pad_name") or "Unknown")
    run_id = str(uuid4())
    created_at = _now()
    signature = _source_signature(candidate, algorithm_version)

    labels = source_df[
        [
            "name",
            "stage_num_td",
            "datetime_fmt",
            "substage",
            "rate_slurry",
            "press_mainline",
            "prop_conc_blend_denso",
        ]
    ].copy()
    labels["datetime_fmt"] = pd.to_datetime(labels["datetime_fmt"], errors="coerce")
    labels = labels.dropna(subset=["datetime_fmt"]).sort_values("datetime_fmt")
    labels = labels.reset_index(drop=True)

    design_mask = (
        labels["substage"]
        .astype("string")
        .str.strip()
        .str.lower()
        .isin(["design", "slurry"])
        .fillna(False)
        .to_numpy(dtype=bool)
    )
    shutdown_mask = require_anomaly_min_length(
        detect_mid_stage_shutdowns(labels)
    ) & design_mask
    sweep_pre_mask, sweep_post_mask = detect_sweep_markers_around_mid_stage_shutdowns(
        labels,
        shutdown_mask,
    )
    sweep_pre_mask &= design_mask
    sweep_post_mask &= design_mask
    pressure_mask, rate_mask, _, _ = detect_design_delta_anomalies(
        labels,
        design_mask=design_mask,
        pressure_threshold_psi=pressure_threshold_psi,
        rate_drop_threshold_bpm=rate_drop_threshold_bpm,
        rolling_points=rolling_points,
    )
    anomaly = np.full(len(labels), None, dtype=object)
    for mask, value in (
        (shutdown_mask, "mid_stage_shutdown"),
        (sweep_pre_mask, "sweep_pre"),
        (sweep_post_mask, "sweep_post"),
        (rate_mask, "pump_rate_drop"),
        (pressure_mask, "pressure_surge"),
    ):
        assign = np.asarray(mask, dtype=bool) & pd.isna(anomaly)
        anomaly[assign] = value
    labels["anomaly"] = anomaly
    labels = labels.loc[
        labels["anomaly"].notna(),
        ["name", "stage_num_td", "datetime_fmt", "anomaly"],
    ].copy()

    segment_input = labels.rename(columns={"stage_num_td": "stage_num"})
    events = build_contiguous_segments(
        segment_input,
        label_col="anomaly",
        datetime_col="datetime_fmt",
        well_col="name",
        stage_col="stage_num",
        max_gap_s=max_gap_seconds,
    )
    events = events.rename(
        columns={"stage_num": "stage_num_td", "label": "anomaly"}
    )
    if not events.empty:
        events["anomaly"] = events["anomaly"].str.lower()
        events["anomaly_display"] = (
            events["anomaly"].str.replace("_", " ", regex=False).str.title()
        )
        events = events[
            [
                "name",
                "stage_num_td",
                "anomaly",
                "anomaly_display",
                "start_ts",
                "end_ts",
                "duration_sec",
                "duration_min",
            ]
        ]

    if not labels.empty:
        labels["fleet_name"] = fleet_name
        labels["pad_name"] = pad_name
        labels["processing_run_id"] = run_id
        labels["algorithm_version"] = algorithm_version
        labels["source_signature"] = signature
        labels["created_at"] = created_at

    if not events.empty:
        events["fleet_name"] = fleet_name
        events["pad_name"] = pad_name
        events["processing_run_id"] = run_id
        events["algorithm_version"] = algorithm_version
        events["source_signature"] = signature
        events["event_id"] = events.apply(
            lambda row: sha256(
                (
                    f"{run_id}|{row['name']}|{row['stage_num_td']}|"
                    f"{row['anomaly']}|{row['start_ts']}|{row['end_ts']}"
                ).encode()
            ).hexdigest(),
            axis=1,
        )
        events["created_at"] = created_at

    event_counts = events["anomaly"].value_counts() if not events.empty else pd.Series(dtype=int)
    stage_summary = pd.DataFrame([{
        "fleet_name": fleet_name,
        "pad_name": pad_name,
        "name": name,
        "stage_num_td": stage,
        "stage_start_ts": candidate.get("first_source_ts"),
        "stage_end_ts": candidate.get("last_source_ts"),
        "source_rows": int(candidate["source_rows"]),
        "total_anomaly_lines": int(len(events)),
        "pressure_surge_lines": int(event_counts.get("pressure_surge", 0)),
        "pump_rate_drop_lines": int(event_counts.get("pump_rate_drop", 0)),
        "mid_stage_shutdown_lines": int(event_counts.get("mid_stage_shutdown", 0)),
        "sweep_lines": int(
            event_counts.get("sweep_pre", 0) + event_counts.get("sweep_post", 0)
        ),
    }])
    stage_summary["processing_run_id"] = run_id
    stage_summary["algorithm_version"] = algorithm_version
    stage_summary["source_signature"] = signature
    stage_summary["created_at"] = created_at

    manifest = pd.DataFrame([{
        "fleet_name": fleet_name,
        "pad_name": pad_name,
        "name": name,
        "stage_num_td": stage,
        "algorithm_version": algorithm_version,
        "active_run_id": run_id,
        "source_signature": signature,
        "source_rows": int(candidate["source_rows"]),
        "first_source_ts": candidate.get("first_source_ts"),
        "last_source_ts": candidate.get("last_source_ts"),
        "latest_source_updated_at": candidate.get("latest_source_updated_at"),
        "processed_at": created_at,
        "status": "active",
    }])
    return {
        LABELS_KEY: pl.from_pandas(labels) if not labels.empty else pl.DataFrame(),
        EVENTS_KEY: pl.from_pandas(events) if not events.empty else pl.DataFrame(),
        STAGE_SUMMARY_KEY: pl.from_pandas(stage_summary),
        MANIFEST_KEY: pl.from_pandas(manifest),
    }


@flow(name="nextier-merged-anomaly-pipeline-v2")
async def nextier_merged_anomaly_pipeline_v2_flow(
    workspace_id: int,
    workflow_id: int,
    mode: str = "incremental",
    max_stages: int = 25,
    well_name: str | None = None,
    stage_num_td: float | None = None,
    algorithm_version: str = "ds-v1",
    pressure_threshold_psi: float = 200.0,
    rate_drop_threshold_bpm: float = 5.0,
    rolling_points: int = 1,
    max_gap_seconds: float = 120.0,
    dry_run: bool = False,
):
    del workflow_id  # Reserved by the platform flow contract.
    if mode not in {"incremental", "historical", "rebuild"}:
        raise ValueError("mode must be incremental, historical, or rebuild")
    logger = get_run_logger()
    candidates = await _candidate_stages(
        workspace_id,
        algorithm_version,
        mode,
        max_stages,
        well_name,
        stage_num_td,
    )
    results = []
    for candidate in candidates:
        source = await _load_stage(
            workspace_id,
            str(candidate["name"]),
            float(candidate["stage_num_td"]),
        )
        outputs = _prepare_outputs(
            source,
            candidate,
            algorithm_version=algorithm_version,
            pressure_threshold_psi=pressure_threshold_psi,
            rate_drop_threshold_bpm=rate_drop_threshold_bpm,
            rolling_points=rolling_points,
            max_gap_seconds=max_gap_seconds,
        )
        counts = {key: len(frame) for key, frame in outputs.items()}
        if not dry_run:
            # The manifest is written last, making the new run visible only
            # after every immutable output has been persisted successfully.
            for key in (LABELS_KEY, EVENTS_KEY, STAGE_SUMMARY_KEY, MANIFEST_KEY):
                frame = outputs[key]
                if frame.is_empty():
                    continue
                await write_featurestore(
                    featurestore_key=key,
                    workspace_id=workspace_id,
                    df=frame,
                    upsert=(key == MANIFEST_KEY),
                )
        logger.info(
            "Anomaly stage processed name=%s stage=%s dry_run=%s counts=%s",
            candidate["name"],
            candidate["stage_num_td"],
            dry_run,
            counts,
        )
        results.append({
            "name": candidate["name"],
            "stage_num_td": candidate["stage_num_td"],
            "counts": counts,
        })
    has_more = len(candidates) == int(max_stages)
    logger.info(
        "Anomaly run completed mode=%s stages_processed=%s has_more=%s",
        mode,
        len(results),
        has_more,
    )
    return {
        "mode": mode,
        "algorithm_version": algorithm_version,
        "dry_run": dry_run,
        "stages_processed": len(results),
        "has_more": has_more,
        "results": results,
    }
