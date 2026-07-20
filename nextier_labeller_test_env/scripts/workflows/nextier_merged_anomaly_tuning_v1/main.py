from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import numpy as np
import pandas as pd
import polars as pl
from prefect import flow, get_run_logger

from nextier_core.anomaly_detection import detect_design_delta_anomalies
from nixdlt.workflow_sdk.platform_tasks import query_store, write_featurestore


PLATINUM_KEY = "merged_platinum_labels_v1"
ANOMALY_MANIFEST_KEY = "merged_anomaly_processing_manifest_v2"
TUNING_KEY = "merged_anomaly_tuning_counts_v1"
TUNING_MANIFEST_KEY = "merged_anomaly_tuning_manifest_v1"
ROLLING_WINDOWS = tuple(range(1, 11))
PRESSURE_THRESHOLDS = tuple(range(50, 501, 10))
RATE_THRESHOLDS = tuple(range(1, 21))
EXPECTED_ROWS_PER_STAGE = len(ROLLING_WINDOWS) * (
    len(PRESSURE_THRESHOLDS) + len(RATE_THRESHOLDS)
)


def _now() -> str:
    return datetime.now(timezone.utc).replace(tzinfo=None).isoformat(sep=" ")


def _count_runs(mask: np.ndarray) -> int:
    values = np.asarray(mask, dtype=bool)
    if not values.any():
        return 0
    return int((values & ~np.r_[False, values[:-1]]).sum())


async def _candidate_stages(
    workspace_id: int,
    algorithm_version: str,
    mode: str,
    max_stages: int,
    well_name: str | None,
    stage_num_td: float | None,
) -> list[dict[str, Any]]:
    rows = await query_store(
        sql=f"""
            SELECT
              m.fleet_name,
              m.pad_name,
              m.name,
              m.stage_num_td,
              m.source_signature
            FROM featurestore:{ANOMALY_MANIFEST_KEY} m
            LEFT JOIN featurestore:{TUNING_MANIFEST_KEY} b
              ON b.name = m.name
             AND b.stage_num_td = m.stage_num_td
             AND b.source_signature = m.source_signature
             AND b.algorithm_version = :algorithm_version
             AND b.status = 'active'
            WHERE m.status = 'active'
              AND m.algorithm_version = 'ds-v1'
              AND (:well_name = '' OR m.name = :well_name)
              AND (:stage_num_td < 0 OR m.stage_num_td = :stage_num_td)
              AND (
                :force_rebuild
                OR b.name IS NULL
                OR b.tuning_rows <> :expected_rows
              )
            ORDER BY m.name, m.stage_num_td
            LIMIT {int(max_stages)}
        """,
        workspace_id=workspace_id,
        params={
            "algorithm_version": algorithm_version,
            "force_rebuild": mode == "rebuild",
            "well_name": (well_name or "").strip(),
            "stage_num_td": (
                float(stage_num_td) if stage_num_td is not None else -1.0
            ),
            "expected_rows": EXPECTED_ROWS_PER_STAGE,
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
              press_mainline
            FROM featurestore:{PLATINUM_KEY}
            WHERE name = :name
              AND stage_num_td = :stage_num_td
            ORDER BY CAST(datetime_fmt AS timestamp)
        """,
        workspace_id=workspace_id,
        params={"name": name, "stage_num_td": float(stage_num_td)},
    )


def _build_counts(
    source: pl.DataFrame,
    candidate: dict[str, Any],
    algorithm_version: str,
) -> pl.DataFrame:
    if source.is_empty():
        raise ValueError(
            f"No Platinum rows found for {candidate['name']} "
            f"stage {candidate['stage_num_td']}"
        )
    frame = source.to_pandas().sort_values("datetime_fmt").reset_index(drop=True)
    design_mask = (
        frame["substage"]
        .astype("string")
        .str.strip()
        .str.lower()
        .isin(["design", "slurry"])
        .fillna(False)
        .to_numpy(dtype=bool)
    )
    records: list[dict[str, Any]] = []
    base = {
        "fleet_name": str(candidate.get("fleet_name") or "Unknown"),
        "pad_name": str(candidate.get("pad_name") or "Unknown"),
        "name": str(candidate["name"]),
        "stage_num_td": float(candidate["stage_num_td"]),
        "source_signature": str(candidate["source_signature"]),
        "algorithm_version": algorithm_version,
        "updated_at": _now(),
    }
    for rolling_points in ROLLING_WINDOWS:
        _, _, delta_pressure, delta_rate = detect_design_delta_anomalies(
            frame,
            design_mask=design_mask,
            pressure_threshold_psi=0,
            rate_drop_threshold_bpm=0,
            rolling_points=rolling_points,
        )
        pressure = delta_pressure.to_numpy()
        rate = delta_rate.to_numpy()
        for threshold in PRESSURE_THRESHOLDS:
            records.append(
                {
                    **base,
                    "rolling_points": rolling_points,
                    "threshold_type": "pressure",
                    "threshold_value": float(threshold),
                    "segment_count": _count_runs(
                        (pressure >= threshold) & design_mask
                    ),
                }
            )
        for threshold in RATE_THRESHOLDS:
            records.append(
                {
                    **base,
                    "rolling_points": rolling_points,
                    "threshold_type": "rate",
                    "threshold_value": float(threshold),
                    "segment_count": _count_runs(
                        (rate <= -threshold) & design_mask
                    ),
                }
            )
    return pl.from_dicts(records)


def _build_manifest(
    candidate: dict[str, Any],
    algorithm_version: str,
    tuning_rows: int,
) -> pl.DataFrame:
    return pl.from_dicts(
        [
            {
                "name": str(candidate["name"]),
                "stage_num_td": float(candidate["stage_num_td"]),
                "algorithm_version": algorithm_version,
                "source_signature": str(candidate["source_signature"]),
                "tuning_rows": int(tuning_rows),
                "processed_at": _now(),
                "status": "active",
            }
        ]
    )


@flow(name="nextier-merged-anomaly-tuning-v1")
async def nextier_merged_anomaly_tuning_v1_flow(
    workspace_id: int,
    workflow_id: int,
    mode: str = "incremental",
    max_stages: int = 10,
    well_name: str | None = None,
    stage_num_td: float | None = None,
    algorithm_version: str = "tuning-v1",
    dry_run: bool = False,
):
    del workflow_id
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
    processed = 0
    rows_written = 0
    for candidate in candidates:
        source = await _load_stage(
            workspace_id,
            str(candidate["name"]),
            float(candidate["stage_num_td"]),
        )
        counts = _build_counts(source, candidate, algorithm_version)
        if not dry_run and not counts.is_empty():
            await write_featurestore(
                featurestore_key=TUNING_KEY,
                workspace_id=workspace_id,
                df=counts,
                upsert=True,
            )
            manifest = _build_manifest(
                candidate,
                algorithm_version,
                len(counts),
            )
            await write_featurestore(
                featurestore_key=TUNING_MANIFEST_KEY,
                workspace_id=workspace_id,
                df=manifest,
                upsert=True,
            )
        processed += 1
        rows_written += len(counts)
        logger.info(
            "Anomaly tuning stage processed name=%s stage=%s rows=%s dry_run=%s",
            candidate["name"],
            candidate["stage_num_td"],
            len(counts),
            dry_run,
        )
    has_more = len(candidates) == int(max_stages)
    logger.info(
        "Anomaly tuning run completed mode=%s stages_processed=%s "
        "rows_generated=%s has_more=%s",
        mode,
        processed,
        rows_written,
        has_more,
    )
    return {
        "mode": mode,
        "algorithm_version": algorithm_version,
        "dry_run": dry_run,
        "stages_processed": processed,
        "rows_generated": rows_written,
        "has_more": has_more,
    }
