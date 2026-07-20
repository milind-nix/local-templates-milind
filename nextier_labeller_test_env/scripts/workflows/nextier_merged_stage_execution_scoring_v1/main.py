from __future__ import annotations

from datetime import datetime, timezone
from hashlib import sha256
from typing import Any
from uuid import uuid4

import pandas as pd
import polars as pl
from prefect import flow, get_run_logger

from nextier_utils.scoring import (
    compute_stage_execution_raw_components,
    normalize_stage_execution_scores,
)
from nixdlt.workflow_sdk.platform_tasks import query_store, write_featurestore


PLATINUM_KEY = "merged_platinum_labels_v1"
BRONZE_KEY = "merged_bronze_labels_v1"
ANOMALY_SUMMARY_KEY = "merged_anomaly_stage_summary_v2"
ANOMALY_MANIFEST_KEY = "merged_anomaly_processing_manifest_v2"
COMPONENTS_KEY = "merged_stage_execution_components_v1"
SCORES_KEY = "merged_stage_execution_scores_v1"

COMPONENT_VALUE_COLUMNS = [
    "anomaly_count",
    "substages_present",
    "design_seconds",
    "design_minutes",
    "raw_a",
    "raw_b",
    "raw_c",
    "cv_rate",
    "cv_press",
    "longest_run_frac",
    "dip_rate_d",
    "design_rate_d",
]


def _now() -> str:
    return datetime.now(timezone.utc).replace(tzinfo=None).isoformat(sep=" ")


def _as_pandas(frame: pl.DataFrame) -> pd.DataFrame:
    return frame.to_pandas() if not frame.is_empty() else pd.DataFrame()


def _source_signature(candidate: dict[str, Any], formula_version: str) -> str:
    payload = "|".join(
        str(candidate.get(key) or "")
        for key in (
            "name",
            "stage_num_td",
            "platinum_rows",
            "bronze_rows",
            "first_source_ts",
            "last_source_ts",
            "latest_source_updated_at",
            "anomaly_source_signature",
        )
    )
    return sha256(f"{formula_version}|{payload}".encode()).hexdigest()


async def _candidate_stages(
    workspace_id: int,
    *,
    mode: str,
    max_stages: int,
    well_name: str | None,
    stage_num_td: float | None,
    formula_version: str,
) -> list[dict[str, Any]]:
    rows = await query_store(
        sql=f"""
            WITH stage_scope AS (
              SELECT
                s.name,
                s.stage_num_td,
                COALESCE(CAST(s.sample_count AS BIGINT), 0) AS platinum_rows,
                COALESCE(CAST(s.sample_count AS BIGINT), 0) AS bronze_rows,
                CAST(s.stage_start_ts AS timestamp) AS first_source_ts,
                CAST(s.stage_end_ts AS timestamp) AS last_source_ts,
                COALESCE(
                  CAST(s.last_created_ts AS timestamp),
                  CAST(s.stage_end_ts AS timestamp)
                ) AS latest_source_updated_at,
                COALESCE(NULLIF(w.fleet_name, ''), NULLIF(s.fleet_name, ''), 'Unknown')
                  AS fleet_name,
                COALESCE(NULLIF(w.pad_name, ''), NULLIF(s.pad_name, ''), 'Unknown')
                  AS pad_name
              FROM featurestore:merged_td_stage_index_v1 s
              LEFT JOIN featurestore:merged_well_index_v1 w
                ON w.name = s.name
              WHERE s.name IS NOT NULL
                AND s.stage_num_td IS NOT NULL
                AND s.stage_start_ts IS NOT NULL
                AND s.stage_end_ts IS NOT NULL
                AND (:well_name = '' OR s.name = :well_name)
                AND (:stage_num_td < 0 OR s.stage_num_td = :stage_num_td)
            ),
            current_anomaly AS (
              SELECT DISTINCT ON (m.name, m.stage_num_td)
                m.name,
                m.stage_num_td,
                m.source_signature AS anomaly_source_signature
              FROM featurestore:{ANOMALY_MANIFEST_KEY} m
              WHERE m.status = 'active'
              ORDER BY m.name, m.stage_num_td, CAST(m.processed_at AS timestamp) DESC
            )
            SELECT
              s.*,
              COALESCE(a.anomaly_source_signature, '') AS anomaly_source_signature,
              EXISTS (
                SELECT 1
                FROM featurestore:{PLATINUM_KEY} p
                WHERE p.name = s.name
                  AND p.stage_num_td = s.stage_num_td
                LIMIT 1
              ) AS has_platinum
            FROM stage_scope s
            LEFT JOIN current_anomaly a
              ON a.name = s.name AND a.stage_num_td = s.stage_num_td
            LEFT JOIN featurestore:{COMPONENTS_KEY} c
              ON c.name = s.name
             AND c.stage_num_td = s.stage_num_td
             AND c.formula_version = :formula_version
            WHERE (
                :force_rebuild
                OR c.name IS NULL
                OR c.platinum_rows IS DISTINCT FROM s.platinum_rows
                OR c.bronze_rows IS DISTINCT FROM s.bronze_rows
                OR CAST(c.latest_source_updated_at AS timestamp)
                     IS DISTINCT FROM s.latest_source_updated_at
                OR c.anomaly_source_signature IS DISTINCT FROM
                     COALESCE(a.anomaly_source_signature, '')
              )
              AND EXISTS (
                SELECT 1
                FROM featurestore:{PLATINUM_KEY} p
                WHERE p.name = s.name
                  AND p.stage_num_td = s.stage_num_td
                LIMIT 1
              )
            ORDER BY s.name, s.stage_num_td
            LIMIT {int(max_stages)}
        """,
        workspace_id=workspace_id,
        params={
            "well_name": (well_name or "").strip(),
            "stage_num_td": float(stage_num_td) if stage_num_td is not None else -1.0,
            "formula_version": formula_version,
            "force_rebuild": mode == "rebuild",
        },
    )
    return rows.to_dicts() if not rows.is_empty() else []


async def _load_stage_sources(
    workspace_id: int,
    name: str,
    stage_num_td: float,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    platinum, bronze, anomaly = await _load_stage_frames(
        workspace_id,
        name,
        stage_num_td,
    )
    return _as_pandas(platinum), _as_pandas(bronze), _as_pandas(anomaly)


async def _load_stage_frames(
    workspace_id: int,
    name: str,
    stage_num_td: float,
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    platinum = await query_store(
        sql=f"""
            SELECT
              name,
              stage_num_td,
              CAST(datetime_fmt AS timestamp) AS datetime_fmt,
              substage,
              rate_slurry,
              press_mainline
            FROM featurestore:{PLATINUM_KEY}
            WHERE name = :name AND stage_num_td = :stage_num_td
            ORDER BY CAST(datetime_fmt AS timestamp)
        """,
        workspace_id=workspace_id,
        params={"name": name, "stage_num_td": stage_num_td},
    )
    bronze = await query_store(
        sql=f"""
            SELECT
              name,
              stage_num_td,
              CAST(datetime_fmt AS timestamp) AS datetime_fmt,
              substage
            FROM featurestore:{BRONZE_KEY}
            WHERE name = :name AND stage_num_td = :stage_num_td
        """,
        workspace_id=workspace_id,
        params={"name": name, "stage_num_td": stage_num_td},
    )
    anomaly = await query_store(
        sql=f"""
            SELECT
              s.name,
              s.stage_num_td,
              s.pressure_surge_lines,
              s.pump_rate_drop_lines,
              s.mid_stage_shutdown_lines
            FROM featurestore:{ANOMALY_SUMMARY_KEY} s
            JOIN featurestore:{ANOMALY_MANIFEST_KEY} m
              ON m.name = s.name
             AND m.stage_num_td = s.stage_num_td
             AND m.active_run_id = s.processing_run_id
            WHERE s.name = :name
              AND s.stage_num_td = :stage_num_td
              AND m.status = 'active'
        """,
        workspace_id=workspace_id,
        params={"name": name, "stage_num_td": stage_num_td},
    )
    return platinum, bronze, anomaly


def _compute_component_row(
    candidate: dict[str, Any],
    platinum: pd.DataFrame,
    bronze: pd.DataFrame,
    anomaly: pd.DataFrame,
    *,
    formula_version: str,
    processing_run_id: str,
    computed_at: str,
) -> pd.DataFrame:
    telemetry = platinum[
        ["name", "datetime_fmt", "rate_slurry", "press_mainline"]
    ].copy()
    labels = platinum[
        ["name", "stage_num_td", "datetime_fmt", "substage"]
    ].copy()
    if anomaly.empty:
        anomaly = pd.DataFrame([{
            "name": candidate["name"],
            "stage_num_td": candidate["stage_num_td"],
            "pressure_surge_lines": 0,
            "pump_rate_drop_lines": 0,
            "mid_stage_shutdown_lines": 0,
        }])

    components = compute_stage_execution_raw_components(
        platinum_labels=labels,
        telemetry=telemetry,
        bronze_labels=bronze,
        anomaly_summary=anomaly,
    ).rename(columns={"stage_num": "stage_num_td"})
    if components.empty:
        raise ValueError(
            f"No scoring components generated for {candidate['name']} "
            f"stage {candidate['stage_num_td']}"
        )
    components["fleet_name"] = str(candidate.get("fleet_name") or "Unknown")
    components["pad_name"] = str(candidate.get("pad_name") or "Unknown")
    components["formula_version"] = formula_version
    components["source_signature"] = _source_signature(candidate, formula_version)
    components["platinum_rows"] = int(candidate["platinum_rows"])
    components["bronze_rows"] = int(candidate["bronze_rows"])
    components["anomaly_source_signature"] = str(
        candidate.get("anomaly_source_signature") or ""
    )
    components["first_source_ts"] = candidate.get("first_source_ts")
    components["last_source_ts"] = candidate.get("last_source_ts")
    components["latest_source_updated_at"] = candidate.get(
        "latest_source_updated_at"
    )
    components["processing_run_id"] = processing_run_id
    components["computed_at"] = computed_at
    columns = [
        "fleet_name",
        "pad_name",
        "name",
        "stage_num_td",
        "formula_version",
        "source_signature",
        "platinum_rows",
        "bronze_rows",
        "anomaly_source_signature",
        "first_source_ts",
        "last_source_ts",
        "latest_source_updated_at",
        *COMPONENT_VALUE_COLUMNS,
        "processing_run_id",
        "computed_at",
    ]
    return components[columns]


async def _load_component_population(
    workspace_id: int,
    formula_version: str,
) -> pd.DataFrame:
    rows = await query_store(
        sql=f"""
            SELECT *
            FROM featurestore:{COMPONENTS_KEY}
            WHERE formula_version = :formula_version
            ORDER BY name, stage_num_td
        """,
        workspace_id=workspace_id,
        params={"formula_version": formula_version},
    )
    return _as_pandas(rows)


def _normalize_population(
    components: pd.DataFrame,
    *,
    formula_version: str,
    normalization_version: str,
    normalization_run_id: str,
    normalized_at: str,
) -> pd.DataFrame:
    if components.empty:
        return pd.DataFrame()
    components = components.sort_values(["name", "stage_num_td"]).reset_index(drop=True)
    signatures = "|".join(components["source_signature"].fillna("").astype(str))
    population_signature = sha256(
        f"{formula_version}|{normalization_version}|{signatures}".encode()
    ).hexdigest()
    scores = normalize_stage_execution_scores(
        components[["name", "stage_num_td", *COMPONENT_VALUE_COLUMNS]]
    )
    metadata = components[
        ["name", "stage_num_td", "fleet_name", "pad_name"]
    ].drop_duplicates(["name", "stage_num_td"])
    scores = scores.rename(columns={"stage_num": "stage_num_td"}).merge(
        metadata,
        on=["name", "stage_num_td"],
        how="left",
    )
    scores["formula_version"] = formula_version
    scores["normalization_version"] = normalization_version
    scores["population_size"] = len(scores)
    scores["population_signature"] = population_signature
    scores["normalization_run_id"] = normalization_run_id
    scores["normalized_at"] = normalized_at
    columns = [
        "fleet_name",
        "pad_name",
        "name",
        "stage_num_td",
        "formula_version",
        "normalization_version",
        "composite_score",
        "score_a",
        "score_b",
        "score_c",
        "score_d",
        "top_driver",
        *COMPONENT_VALUE_COLUMNS,
        "population_size",
        "population_signature",
        "normalization_run_id",
        "normalized_at",
    ]
    return scores[columns]


@flow(name="nextier-merged-stage-execution-scoring-v1")
async def nextier_merged_stage_execution_scoring_v1_flow(
    workspace_id: int,
    workflow_id: int,
    mode: str = "incremental",
    max_stages: int = 25,
    well_name: str | None = None,
    stage_num_td: float | None = None,
    formula_version: str = "stage-execution-v1.0",
    normalization_version: str = "percentile-v1",
    dry_run: bool = False,
):
    del workflow_id
    if mode not in {"incremental", "historical", "rebuild"}:
        raise ValueError("mode must be incremental, historical, or rebuild")
    logger = get_run_logger()
    run_id = str(uuid4())
    run_timestamp = _now()
    candidates = await _candidate_stages(
        workspace_id,
        mode=mode,
        max_stages=max_stages,
        well_name=well_name,
        stage_num_td=stage_num_td,
        formula_version=formula_version,
    )

    computed_rows: list[pd.DataFrame] = []
    for candidate in candidates:
        platinum, bronze, anomaly = await _load_stage_sources(
            workspace_id,
            str(candidate["name"]),
            float(candidate["stage_num_td"]),
        )
        component_row = _compute_component_row(
            candidate,
            platinum,
            bronze,
            anomaly,
            formula_version=formula_version,
            processing_run_id=run_id,
            computed_at=run_timestamp,
        )
        computed_rows.append(component_row)
        if not dry_run:
            await write_featurestore(
                featurestore_key=COMPONENTS_KEY,
                workspace_id=workspace_id,
                df=pl.from_pandas(component_row),
                upsert=True,
            )
        logger.info(
            "Stage execution components computed name=%s stage=%s dry_run=%s",
            candidate["name"],
            candidate["stage_num_td"],
            dry_run,
        )

    population = await _load_component_population(workspace_id, formula_version)
    if computed_rows:
        changed = pd.concat(computed_rows, ignore_index=True)
        population = pd.concat([population, changed], ignore_index=True)
        population = population.drop_duplicates(
            ["name", "stage_num_td", "formula_version"],
            keep="last",
        )
    scores = _normalize_population(
        population,
        formula_version=formula_version,
        normalization_version=normalization_version,
        normalization_run_id=run_id,
        normalized_at=run_timestamp,
    )
    if not dry_run and not scores.empty:
        await write_featurestore(
            featurestore_key=SCORES_KEY,
            workspace_id=workspace_id,
            df=pl.from_pandas(scores),
            upsert=True,
        )

    has_more = len(candidates) == int(max_stages)
    logger.info(
        "Stage execution scoring completed mode=%s components_updated=%s "
        "population_size=%s has_more=%s dry_run=%s",
        mode,
        len(computed_rows),
        len(scores),
        has_more,
        dry_run,
    )
    return {
        "mode": mode,
        "formula_version": formula_version,
        "normalization_version": normalization_version,
        "components_updated": len(computed_rows),
        "population_size": len(scores),
        "has_more": has_more,
        "dry_run": dry_run,
        "processing_run_id": run_id,
    }
