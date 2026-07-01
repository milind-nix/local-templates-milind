from __future__ import annotations

from pathlib import Path
import sys
from typing import Any

import pandas as pd
import polars as pl
from prefect import flow, get_run_logger

from nixdlt.workflow_sdk.platform_tasks import (
    delete_featurestore_records,
    get_state,
    query_store,
    set_state,
    write_featurestore,
)


_WORKFLOWS_DIR = Path(__file__).resolve().parents[1]
if str(_WORKFLOWS_DIR) not in sys.path:
    sys.path.insert(0, str(_WORKFLOWS_DIR))

from nextier_live_stage_processing import main as live_stage_processing


SOURCE_DATASTORE_KEY = "merged_fleet_stream_customer_full_v2"
WELL_INDEX_FEATURESTORE_KEY = "merged_well_index_v1"
TARGET_FEATURESTORE_KEY = "merged_td_stage_index_v1"
BRONZE_LABELS_FEATURESTORE_KEY = "merged_bronze_labels_v1"
GOLD_LABELS_FEATURESTORE_KEY = "merged_gold_labels_v1"
PLATINUM_LABELS_FEATURESTORE_KEY = "merged_platinum_labels_v1"
RECOMPUTE_CHECKPOINT_PREFIX = "__checkpoint__merged_recompute"


def _derive_well_family(name: str, pad_name: Any = None) -> str:
    if pad_name is not None and not pd.isna(pad_name) and str(pad_name).strip():
        return str(pad_name).strip()
    parts = str(name or "").strip().split()
    if not parts:
        return "Unknown"
    if len(parts) == 1:
        return parts[0]
    if len(parts) == 2:
        return f"{parts[0]} {parts[1]}"
    if parts[2].isdigit():
        return f"{parts[0]} {parts[1]} {parts[2]}"
    return f"{parts[0]} {parts[1]}"


def _last_non_null(series: pd.Series) -> Any:
    values = series.dropna()
    if values.empty:
        return None
    return values.iloc[-1]


def _build_well_index(raw_df: pl.DataFrame) -> pl.DataFrame:
    if raw_df.is_empty():
        return pl.DataFrame()

    df = raw_df.to_pandas().copy()
    df["record_ts"] = pd.to_datetime(df["record_ts"], errors="coerce")
    df["created_ts"] = pd.to_datetime(df["created_ts"], errors="coerce")
    df = df.dropna(subset=["name", "record_ts"])
    if df.empty:
        return pl.DataFrame()

    rows = []
    for well_name, group in df.sort_values("record_ts").groupby("name", sort=True):
        pad_name = _last_non_null(group.get("pad_name", pd.Series(dtype=object)))
        rows.append(
            {
                "name": str(well_name),
                "well_family": _derive_well_family(str(well_name), pad_name),
                "fleet_name": _last_non_null(
                    group.get("fleet_name", pd.Series(dtype=object)),
                ),
                "pad_name": pad_name,
                "well_id": _last_non_null(group.get("id", pd.Series(dtype=object))),
                "api_num": _last_non_null(
                    group.get("api_num", pd.Series(dtype=object)),
                ),
                "first_record_ts": live_stage_processing._format_dt(
                    group["record_ts"].min(),
                ),
                "last_record_ts": live_stage_processing._format_dt(
                    group["record_ts"].max(),
                ),
                "first_created_ts": live_stage_processing._format_dt(
                    group["created_ts"].min(),
                ),
                "last_created_ts": live_stage_processing._format_dt(
                    group["created_ts"].max(),
                ),
                "sample_count": len(group),
            }
        )

    return pl.from_pandas(pd.DataFrame(rows))


async def _get_well_names(
    workspace_id: int,
    well_name: str | None,
    well_source: str,
    max_wells: int,
    start_after_well_name: str | None,
) -> list[str]:
    if well_name and well_name.strip():
        return [well_name.strip()]

    params = {"start_after_well_name": start_after_well_name}
    if well_source == "poc_well_index":
        sql = f"""
            SELECT DISTINCT name
            FROM featurestore:well_index_v1
            WHERE
              name IS NOT NULL
              AND name > :start_after_well_name
            ORDER BY name
            LIMIT {max_wells}
        """
    elif well_source == "poc_raw":
        sql = f"""
            SELECT DISTINCT name
            FROM datastore:historian_telemetry_v1
            WHERE
              name IS NOT NULL
              AND name > :start_after_well_name
            ORDER BY name
            LIMIT {max_wells}
        """
    elif well_source == "merged_raw":
        sql = f"""
            SELECT DISTINCT name
            FROM datastore:{SOURCE_DATASTORE_KEY}
            WHERE
              name IS NOT NULL
              AND name > :start_after_well_name
            ORDER BY name
            LIMIT {max_wells}
        """
    else:
        raise ValueError("well_source must be one of: poc_well_index, poc_raw, merged_raw")

    wells_df = await query_store(sql=sql, workspace_id=workspace_id, params=params)
    if wells_df.is_empty():
        return []
    return [str(name) for name in wells_df["name"].drop_nulls().to_list()]


async def _delete_well_outputs(workspace_id: int, well_name: str) -> dict[str, int | None]:
    filters = [{"field": "name", "op": "eq", "value": well_name}]
    deleted = {}
    for featurestore_key in [
        WELL_INDEX_FEATURESTORE_KEY,
        TARGET_FEATURESTORE_KEY,
        BRONZE_LABELS_FEATURESTORE_KEY,
        PLATINUM_LABELS_FEATURESTORE_KEY,
        GOLD_LABELS_FEATURESTORE_KEY,
    ]:
        deleted[featurestore_key] = await delete_featurestore_records(
            featurestore_key=featurestore_key,
            workspace_id=workspace_id,
            filters=filters,
            require_primary_key_filter=True,
        )
    return deleted


async def _recompute_one_well(
    workspace_id: int,
    well_name: str,
    td_gap_seconds: int,
    dry_run: bool,
) -> dict[str, Any]:
    logger = get_run_logger()
    raw_df = await query_store(
        sql=f"""
            SELECT
              created_ts,
              fleet_name,
              pad_name,
              record_ts,
              name,
              id,
              api_num,
              rate_slurry,
              prop_conc_blend_denso,
              press_mainline
            FROM datastore:{SOURCE_DATASTORE_KEY}
            WHERE
              created_ts IS NOT NULL
              AND record_ts IS NOT NULL
              AND name = :well_name
            ORDER BY
              record_ts ASC,
              created_ts ASC
        """,
        workspace_id=workspace_id,
        params={"well_name": well_name},
    )
    if raw_df.is_empty():
        logger.info("Merged recompute skipped well=%s raw_rows=0", well_name)
        return {
            "well_name": well_name,
            "status": "skipped_no_raw_rows",
            "raw_rows": 0,
        }

    well_index = _build_well_index(raw_df)
    classified_df, td_state = live_stage_processing.assign_td_stages(
        raw_df=raw_df,
        td_state_by_well={},
        td_gap_seconds=int(td_gap_seconds),
    )
    stage_windows = live_stage_processing.build_stage_windows(classified_df)
    if stage_windows.is_empty():
        logger.info(
            "Merged recompute skipped well=%s raw_rows=%s stage_windows=0",
            well_name,
            len(raw_df),
        )
        return {
            "well_name": well_name,
            "status": "skipped_no_stage_windows",
            "raw_rows": len(raw_df),
        }

    live_stage_processing.validate_stage_window_overlaps(
        existing_windows=stage_windows.head(0),
        windows_to_write=stage_windows,
    )

    bronze_labels = live_stage_processing.build_bronze_labels(
        stage_rows=classified_df,
        stage_windows=stage_windows,
    )
    platinum_labels = live_stage_processing.build_platinum_labels(
        stage_rows=classified_df,
        stage_windows=stage_windows,
        bronze_labels=bronze_labels,
        td_state_by_well=td_state,
    )
    gold_labels = live_stage_processing.build_gold_labels(
        stage_rows=classified_df,
        stage_windows=stage_windows,
        td_state_by_well=td_state,
    )

    logger.info(
        (
            "Merged recompute plan well=%s dry_run=%s raw_rows=%s "
            "well_index_rows=%s stage_windows=%s bronze_rows=%s "
            "platinum_rows=%s gold_rows=%s"
        ),
        well_name,
        dry_run,
        len(raw_df),
        len(well_index),
        len(stage_windows),
        len(bronze_labels),
        len(platinum_labels),
        len(gold_labels),
    )

    deleted = {}
    if not dry_run:
        logger.info("Deleting existing merged outputs for well=%s", well_name)
        deleted = await _delete_well_outputs(workspace_id=workspace_id, well_name=well_name)
        logger.info("Deleted existing merged outputs for well=%s deleted=%s", well_name, deleted)
        if not well_index.is_empty():
            logger.info(
                "Writing featurestore=%s well=%s rows=%s upsert=True",
                WELL_INDEX_FEATURESTORE_KEY,
                well_name,
                len(well_index),
            )
            await write_featurestore(
                featurestore_key=WELL_INDEX_FEATURESTORE_KEY,
                workspace_id=workspace_id,
                df=well_index,
                upsert=True,
            )
            logger.info(
                "Finished featurestore=%s well=%s rows=%s",
                WELL_INDEX_FEATURESTORE_KEY,
                well_name,
                len(well_index),
            )
        logger.info(
            "Writing featurestore=%s well=%s rows=%s upsert=True",
            TARGET_FEATURESTORE_KEY,
            well_name,
            len(stage_windows),
        )
        await write_featurestore(
            featurestore_key=TARGET_FEATURESTORE_KEY,
            workspace_id=workspace_id,
            df=stage_windows,
            upsert=True,
        )
        logger.info(
            "Finished featurestore=%s well=%s rows=%s",
            TARGET_FEATURESTORE_KEY,
            well_name,
            len(stage_windows),
        )
        if not bronze_labels.is_empty():
            logger.info(
                "Writing featurestore=%s well=%s rows=%s upsert=True",
                BRONZE_LABELS_FEATURESTORE_KEY,
                well_name,
                len(bronze_labels),
            )
            await write_featurestore(
                featurestore_key=BRONZE_LABELS_FEATURESTORE_KEY,
                workspace_id=workspace_id,
                df=bronze_labels,
                upsert=True,
            )
            logger.info(
                "Finished featurestore=%s well=%s rows=%s",
                BRONZE_LABELS_FEATURESTORE_KEY,
                well_name,
                len(bronze_labels),
            )
        if not platinum_labels.is_empty():
            logger.info(
                "Writing featurestore=%s well=%s rows=%s upsert=True",
                PLATINUM_LABELS_FEATURESTORE_KEY,
                well_name,
                len(platinum_labels),
            )
            await write_featurestore(
                featurestore_key=PLATINUM_LABELS_FEATURESTORE_KEY,
                workspace_id=workspace_id,
                df=platinum_labels,
                upsert=True,
            )
            logger.info(
                "Finished featurestore=%s well=%s rows=%s",
                PLATINUM_LABELS_FEATURESTORE_KEY,
                well_name,
                len(platinum_labels),
            )
        if not gold_labels.is_empty():
            logger.info(
                "Writing featurestore=%s well=%s rows=%s upsert=True",
                GOLD_LABELS_FEATURESTORE_KEY,
                well_name,
                len(gold_labels),
            )
            await write_featurestore(
                featurestore_key=GOLD_LABELS_FEATURESTORE_KEY,
                workspace_id=workspace_id,
                df=gold_labels,
                upsert=True,
            )
            logger.info(
                "Finished featurestore=%s well=%s rows=%s",
                GOLD_LABELS_FEATURESTORE_KEY,
                well_name,
                len(gold_labels),
            )

    return {
        "well_name": well_name,
        "status": "dry_run" if dry_run else "written",
        "raw_rows": len(raw_df),
        "well_index_rows": len(well_index),
        "stage_windows": len(stage_windows),
        "bronze_labels": len(bronze_labels),
        "platinum_labels": len(platinum_labels),
        "gold_labels": len(gold_labels),
        "deleted": deleted,
    }


@flow(name="nextier-merged-well-recompute")
async def nextier_merged_well_recompute_flow(
    workspace_id: int,
    workflow_id: int,
    well_name: str | None = None,
    well_source: str = "poc_well_index",
    max_wells: int = 1,
    td_gap_seconds: int = 3600,
    dry_run: bool = True,
    reset_checkpoint: bool = False,
):
    logger = get_run_logger()
    well_source = (well_source or "poc_well_index").strip()
    checkpoint_key = f"{RECOMPUTE_CHECKPOINT_PREFIX}_{well_source}_well_name"
    state = await get_state(workflow_id=workflow_id, workspace_id=workspace_id)
    start_after_well_name = None
    if not well_name and not reset_checkpoint:
        start_after_well_name = state.get(checkpoint_key)

    well_names = await _get_well_names(
        workspace_id=workspace_id,
        well_name=well_name,
        well_source=well_source,
        max_wells=int(max_wells),
        start_after_well_name=start_after_well_name,
    )
    if not well_names:
        logger.info("No merged wells selected for recompute")
        return {"wells_processed": 0, "results": []}

    results = []
    for selected_well in well_names:
        logger.info("Recomputing merged well %s dry_run=%s", selected_well, dry_run)
        results.append(
            await _recompute_one_well(
                workspace_id=workspace_id,
                well_name=selected_well,
                td_gap_seconds=int(td_gap_seconds),
                dry_run=bool(dry_run),
            )
        )

    if results and not dry_run and not well_name:
        await set_state(
            workflow_id=workflow_id,
            workspace_id=workspace_id,
            key=checkpoint_key,
            value=well_names[-1],
        )

    return {
        "wells_processed": len(results),
        "dry_run": bool(dry_run),
        "well_source": well_source,
        "checkpoint_key": checkpoint_key,
        "start_after_well_name": start_after_well_name,
        "next_start_after_well_name": well_names[-1] if results else start_after_well_name,
        "results": results,
    }
