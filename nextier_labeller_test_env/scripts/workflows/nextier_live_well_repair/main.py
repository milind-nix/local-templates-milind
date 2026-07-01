from __future__ import annotations

import importlib.util
from pathlib import Path

from prefect import flow, get_run_logger

from nixdlt.workflow_sdk.platform_tasks import query_store, write_featurestore


_LIVE_STAGE_PROCESSING_PATH = (
    Path(__file__).resolve().parents[1] / "nextier_live_stage_processing" / "main.py"
)
_SPEC = importlib.util.spec_from_file_location(
    "nextier_live_stage_processing_main",
    _LIVE_STAGE_PROCESSING_PATH,
)
if _SPEC is None or _SPEC.loader is None:
    raise RuntimeError(f"Unable to load {_LIVE_STAGE_PROCESSING_PATH}")
live_stage_processing = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(live_stage_processing)


@flow(name="nextier-live-well-repair")
async def nextier_live_well_repair_flow(
    workspace_id: int,
    workflow_id: int,
    well_name: str,
    td_gap_seconds: int = 3600,
):
    logger = get_run_logger()
    if not well_name or not well_name.strip():
        raise ValueError("well_name is required")
    well_name = well_name.strip()

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
            FROM datastore:{live_stage_processing.SOURCE_DATASTORE_KEY}
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
        raise ValueError(f"No raw live telemetry rows found for well {well_name!r}")

    classified_df, td_state = live_stage_processing.assign_td_stages(
        raw_df=raw_df,
        td_state_by_well={},
        td_gap_seconds=int(td_gap_seconds),
    )
    stage_windows = live_stage_processing.build_stage_windows(classified_df)
    if stage_windows.is_empty():
        raise ValueError(f"Unable to build TD stage windows for well {well_name!r}")

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

    await write_featurestore(
        featurestore_key=live_stage_processing.TARGET_FEATURESTORE_KEY,
        workspace_id=workspace_id,
        df=stage_windows,
        upsert=True,
    )
    if not bronze_labels.is_empty():
        await write_featurestore(
            featurestore_key=live_stage_processing.BRONZE_LABELS_FEATURESTORE_KEY,
            workspace_id=workspace_id,
            df=bronze_labels,
            upsert=True,
        )
    if not platinum_labels.is_empty():
        await write_featurestore(
            featurestore_key=live_stage_processing.PLATINUM_LABELS_FEATURESTORE_KEY,
            workspace_id=workspace_id,
            df=platinum_labels,
            upsert=True,
        )
    if not gold_labels.is_empty():
        await write_featurestore(
            featurestore_key=live_stage_processing.GOLD_LABELS_FEATURESTORE_KEY,
            workspace_id=workspace_id,
            df=gold_labels,
            upsert=True,
        )

    logger.info(
        "Rebuilt well=%s with %s raw rows, %s TD stage windows, %s Bronze labels, %s Platinum labels, and %s Gold labels",
        well_name,
        len(raw_df),
        len(stage_windows),
        len(bronze_labels),
        len(platinum_labels),
        len(gold_labels),
    )
    return {
        "well_name": well_name,
        "raw_rows": len(raw_df),
        "stage_windows": len(stage_windows),
        "bronze_labels": len(bronze_labels),
        "platinum_labels": len(platinum_labels),
        "gold_labels": len(gold_labels),
    }
