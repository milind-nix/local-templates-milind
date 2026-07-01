from __future__ import annotations

from pathlib import Path
import sys
from typing import Any

import polars as pl
from prefect import flow, get_run_logger

from nixdlt.workflow_sdk.platform_tasks import (
    delete_featurestore_records,
    get_state,
    query_store,
    set_state,
    write_datastore,
    write_featurestore,
)


_WORKFLOWS_DIR = Path(__file__).resolve().parents[1]
if str(_WORKFLOWS_DIR) not in sys.path:
    sys.path.insert(0, str(_WORKFLOWS_DIR))

from nextier_live_stage_processing import main as live_stage_processing
from nextier_merged_well_recompute import main as merged_well_recompute


LIVE_SOURCE_DATASTORE_KEY = "live_fleet_stream_customer_full_v1"
MERGED_SOURCE_DATASTORE_KEY = "merged_fleet_stream_customer_full_v2"
WELL_INDEX_FEATURESTORE_KEY = "merged_well_index_v1"
TARGET_FEATURESTORE_KEY = "merged_td_stage_index_v1"
BRONZE_LABELS_FEATURESTORE_KEY = "merged_bronze_labels_v1"
GOLD_LABELS_FEATURESTORE_KEY = "merged_gold_labels_v1"
PLATINUM_LABELS_FEATURESTORE_KEY = "merged_platinum_labels_v1"
CHECKPOINT_KEY = "__checkpoint__merged_live_stage_created_ts"
TD_STATE_KEY = "merged_td_stage_state_by_well"


def _well_names_from_frame(df: pl.DataFrame) -> list[str]:
    if df.is_empty() or "name" not in df.columns:
        return []
    return sorted({str(name) for name in df["name"].drop_nulls().to_list()})


def _values_cte(values: list[str], key_prefix: str) -> tuple[str, dict[str, str]]:
    params: dict[str, str] = {}
    selects: list[str] = []
    for idx, value in enumerate(values):
        key = f"{key_prefix}_{idx}"
        params[key] = value
        selects.append(f"SELECT :{key} AS name")
    return " UNION ALL ".join(selects), params


async def _refresh_merged_well_index_for_wells(
    workspace_id: int,
    well_names: list[str],
) -> int:
    if not well_names:
        return 0

    cte_sql, params = _values_cte(well_names, "well_index_name")
    raw_df = await query_store(
        sql=f"""
            WITH target_wells AS (
              {cte_sql}
            )
            SELECT
              t.created_ts,
              t.fleet_name,
              t.pad_name,
              t.record_ts,
              t.name,
              t.id,
              t.api_num
            FROM datastore:{MERGED_SOURCE_DATASTORE_KEY} t
            JOIN target_wells w
              ON t.name = w.name
            WHERE
              t.created_ts IS NOT NULL
              AND t.record_ts IS NOT NULL
              AND t.name IS NOT NULL
            ORDER BY
              t.name ASC,
              t.record_ts ASC,
              t.created_ts ASC
        """,
        workspace_id=workspace_id,
        params=params,
    )
    if raw_df.is_empty():
        return 0

    well_index = merged_well_recompute._build_well_index(raw_df)
    if well_index.is_empty():
        return 0

    await write_featurestore(
        featurestore_key=WELL_INDEX_FEATURESTORE_KEY,
        workspace_id=workspace_id,
        df=well_index,
        upsert=True,
    )
    return len(well_index)


@flow(name="nextier-merged-live-processing")
async def nextier_merged_live_processing_flow(
    workspace_id: int,
    workflow_id: int,
    batch_limit: int = 200000,
    td_gap_seconds: int = 3600,
):
    logger = get_run_logger()
    state = await get_state(workflow_id=workflow_id, workspace_id=workspace_id)

    raw_live_df = await query_store(
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
              prop_conc_target,
              prop_conc_blend_denso,
              prop_conc_blend_auger,
              prop_conc_inline,
              press_mainline
            FROM datastore:{LIVE_SOURCE_DATASTORE_KEY}
            WHERE
              created_ts IS NOT NULL
              AND record_ts IS NOT NULL
              AND name IS NOT NULL
              AND created_ts > :{CHECKPOINT_KEY}
            ORDER BY
              created_ts ASC,
              record_ts ASC,
              name ASC
            LIMIT {batch_limit}
        """,
        workspace_id=workspace_id,
        params=state,
    )

    if raw_live_df.is_empty():
        logger.info("No new live telemetry rows to merge and process")
        return {
            "rows_seeded": 0,
            "rows_processed": 0,
            "stage_windows_upserted": 0,
            "well_index_rows_upserted": 0,
        }

    logger.info(
        "Seeding merged raw datastore=%s rows=%s",
        MERGED_SOURCE_DATASTORE_KEY,
        len(raw_live_df),
    )
    await write_datastore(
        datastore_key=MERGED_SOURCE_DATASTORE_KEY,
        workspace_id=workspace_id,
        df=raw_live_df,
        upsert=True,
    )

    existing_windows = await query_store(
        sql=f"""
            SELECT
              name,
              stage_num_td,
              fleet_name,
              pad_name,
              well_id,
              api_num,
              stage_start_ts,
              stage_end_ts,
              first_created_ts,
              last_created_ts,
              sample_count,
              avg_rate_slurry,
              max_rate_slurry,
              avg_press_mainline,
              max_press_mainline,
              avg_prop_conc_blend_denso,
              max_prop_conc_blend_denso,
              stage_detection_method
            FROM featurestore:{TARGET_FEATURESTORE_KEY}
        """,
        workspace_id=workspace_id,
    )

    normal_raw_df, late_raw_df = live_stage_processing.split_late_telemetry_rows(
        raw_df=raw_live_df,
        td_state_by_well=state.get(TD_STATE_KEY) or {},
    )
    classified_normal_df, next_td_state = live_stage_processing.assign_td_stages(
        raw_df=normal_raw_df,
        td_state_by_well=state.get(TD_STATE_KEY) or {},
        td_gap_seconds=int(td_gap_seconds),
    )
    classified_late_df, repair_wells = (
        live_stage_processing.assign_late_rows_to_existing_stages(
            late_df=late_raw_df,
            existing_windows=existing_windows,
        )
    )
    repair_well_set = set(repair_wells)
    classified_normal_df = live_stage_processing._filter_out_wells(
        classified_normal_df,
        repair_well_set,
    )
    classified_late_df = live_stage_processing._filter_out_wells(
        classified_late_df,
        repair_well_set,
    )
    classified_frames = [
        df for df in [classified_normal_df, classified_late_df] if not df.is_empty()
    ]

    windows_to_write = pl.DataFrame()
    bronze_labels = live_stage_processing._empty_bronze_labels()
    platinum_labels = live_stage_processing._empty_platinum_labels()
    gold_labels = live_stage_processing._empty_gold_labels()

    if classified_frames:
        classified_df = pl.concat(classified_frames, how="diagonal_relaxed")
        new_windows = live_stage_processing.build_stage_windows(classified_df)
        if not new_windows.is_empty():
            windows_to_write = live_stage_processing.merge_existing_stage_windows(
                new_windows,
                existing_windows,
            )
            live_stage_processing.validate_stage_window_overlaps(
                existing_windows=existing_windows,
                windows_to_write=windows_to_write,
            )

            await write_featurestore(
                featurestore_key=TARGET_FEATURESTORE_KEY,
                workspace_id=workspace_id,
                df=windows_to_write,
                upsert=True,
            )

            windows_pd = windows_to_write.to_pandas()
            affected_params: dict[str, Any] = {}
            affected_values: list[str] = []
            for idx, row in windows_pd.reset_index(drop=True).iterrows():
                name_key = f"affected_name_{idx}"
                start_key = f"affected_start_{idx}"
                end_key = f"affected_end_{idx}"
                affected_params[name_key] = str(row["name"])
                affected_params[start_key] = live_stage_processing._format_dt(
                    row["stage_start_ts"],
                )
                affected_params[end_key] = live_stage_processing._format_dt(
                    row["stage_end_ts"],
                )
                affected_values.append(
                    f"SELECT :{name_key} AS name, "
                    f"CAST(:{start_key} AS timestamp) AS stage_start_ts, "
                    f"CAST(:{end_key} AS timestamp) AS stage_end_ts"
                )

            affected_stage_rows = await query_store(
                sql=f"""
                    WITH affected_windows AS (
                      {" UNION ALL ".join(affected_values)}
                    )
                    SELECT DISTINCT
                      t.created_ts,
                      t.fleet_name,
                      t.pad_name,
                      t.record_ts,
                      t.name,
                      t.id,
                      t.api_num,
                      t.rate_slurry,
                      t.prop_conc_blend_denso,
                      t.press_mainline
                    FROM datastore:{MERGED_SOURCE_DATASTORE_KEY} t
                    JOIN affected_windows w
                      ON t.name = w.name
                     AND t.record_ts >= w.stage_start_ts
                     AND t.record_ts <= w.stage_end_ts
                    ORDER BY
                      t.name ASC,
                      t.record_ts ASC
                """,
                workspace_id=workspace_id,
                params=affected_params,
            )
            bronze_labels = live_stage_processing.build_bronze_labels(
                stage_rows=affected_stage_rows,
                stage_windows=windows_to_write,
            )
            if not bronze_labels.is_empty():
                await write_featurestore(
                    featurestore_key=BRONZE_LABELS_FEATURESTORE_KEY,
                    workspace_id=workspace_id,
                    df=bronze_labels,
                    upsert=True,
                )

            platinum_labels = live_stage_processing.build_platinum_labels(
                stage_rows=affected_stage_rows,
                stage_windows=windows_to_write,
                bronze_labels=bronze_labels,
                td_state_by_well=next_td_state,
            )
            if not platinum_labels.is_empty():
                await write_featurestore(
                    featurestore_key=PLATINUM_LABELS_FEATURESTORE_KEY,
                    workspace_id=workspace_id,
                    df=platinum_labels,
                    upsert=True,
                )

            gold_labels = live_stage_processing.build_gold_labels(
                stage_rows=affected_stage_rows,
                stage_windows=windows_to_write,
                td_state_by_well=next_td_state,
            )
            if not gold_labels.is_empty():
                await write_featurestore(
                    featurestore_key=GOLD_LABELS_FEATURESTORE_KEY,
                    workspace_id=workspace_id,
                    df=gold_labels,
                    upsert=True,
                )

    repair_stage_windows = pl.DataFrame()
    repair_bronze_labels = live_stage_processing._empty_bronze_labels()
    repair_platinum_labels = live_stage_processing._empty_platinum_labels()
    repair_gold_labels = live_stage_processing._empty_gold_labels()
    if repair_wells:
        logger.warning("Running full merged TD stage repair for wells: %s", repair_wells)
        repair_params: dict[str, Any] = {}
        repair_values: list[str] = []
        for idx, well_name in enumerate(repair_wells):
            key = f"repair_well_{idx}"
            repair_params[key] = well_name
            repair_values.append(f"SELECT :{key} AS name")

        repair_raw_df = await query_store(
            sql=f"""
                WITH repair_wells AS (
                  {" UNION ALL ".join(repair_values)}
                )
                SELECT
                  t.created_ts,
                  t.fleet_name,
                  t.pad_name,
                  t.record_ts,
                  t.name,
                  t.id,
                  t.api_num,
                  t.rate_slurry,
                  t.prop_conc_blend_denso,
                  t.press_mainline
                FROM datastore:{MERGED_SOURCE_DATASTORE_KEY} t
                JOIN repair_wells w
                  ON t.name = w.name
                WHERE
                  t.created_ts IS NOT NULL
                  AND t.record_ts IS NOT NULL
                  AND t.name IS NOT NULL
                ORDER BY
                  t.name ASC,
                  t.record_ts ASC,
                  t.created_ts ASC
            """,
            workspace_id=workspace_id,
            params=repair_params,
        )
        repair_classified_df, repair_td_state = live_stage_processing.assign_td_stages(
            raw_df=repair_raw_df,
            td_state_by_well={},
            td_gap_seconds=int(td_gap_seconds),
        )
        repair_stage_windows = live_stage_processing.build_stage_windows(
            repair_classified_df,
        )
        if repair_raw_df.is_empty() or repair_stage_windows.is_empty():
            raise ValueError(
                "Targeted merged TD stage repair produced no replacement rows; refusing to delete existing generated rows"
            )
        live_stage_processing.validate_stage_window_overlaps(
            existing_windows=pl.DataFrame(),
            windows_to_write=repair_stage_windows,
        )

        for well_name in repair_wells:
            filters = [{"field": "name", "op": "eq", "value": well_name}]
            await delete_featurestore_records(
                featurestore_key=BRONZE_LABELS_FEATURESTORE_KEY,
                workspace_id=workspace_id,
                filters=filters,
            )
            await delete_featurestore_records(
                featurestore_key=GOLD_LABELS_FEATURESTORE_KEY,
                workspace_id=workspace_id,
                filters=filters,
            )
            await delete_featurestore_records(
                featurestore_key=PLATINUM_LABELS_FEATURESTORE_KEY,
                workspace_id=workspace_id,
                filters=filters,
            )
            await delete_featurestore_records(
                featurestore_key=TARGET_FEATURESTORE_KEY,
                workspace_id=workspace_id,
                filters=filters,
            )

        if not repair_stage_windows.is_empty():
            await write_featurestore(
                featurestore_key=TARGET_FEATURESTORE_KEY,
                workspace_id=workspace_id,
                df=repair_stage_windows,
                upsert=True,
            )
            repair_bronze_labels = live_stage_processing.build_bronze_labels(
                stage_rows=repair_raw_df,
                stage_windows=repair_stage_windows,
            )
            if not repair_bronze_labels.is_empty():
                await write_featurestore(
                    featurestore_key=BRONZE_LABELS_FEATURESTORE_KEY,
                    workspace_id=workspace_id,
                    df=repair_bronze_labels,
                    upsert=True,
                )
            repair_platinum_labels = live_stage_processing.build_platinum_labels(
                stage_rows=repair_raw_df,
                stage_windows=repair_stage_windows,
                bronze_labels=repair_bronze_labels,
                td_state_by_well=repair_td_state,
            )
            if not repair_platinum_labels.is_empty():
                await write_featurestore(
                    featurestore_key=PLATINUM_LABELS_FEATURESTORE_KEY,
                    workspace_id=workspace_id,
                    df=repair_platinum_labels,
                    upsert=True,
                )
            repair_gold_labels = live_stage_processing.build_gold_labels(
                stage_rows=repair_raw_df,
                stage_windows=repair_stage_windows,
                td_state_by_well=repair_td_state,
            )
            if not repair_gold_labels.is_empty():
                await write_featurestore(
                    featurestore_key=GOLD_LABELS_FEATURESTORE_KEY,
                    workspace_id=workspace_id,
                    df=repair_gold_labels,
                    upsert=True,
                )
            next_td_state.update(repair_td_state)

    affected_wells = sorted(
        set(_well_names_from_frame(raw_live_df))
        | set(_well_names_from_frame(windows_to_write))
        | set(repair_wells)
    )
    well_index_rows = await _refresh_merged_well_index_for_wells(
        workspace_id=workspace_id,
        well_names=affected_wells,
    )

    checkpoint = raw_live_df["created_ts"].max()
    await set_state(
        workflow_id=workflow_id,
        workspace_id=workspace_id,
        key=CHECKPOINT_KEY,
        value=str(checkpoint),
    )
    await set_state(
        workflow_id=workflow_id,
        workspace_id=workspace_id,
        key=TD_STATE_KEY,
        value=next_td_state,
    )

    logger.info(
        "Merged live processed rows=%s windows=%s bronze=%s platinum=%s gold=%s repair_wells=%s repair_windows=%s well_index_rows=%s checkpoint=%s",
        len(raw_live_df),
        len(windows_to_write),
        len(bronze_labels),
        len(platinum_labels),
        len(gold_labels),
        len(repair_wells),
        len(repair_stage_windows),
        well_index_rows,
        checkpoint,
    )
    return {
        "rows_seeded": len(raw_live_df),
        "rows_processed": len(raw_live_df),
        "stage_windows_upserted": len(windows_to_write),
        "bronze_labels_upserted": len(bronze_labels),
        "platinum_labels_upserted": len(platinum_labels),
        "gold_labels_upserted": len(gold_labels),
        "repair_wells": repair_wells,
        "repair_stage_windows_rewritten": len(repair_stage_windows),
        "repair_bronze_labels_rewritten": len(repair_bronze_labels),
        "repair_platinum_labels_rewritten": len(repair_platinum_labels),
        "repair_gold_labels_rewritten": len(repair_gold_labels),
        "well_index_rows_upserted": well_index_rows,
        CHECKPOINT_KEY: str(checkpoint),
    }
