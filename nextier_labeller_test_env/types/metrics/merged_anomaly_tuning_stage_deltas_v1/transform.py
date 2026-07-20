#!/usr/bin/env python3
from __future__ import annotations

import json
import sys

import numpy as np
import pandas as pd
import polars as pl
from sqlalchemy import create_engine, text

from nextier_core.anomaly_detection import detect_design_delta_anomalies


def empty_output(path: str) -> None:
    pl.DataFrame(
        schema={
            "timestamp": pl.Datetime,
            "delta_pressure": pl.Float64,
            "delta_rate": pl.Float64,
            "pressure_trigger": pl.Float64,
            "rate_trigger": pl.Float64,
            "pressure_threshold": pl.Float64,
            "negative_rate_threshold": pl.Float64,
        }
    ).write_parquet(path)


def build_output(
    frame: pd.DataFrame,
    pressure_threshold: float,
    rate_threshold: float,
    rolling_points: int,
) -> pd.DataFrame:
    design_mask = (
        frame["substage"]
        .astype("string")
        .str.strip()
        .str.lower()
        .isin(["design", "slurry"])
        .fillna(False)
        .to_numpy(dtype=bool)
    )
    pressure_mask, rate_mask, delta_pressure, delta_rate = (
        detect_design_delta_anomalies(
            frame,
            design_mask=design_mask,
            pressure_threshold_psi=pressure_threshold,
            rate_drop_threshold_bpm=rate_threshold,
            rolling_points=rolling_points,
        )
    )
    return pd.DataFrame(
        {
            "timestamp": pd.to_datetime(frame["datetime_fmt"], errors="coerce"),
            "delta_pressure": delta_pressure.astype(float),
            "delta_rate": delta_rate.astype(float),
            "pressure_trigger": np.where(
                pressure_mask, delta_pressure.astype(float), np.nan
            ),
            "rate_trigger": np.where(
                rate_mask, delta_rate.astype(float), np.nan
            ),
            "pressure_threshold": pressure_threshold,
            "negative_rate_threshold": -rate_threshold,
        }
    )


def main() -> None:
    params_path, output_path = sys.argv[1], sys.argv[2]
    params = json.loads(open(params_path).read())
    workspace_id = int(params["__workspace_id__"])
    well_name = str(params["well_name"])
    stage_num = float(params["stage_num"])
    pressure_threshold = float(params["pressure_threshold"])
    rate_threshold = float(params["rate_threshold"])
    rolling_points = int(params["rolling_points"])
    table_name = f"merged_platinum_labels_v1_wid{workspace_id}"

    engine = create_engine(params["__connection_url__"])
    query = text(
        f"""
        SELECT
          CAST(datetime_fmt AS timestamp) AS datetime_fmt,
          substage,
          rate_slurry,
          press_mainline
        FROM {table_name}
        WHERE name = :well_name
          AND stage_num_td = :stage_num
        ORDER BY CAST(datetime_fmt AS timestamp)
        """
    )
    with engine.connect() as connection:
        frame = pd.read_sql_query(
            query,
            connection,
            params={"well_name": well_name, "stage_num": stage_num},
        )
    engine.dispose()
    if frame.empty:
        empty_output(output_path)
        return

    result = build_output(
        frame,
        pressure_threshold,
        rate_threshold,
        rolling_points,
    )
    pl.from_pandas(result).write_parquet(output_path)


if __name__ == "__main__":
    main()
