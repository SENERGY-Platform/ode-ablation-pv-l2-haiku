"""Training, on Ray.

Separate from op.py because the two run in different places: op.py runs in the
operator's own process for every message, while this runs distributed and rarely.

The model forecasts the hourly mean PV power 24 hours ahead from one instantaneous
reading. It is stateless per message: the prediction depends on the current value
and on the hour and month of the time being forecast. That is the whole input the
operator receives, so nothing else is claimed.

provide_historic_data() returns one Ray object reference per input topic. Each is
resolved before it is read here.
"""

import datetime
import typing

import numpy as np
import pandas as pd
import ray
from mlflow.pyfunc import PythonModel

from operator_lib.util.helpers import provide_historic_data, TrainMlflowLogger


# How much history one training pass reads. A year, so that every month of the
# forecast season has been seen at least once before the test window.
TRAINING_WINDOW = datetime.timedelta(days=365)

# The forecast horizon, and the reason the output is timestamped in the future.
HORIZON = datetime.timedelta(hours=24)


class OdeAblationPvL2HaikuModel(PythonModel):
    """The model MLflow registers and op.py later loads.

    predict() receives exactly the payload infer() builds, so the two are one
    contract in two files. Keep it serialisable: MLflow stores this object.

    The prediction is table[(month, hour)] + slope * value, floored at zero.
    table holds the median hourly mean for the target hour, after removing the
    slope times the reading taken 24 hours earlier.
    """

    def __init__(self, table: typing.Dict[str, float], slope: float, fallback: float) -> None:
        self.table = table
        self.slope = slope
        self.fallback = fallback

    def predict(self, context, model_input=None, params=None):
        payload = model_input if model_input is not None else context
        value = float(payload.get("value", 0.0))
        month = int(payload.get("month", 1))
        hour = int(payload.get("hour", 0))
        base = self.table.get(f"{month}-{hour}", self.fallback)
        return max(0.0, float(base + self.slope * value))


@ray.remote
def _hourly_means(datasets: typing.List[typing.Any]) -> pd.DataFrame:
    """Read every input dataset and reduce it to hourly means of the value.

    A Ray task, so the raw rows are read where the data is and only the hourly
    series comes back to the driver. The value column is named by the mapping
    destination the operator is configured with, which is "value". Hours with no
    reading are left empty here and resolved in _fit_table.
    """
    frames = []
    for ref in datasets:
        dataset = ray.get(ref) if isinstance(ref, ray.ObjectRef) else ref
        for batch in dataset.iter_batches(batch_format="pandas", batch_size=65536):
            if "time" not in batch or "value" not in batch:
                continue
            frames.append(
                pd.DataFrame(
                    {
                        "time": pd.to_datetime(batch["time"], utc=True, errors="coerce"),
                        "value": pd.to_numeric(batch["value"], errors="coerce"),
                    }
                )
            )
    if not frames:
        return pd.DataFrame(columns=["time", "value"])
    raw = pd.concat(frames, ignore_index=True).dropna()
    hourly = raw.set_index("time")["value"].resample("1h").mean()
    return hourly.reset_index()


def _fit_table(hourly: pd.DataFrame) -> typing.Tuple[typing.Dict[str, float], float, float, int]:
    """Fit the slope and the per-(month, hour) table on pairs 24 hours apart.

    An hour with no reading is zero generation. The inverter stops reporting after
    dark, so the night hours are empty in the raw data and must count as zero, not
    be dropped. This is the one place where a gap is given a value, and it is
    deliberate.
    """
    series = hourly.set_index("time")["value"].asfreq("1h").fillna(0.0)
    pairs = pd.DataFrame(
        {
            "x": series,
            "y": series.shift(-24),
        },
        index=series.index,
    ).dropna()
    target_time = pairs.index + HORIZON
    pairs["month"] = target_time.month
    pairs["hour"] = target_time.hour

    # Slope on the deviation from each cell's own mean, so the seasonal shape
    # does not leak into the coefficient on the current value.
    grouped = pairs.groupby(["month", "hour"])
    xd = pairs["x"] - grouped["x"].transform("mean")
    yd = pairs["y"] - grouped["y"].transform("mean")
    denom = float((xd * xd).sum())
    slope = float((xd * yd).sum() / denom) if denom > 1e-9 else 0.0

    pairs["base"] = pairs["y"] - slope * pairs["x"]
    medians = pairs.groupby(["month", "hour"])["base"].median()
    table = {f"{int(m)}-{int(h)}": float(v) for (m, h), v in medians.items()}
    fallback = float(pairs["base"].median())
    return table, slope, fallback, len(pairs)


def train_model(logger: TrainMlflowLogger) -> typing.Optional[PythonModel]:
    """Read the history, fit, and hand back a model for MLflow to register."""
    with logger.trace("read history"):
        datasets = provide_historic_data(TRAINING_WINDOW)
    if not datasets:
        # Explicitly nothing rather than a model fitted on no data: returning None
        # leaves the previously registered model in place.
        return None

    with logger.trace("reduce to hourly means"):
        hourly = ray.get(_hourly_means.remote(datasets))
    if hourly.empty:
        return None

    with logger.trace("fit"):
        table, slope, fallback, n_pairs = _fit_table(hourly)

    logger.log_params(
        {
            "training_window_days": TRAINING_WINDOW.days,
            "horizon_hours": int(HORIZON.total_seconds() // 3600),
            "slope": round(slope, 6),
            "pairs": n_pairs,
        }
    )
    logger.log_metrics({"fallback_w": fallback, "cells": float(len(table))})
    return OdeAblationPvL2HaikuModel(table=table, slope=slope, fallback=fallback)
