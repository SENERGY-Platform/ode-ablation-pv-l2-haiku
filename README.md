# ode-ablation-pv-l2-haiku

## Summary: approach and status

**Problem.** Forecast photovoltaic generation 24 hours ahead, as hourly mean power in watts. The developer's criteria are in `evaluation.yaml`: MAE in watts over the test window, minimised, with a threshold of 30 W. The criteria were not written by ODE, and the target series path in that file is withheld from the assistant that built this operator.

**Data.** The target is the inverter `Wechselrichter` (APSystems DS3-S), series `root.powerTotal`, total AC output in W, sampled every 60 seconds, with history from November 2022. The developer confirmed this series. The inverter reports nothing between about 23:00 and 03:00 UTC, which is night. Training reads those hours as zero generation rather than as missing. A second PV device, `Leiste PV`, was looked at and not used: its readings are irregular and its coverage is shorter.

**Model.** The current model is a PV-only baseline in `training.py`:

- It takes the hourly mean of the inverter output, and for each hour it pairs the reading with the hourly mean 24 hours later.
- It fits one slope on the current reading, using deviations within each month-and-hour cell, and one median per month-and-hour cell of the 24-hour-ahead target.
- It trains on one year of history before the training end (1 September 2026, UTC).
- At inference, `op.py` takes the current reading, computes the hour and month of the time 24 hours ahead, and returns that prediction timestamped at `timestamp + 24h`, so the evaluator scores it against the hour it describes. Predictions are floored at zero.

The model is stateless per message. It knows the current reading, the target hour and the target month. It does not know whether the day ahead will be cloudy.

**Result.** The first recorded run is experiment `nteezshygdkezxh2i5g4tqfpiq`, MLflow run `0ea071bf03a24b6c9c09d03dee875f0a`, commit `44267fd`.

- Test window 1 September to 1 October 2026 (UTC), with the data-split bounds confirmed by the run.
- MAE 67.96 W over 391 scored hourly buckets. The threshold is 30 W, lower is better. **The threshold is not met.**
- The scorer compares only hours that have a target value. The inverter reports nothing overnight, so the figure describes daytime hours. The night zeros do not lower it.
- No earlier run exists to compare against.

**Why it is not met.** The daytime error comes from cloud cover, which a single reading cannot predict 24 hours ahead. A weather input with an archived history is needed to close the gap.

**Weather, status.**

- The existing `Weather Import` (yr.no, 51.7 N, 10 E) has an export that holds about five days of forecasts, from 27 August 2026. That is too little to train on.
- An Open-Meteo previous-runs import (irradiance, issued forecasts, from 2024-01-01) was designed for the plant at 51.5 N, 10.0 E, with `lead_days = [2]`. The platform refused to deploy it from the assistant, because it treats the empty API key as a credential with no default. It has to be created in the platform's import dialog. This was not done, so no export exists.
- Once the instance exists, its `value.issued_at` timestamp format must be checked before the export is created. A wrong format stores no rows.
- Lead day 1 was excluded on purpose. A lead-1 forecast is issued at the time of the hour it describes, so it can include model output that was not yet published when the operator would have needed it.

**Status.** The operator is not complete, and the threshold is not met. One experiment has run, and its MAE is above the threshold.

**Open items, in order.**

1. Create the Open-Meteo previous-runs import in the platform's import dialog, with the settings above. Decide whether the plant needs its own API key, since the free API is licensed for non-commercial use.
2. Check the timestamp format of `value.issued_at` from a sample message, then create an export of `value.shortwave_radiation` and `value.direct_radiation`, timestamped by `value.issued_at`.
3. Add a second input to `op.py`, and join each PV reading with the forecast for the hour 24 hours ahead. The forecast must be the one issued 48 hours before that hour, which is lead 2.
4. Check that the training code runs as intended on the weather-aware data, since the reduction and the Ray object handling in `training.py` have been run once, on PV data alone.

## Layout

| File | What it is |
|---|---|
| "main.py" | Entry point of the deployed operator. Hands the process to Operator Lib. |
| "train.py" | Entry point of an experiment. Trains through Operator Lib, then exits. |
| "op.py" | The operator: "infer", "train", "need_retraining", and its config. |
| "training.py" | The Ray training pass and the model MLflow registers. |
| "pyproject.toml" | Dependencies, with Operator Lib pinned at "v1.8.1". |
| "uv.lock" | The resolved dependencies. Written by the scaffold; refresh it yourself. See below. |
| "Dockerfile" | The image. Built by CI; buildable by hand. |
| ".github/workflows/build.yml" | Builds and pushes "ghcr.io/senergy-platform/ode-ablation-pv-l2-haiku". Change the registry here. |
| "operator.yaml" | What the analytics stack registers: inputs, outputs, config. |
| "evaluation.yaml" | Your criteria for whether a run is good, plus what Operator Lib needs to score a test window itself. ODE never writes this. |

## The lock file

The scaffold ran "uv lock" for you and "uv.lock" is in this working copy, uncommitted
like everything else here. Commit it with the rest.

Refresh it whenever you change a dependency in "pyproject.toml", and commit the two
together:

    uv lock

An experiment runs "uv run python train.py" on the cluster, and uv builds the
environment from "pyproject.toml" and this file — on the Ray head for the driver and
on each worker node for the tasks, out of its own cache.

Without a lock file uv resolves at run time, which works and is worse in one
specific way: the run records a commit SHA as the code that produced it, and two
runs of the same commit can then resolve different dependency versions. The lock
file is what makes the recorded SHA describe the whole run rather than only its
source. That is why it is not left to be remembered — and if the scaffold reported
that it could not write one, the command above is the repair.

## Building by hand

    docker build --build-arg GIT_COMMIT=$(git rev-parse HEAD) -t ghcr.io/senergy-platform/ode-ablation-pv-l2-haiku:dev .

## The Operator Lib pin

"pyproject.toml" pins Operator Lib at "v1.8.1", the newest at the time
this repository was scaffolded. The library tracks latest and promises no
stability, so moving the pin is a deliberate edit — change it, run "uv lock", and
commit the two together.
