# MLOps Batch Signal Pipeline

A minimal, reproducible batch job that computes a rolling-mean trading signal
from OHLCV data and emits structured metrics + logs.

---
## Features

- Deterministic runs via YAML config + random seed
- Robust input validation (CSV + schema checks)
- Rolling mean signal generation
- Structured metrics output (`metrics.json`)
- Detailed logging (`run.log`)
- Fully Dockerized for one-command execution

## Project structure

```
mlops-task/
├── run.py            # Pipeline entry point
├── config.yaml       # Seed, window, version config
├── data.csv          # 10 000-row OHLCV dataset
├── requirements.txt  # Python dependencies
├── Dockerfile        # Container definition
├── README.md         # This file
├── metrics.json      # Sample output (successful run)
└── run.log           # Sample log (successful run)
```

---

## Local run

### Prerequisites
- Python 3.9+
- pip

### Install dependencies
```bash
pip install -r requirements.txt
```

### Run
```bash
python run.py \
  --input    data.csv \
  --config   config.yaml \
  --output   metrics.json \
  --log-file run.log
```

---

## Running on Windows (PowerShell)

PowerShell does not support `\` for line continuation.  
Use a single-line command:

```bash
python run.py --input data.csv --config config.yaml --output metrics.json --log-file run.log

## Docker build & run

```bash
docker build -t mlops-task .
docker run --rm mlops-task
```

The container prints the final metrics JSON to stdout and exits 0 on success.

---

## Config (`config.yaml`)

| Key     | Type   | Description                        |
|---------|--------|------------------------------------|
| seed    | int    | NumPy random seed for determinism  |
| window  | int    | Rolling-mean window size           |
| version | string | Pipeline version tag               |

---

## Processing logic

1. Load and validate config (required keys: `seed`, `window`, `version`).
2. Set `numpy.random.seed(seed)`.
3. Load and validate CSV — must be non-empty and contain a `close` column.
4. Compute `rolling_mean = close.rolling(window).mean()`.
   - The first `window - 1` rows produce NaN; NaN comparisons evaluate to False.
5. `signal = 1 if close > rolling_mean else 0` for each row (NaN → 0).
6. Emit `metrics.json` and `run.log`; print metrics to stdout.
7. `rows_processed` = total rows (including NaN window rows).
   `signal_rate` = mean(signal) across all rows.

---

## Example `metrics.json`

```json
{
  "version": "v1",
  "rows_processed": 10000,
  "metric": "signal_rate",
  "value": 0.5123,
  "latency_ms": 24,
  "seed": 42,
  "status": "success"
}
```

Error output format:

```json
{
  "version": "v1",
  "status": "error",
  "error_message": "Description of what went wrong",
  "latency_ms": 5
}
```

---


## Snaps

<img width="852" height="678" alt="Screenshot 2026-04-14 222622" src="https://github.com/user-attachments/assets/75b728bd-75fb-416f-900f-6ae9c94d7080" />
<img width="1070" height="372" alt="Screenshot 2026-04-14 222731" src="https://github.com/user-attachments/assets/5c3cf995-9719-4115-b985-470619f10205" />



## Exit codes

| Code | Meaning        |
|------|----------------|
| 0    | Success        |
| 1    | Pipeline error |
