# MLOps Batch Signal Pipeline

A minimal, reproducible batch job that computes a rolling-mean trading signal
from OHLCV data and emits structured metrics + logs.

---

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

## Exit codes

| Code | Meaning        |
|------|----------------|
| 0    | Success        |
| 1    | Pipeline error |
