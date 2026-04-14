"""
MLOps batch job: rolling-mean signal pipeline.
Usage:
    python run.py --input data.csv --config config.yaml \
                  --output metrics.json --log-file run.log
"""
import argparse
import json
import logging
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd
import yaml


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def build_logger(log_file: str) -> logging.Logger:
    logger = logging.getLogger("mlops")
    logger.setLevel(logging.DEBUG)
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

    fh = logging.FileHandler(log_file, mode="w")
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    logger.addHandler(sh)

    return logger


def write_metrics(path: str, payload: dict) -> None:
    with open(path, "w") as f:
        json.dump(payload, f, indent=2)


def load_config(config_path: str) -> dict:
    p = Path(config_path)
    if not p.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")
    with open(p) as f:
        cfg = yaml.safe_load(f)
    if not isinstance(cfg, dict):
        raise ValueError("Config must be a YAML mapping.")
    required = {"seed", "window", "version"}
    missing = required - cfg.keys()
    if missing:
        raise ValueError(f"Config missing required keys: {missing}")
    if not isinstance(cfg["seed"], int):
        raise ValueError("Config 'seed' must be an integer.")
    if not isinstance(cfg["window"], int) or cfg["window"] < 1:
        raise ValueError("Config 'window' must be a positive integer.")
    if not isinstance(cfg["version"], str):
        raise ValueError("Config 'version' must be a string.")
    return cfg


def load_dataset(input_path: str) -> pd.DataFrame:
    p = Path(input_path)
    if not p.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")
    try:
        df = pd.read_csv(p)
    except Exception as exc:
        raise ValueError(f"Failed to parse CSV: {exc}") from exc
    if df.empty:
        raise ValueError("Input CSV is empty.")
    if "close" not in df.columns:
        raise ValueError(f"Required column 'close' not found. Columns: {list(df.columns)}")
    return df


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

def run_pipeline(args: argparse.Namespace) -> None:
    start_ts = time.time()
    logger = build_logger(args.log_file)
    version = "unknown"

    try:
        logger.info("Job started.")

        # 1) Config
        cfg = load_config(args.config)
        version = cfg["version"]
        seed    = cfg["seed"]
        window  = cfg["window"]
        logger.info(
            "Config loaded — version=%s  seed=%d  window=%d",
            version, seed, window,
        )

        # 2) Seed
        np.random.seed(seed)
        logger.info("NumPy random seed set to %d.", seed)

        # 3) Dataset
        df = load_dataset(args.input)
        logger.info("Dataset loaded: %d rows, columns=%s", len(df), list(df.columns))

        # 4) Rolling mean (first window-1 rows will be NaN; excluded from signal)
        df["rolling_mean"] = df["close"].rolling(window=window).mean()
        valid_mask = df["rolling_mean"].notna()
        logger.info(
            "Rolling mean computed (window=%d). Valid rows: %d / %d.",
            window, valid_mask.sum(), len(df),
        )

        # 5) Signal
        # NaN rolling_mean rows → comparison is False → signal = 0 (consistent)
        df["signal"] = np.where(df["close"] > df["rolling_mean"], 1, 0)
        signal_rate = float(df["signal"].mean())
        rows_processed = len(df)
        logger.info(
            "Signal generated. rows_processed=%d  signal_rate=%.4f",
            rows_processed, signal_rate,
        )

        latency_ms = int((time.time() - start_ts) * 1000)

        metrics = {
            "version": version,
            "rows_processed": rows_processed,
            "metric": "signal_rate",
            "value": round(signal_rate, 4),
            "latency_ms": latency_ms,
            "seed": seed,
            "status": "success",
        }
        write_metrics(args.output, metrics)
        logger.info("Metrics written to %s.", args.output)
        logger.info("Job completed successfully. status=success")

        print(json.dumps(metrics, indent=2))

    except Exception as exc:
        latency_ms = int((time.time() - start_ts) * 1000)
        logger.error("Job failed: %s", exc, exc_info=True)

        error_metrics = {
            "version": version,
            "status": "error",
            "error_message": str(exc),
            "latency_ms": latency_ms,
        }
        write_metrics(args.output, error_metrics)
        logger.info("Error metrics written to %s.", args.output)

        print(json.dumps(error_metrics, indent=2))
        sys.exit(1)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="MLOps rolling-mean signal pipeline.")
    parser.add_argument("--input",    required=True, help="Path to input CSV file.")
    parser.add_argument("--config",   required=True, help="Path to YAML config file.")
    parser.add_argument("--output",   required=True, help="Path for output metrics JSON.")
    parser.add_argument("--log-file", required=True, help="Path for log file.")
    return parser.parse_args()


if __name__ == "__main__":
    run_pipeline(parse_args())
