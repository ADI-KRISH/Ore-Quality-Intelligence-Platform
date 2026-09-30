"""iop run --stage <name> --config <path>. Same entrypoint locally and,
via a Databricks Job task per stage, in the cloud (spec 12.1)."""

from __future__ import annotations

import argparse
import sys

from iop import reports
from iop.config import load_config
from iop.ingest import bronze
from iop.ml import score, train
from iop.pipeline_log import log_run
from iop.quality import runner as dq_runner
from iop.serving import export
from iop.spark import get_spark
from iop.transform import gold, silver

STAGE_REGISTRY = {
    "ingest_bronze": bronze.run,
    "build_silver": silver.run,
    "dq_checks": dq_runner.run,
    "build_gold": gold.run,
    "train_or_load_model": train.run,
    "score_predictions": score.run,
    "export_serving": export.run,
    "refresh_reports": reports.run,
}

# Not STAGE_REGISTRY's key order: spec 12.1's diagram lists dq_checks before
# build_gold, but this dq_checks reads gold.fact_dq_results (DQ06/08/09,
# computed in build_gold) as the pipeline's final gate, so it must run after.
PIPELINE_ORDER = [
    "ingest_bronze",
    "build_silver",
    "build_gold",
    "dq_checks",
    "train_or_load_model",
    "score_predictions",
    "export_serving",
    "refresh_reports",
]


def run_stage(stage: str, config_path: str) -> int:
    cfg = load_config(config_path)
    spark = get_spark(cfg)
    stages = PIPELINE_ORDER if stage == "all" else [stage]

    for name in stages:
        fn = STAGE_REGISTRY[name]
        result = fn(spark, cfg)
        print(
            f"[{result.status.upper()}] {result.stage}: "
            f"{result.rows_in} in -> {result.rows_out} out "
            f"({result.duration_seconds:.2f}s)"
        )
        log_run(spark, cfg, result)
        if result.status == "failed":
            return 1
    return 0


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="iop")
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_parser = subparsers.add_parser("run", help="Run one or all pipeline stages")
    run_parser.add_argument(
        "--stage",
        required=True,
        choices=[*PIPELINE_ORDER, "all"],
        help="Stage to run, or 'all' to run the full pipeline in order",
    )
    run_parser.add_argument(
        "--config", required=True, help="Path to a config yaml (config/local.yaml, ...)"
    )

    args = parser.parse_args(argv)

    if args.command == "run":
        sys.exit(run_stage(args.stage, args.config))


if __name__ == "__main__":
    main()
