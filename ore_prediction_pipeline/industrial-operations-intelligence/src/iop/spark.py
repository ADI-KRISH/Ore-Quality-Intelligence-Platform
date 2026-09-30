"""One SparkSession builder for both run modes (spec 5.1). Local mode builds a
Delta-enabled session; Databricks mode reuses the notebook's active session
(Delta support is already built in there) so stage code never branches on mode.

Local mode runs inside the Docker Compose `app` service (Linux), not on the
bare host - PySpark's Windows support needs a winutils.exe binary with no
trustworthy first-party source, so we sidestep it entirely."""

from __future__ import annotations

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from iop.config import Config


def get_spark(cfg: Config) -> SparkSession:
    if not cfg.is_local:
        # On Databricks a session already exists in the job/notebook context.
        return SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()

    # spark.sql.warehouse.dir is deliberately NOT set: it's only the default
    # location for managed tables, and every stage writes to an explicit
    # Delta path from config instead.
    builder = (
        SparkSession.builder.appName("iop-local")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", "4")
        .master("local[*]")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()
