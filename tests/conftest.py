"""Fixtures compartilhadas.

Testes marcados com `spark` precisam de uma SparkSession:
  - MKT_TEST_SPARK=connect  -> Databricks Connect (serverless do seu workspace; uso local)
  - padrao                  -> PySpark local (CI; exige Java e `pip install -e .[spark-local]`)
Sem nenhum dos dois, os testes de Spark sao pulados (nunca quebram o ambiente).
"""

from __future__ import annotations

import os

import pytest


@pytest.fixture(scope="session")
def spark():
    modo = os.getenv("MKT_TEST_SPARK", "local")
    try:
        if modo == "connect":
            from databricks.connect import DatabricksSession

            return DatabricksSession.builder.serverless(True).getOrCreate()
        from pyspark.sql import SparkSession

        return (
            SparkSession.builder.master("local[1]")
            .appName("mkt-pipeline-tests")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"SparkSession indisponivel ({modo}): {exc}")
