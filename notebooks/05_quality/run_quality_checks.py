# Databricks notebook source
# MAGIC %run ../_bootstrap

# COMMAND ----------

from fhir_lakehouse.control import run_step
from fhir_lakehouse.quality import DQ_SCHEMA, run_checks

with run_step(spark, cfg, run_id, "quality_checks") as step:
    results = run_checks(spark, cfg, run_id)
    step["rows"] = len(results)
    step["details"] = {"failed": [r["check_name"] for r in results if not r["passed"]]}

# COMMAND ----------

display(spark.createDataFrame(results, DQ_SCHEMA))
