# Databricks notebook source
# MAGIC %run ../_bootstrap

# COMMAND ----------

from fhir_lakehouse.control import run_step
from fhir_lakehouse.gold import create_gold_views

with run_step(spark, cfg, run_id, "gold_views") as step:
    views = create_gold_views(spark, cfg)
    step["rows"] = len(views)
    step["details"] = {"views": views}

# COMMAND ----------

display(spark.createDataFrame([(v, spark.table(v).count()) for v in views], "view string, rows long"))
