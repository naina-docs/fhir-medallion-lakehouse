# Databricks notebook source
# MAGIC %run ../_bootstrap

# COMMAND ----------

from fhir_lakehouse.config import schema_name
from fhir_lakehouse.control import create_control_objects

catalog = cfg["catalog"]["catalog"]
volume_schema = f"{catalog}.{cfg['storage']['volume_schema']}"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {volume_schema}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {volume_schema}.{cfg['storage']['volume_name']}")
create_control_objects(spark, cfg)

# COMMAND ----------

display(spark.sql(f"SHOW TABLES IN {schema_name(cfg, 'control')}"))
