# Databricks notebook source
import os
import sys
import uuid
from datetime import date

repo_root = os.path.abspath(os.path.join(os.getcwd(), ".."))
if not os.path.exists(os.path.join(repo_root, "src")):
    repo_root = os.path.abspath(os.path.join(repo_root, ".."))
sys.path.insert(0, os.path.join(repo_root, "src"))

from fhir_lakehouse.config import enabled_resources, load_config

dbutils.widgets.text("run_id", "")
dbutils.widgets.text("run_date", "")
dbutils.widgets.text("lookback_days", "")
dbutils.widgets.text("resource", "")

overrides = {}
if dbutils.widgets.get("lookback_days"):
    overrides["ingestion"] = {"lookback_days": int(dbutils.widgets.get("lookback_days"))}

cfg = load_config(os.path.join(repo_root, "conf", "pipeline.yaml"), overrides)
run_id = dbutils.widgets.get("run_id") or uuid.uuid4().hex[:12]
run_date = date.fromisoformat(dbutils.widgets.get("run_date")) if dbutils.widgets.get("run_date") else date.today()
selected = [r.strip() for r in dbutils.widgets.get("resource").split(",") if r.strip()]
resources = selected or enabled_resources(cfg)
print(f"run_id={run_id} run_date={run_date} resources={resources}")
