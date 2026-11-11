from __future__ import annotations

import copy
import os
from pathlib import Path
from typing import Any

import yaml

DEFAULT_CONFIG = Path(__file__).resolve().parents[2] / "conf" / "pipeline.yaml"


def _deep_merge(base: dict, override: dict) -> dict:
    merged = copy.deepcopy(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def load_config(path: str | os.PathLike | None = None, overrides: dict | None = None) -> dict[str, Any]:
    cfg_path = Path(path or os.environ.get("FHIR_PIPELINE_CONFIG", DEFAULT_CONFIG))
    with open(cfg_path, encoding="utf-8") as fh:
        cfg = yaml.safe_load(fh)

    env = os.environ.get("FHIR_PIPELINE_ENV", cfg.get("project", {}).get("environment"))
    env_file = cfg_path.with_name(f"pipeline.{env}.yaml")
    if env and env_file.exists():
        with open(env_file, encoding="utf-8") as fh:
            cfg = _deep_merge(cfg, yaml.safe_load(fh) or {})

    if overrides:
        cfg = _deep_merge(cfg, overrides)
    return cfg


def table_name(cfg: dict, layer: str, name: str) -> str:
    catalog = cfg["catalog"]["catalog"]
    schema = cfg["catalog"]["schemas"][layer]
    return f"{catalog}.{schema}.{name}"


def enabled_resources(cfg: dict) -> list[str]:
    return [r["name"] for r in cfg["resources"] if r.get("enabled", True)]
