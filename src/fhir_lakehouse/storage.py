from __future__ import annotations

import os
from datetime import date
from pathlib import Path


def raw_root(cfg: dict) -> Path:
    return Path(cfg["storage"]["root_path"]) / cfg["storage"]["raw_folder"]


def raw_dir(cfg: dict, resource: str, load_date: date) -> Path:
    return raw_root(cfg) / resource / f"load_date={load_date.isoformat()}"


def raw_file_name(run_id: str, window_start: date, page_number: int, fmt: str) -> str:
    return f"{run_id}_{window_start.isoformat()}_page_{page_number:04d}.{fmt}"


def write_raw(path: Path, body: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(body, encoding="utf-8")
    os.replace(tmp, path)
    return path
