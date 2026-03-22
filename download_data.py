#!/usr/bin/env python3
"""Parse data.yaml and run wget for selected datasets (parallel optional).

Intended entrypoints:
  ./download_data.sh …           (checks wget, passes default data.yaml)
  python3 download_data.py /path/to/data.yaml [--list] [-j N] [dataset_id ...]
"""

from __future__ import annotations

import os
import re
import subprocess
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml

_MANIFEST_NAME = "dataset_manifest.yaml"
_LABEL_RE = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9_.-]{1,120}$")


def _die(msg: str, code: int = 1) -> None:
    print(f"error: {msg}", file=sys.stderr)
    raise SystemExit(code)


def _parse_args(argv: list[str], default_yaml: Path) -> tuple[Path, bool, list[str], int]:
    """Return (config_path, list_only, id_filters, max_parallel_workers)."""
    path = default_yaml
    list_only = False
    ids: list[str] = []
    max_workers = 1
    raw_jobs = os.environ.get("DOWNLOAD_JOBS", "1")
    try:
        max_workers = int(raw_jobs)
    except ValueError:
        _die(f"DOWNLOAD_JOBS must be a positive integer, got {raw_jobs!r}")
    if max_workers < 1:
        _die("DOWNLOAD_JOBS must be at least 1")

    i = 0
    while i < len(argv):
        a = argv[i]
        if a == "--config":
            if i + 1 >= len(argv):
                _die("--config requires a file path")
            path = Path(argv[i + 1])
            i += 2
            continue
        if a in ("--jobs", "-j"):
            if i + 1 >= len(argv):
                _die(f"{a} requires a positive integer")
            try:
                max_workers = int(argv[i + 1])
            except ValueError:
                _die(f"{a} must be a positive integer")
            if max_workers < 1:
                _die(f"{a} must be at least 1")
            i += 2
            continue
        if a == "--list":
            list_only = True
            i += 1
            continue
        if a.startswith("-"):
            _die(f"unknown option: {a}")
        ids.append(a)
        i += 1
    return path, list_only, ids, max_workers


def _as_str_list(x: Any, ctx: str) -> list[str]:
    if x is None:
        return []
    if not isinstance(x, list) or not all(isinstance(t, str) for t in x):
        _die(f"{ctx} must be a list of strings")
    return list(x)


def _merge_wget_args(defaults: list[str], override: list[str]) -> list[str]:
    """Per-dataset list replaces global default when non-empty; else use default."""
    return override if override else defaults


def _validate_label(raw: str, ds_id: str) -> str:
    """Ensure label is safe for directory names and unambiguous in manifests."""
    s = raw.strip()
    if not _LABEL_RE.fullmatch(s):
        _die(
            f"datasets[{ds_id}].label must match {_LABEL_RE.pattern!r} "
            f"(start with alphanumeric, 2–121 chars, allowed: ._-)"
        )
    return s


def _resolve_subdir(
    *,
    ds_id: str,
    label: str,
    st: dict[str, Any],
    meta: dict[str, Any],
    dataset_prefix: str,
) -> str:
    """Return path relative to storage root."""
    explicit = st.get("subdir")
    if explicit is not None:
        if not isinstance(explicit, str) or not explicit.strip():
            _die(f"datasets[{ds_id}].storage.subdir must be a non-empty string when set")
        return explicit.strip().strip("/")

    geo = meta.get("geo_accession")
    suffix: str
    if isinstance(geo, str) and geo.strip():
        suffix = geo.strip()
    else:
        suffix = ds_id
    return f"{dataset_prefix.strip().strip('/')}/{label}__{suffix}"


@dataclass(frozen=True)
class DownloadJob:
    """One dataset download: destination, URL, wget flags, manifest payload."""

    dataset_id: str
    label: str
    dest: Path
    url: str
    wget_args: list[str]
    manifest: dict[str, Any]


def _load_jobs(
    cfg_path: Path,
    id_filters: list[str],
    data_root_override: str | None,
) -> list[DownloadJob]:
    """Build download jobs and manifest metadata for each selected dataset."""
    if not cfg_path.is_file():
        _die(f"config file not found: {cfg_path}")

    with cfg_path.open(encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    if not isinstance(cfg, dict):
        _die("YAML root must be a mapping")

    storage = cfg.get("storage") or {}
    root = data_root_override if data_root_override else storage.get("root", "/mnt/data")
    if not isinstance(root, str):
        _die("storage.root must be a string")

    dataset_prefix = storage.get("dataset_prefix", "tumor_niche")
    if not isinstance(dataset_prefix, str) or not dataset_prefix.strip():
        _die("storage.dataset_prefix must be a non-empty string")

    defaults = cfg.get("defaults") or {}
    default_dl = defaults.get("download") or {}
    global_wget = _as_str_list(default_dl.get("wget_extra_args"), "defaults.download.wget_extra_args")

    raw_ds = cfg.get("datasets")
    if raw_ds is None:
        _die("missing top-level key: datasets (list of dataset entries)")
    if not isinstance(raw_ds, list):
        _die("datasets must be a list")

    want: set[str] | None = set(id_filters) if id_filters else None
    jobs: list[DownloadJob] = []

    for idx, item in enumerate(raw_ds):
        if not isinstance(item, dict):
            _die(f"datasets[{idx}] must be a mapping")
        ds_id = item.get("id")
        if not ds_id or not isinstance(ds_id, str):
            _die(f"datasets[{idx}].id must be a non-empty string")

        if want is not None and ds_id not in want:
            continue

        enabled = item.get("enabled", True)
        if not isinstance(enabled, bool):
            _die(f"datasets[{ds_id}].enabled must be a boolean")
        if not enabled and want is None:
            continue

        raw_label = item.get("label")
        if not raw_label or not isinstance(raw_label, str):
            _die(f"datasets[{ds_id}].label is required (short human-readable name for paths/manifest)")
        label = _validate_label(raw_label, ds_id)

        raw_meta = item.get("metadata")
        if raw_meta is None:
            meta = {}
        elif isinstance(raw_meta, dict):
            meta = raw_meta
        else:
            _die(f"datasets[{ds_id}].metadata must be a mapping")

        dl = item.get("download") or {}
        url = dl.get("url")
        if not url or not isinstance(url, str):
            _die(f"datasets[{ds_id}].download.url must be a non-empty string")

        st = item.get("storage") or {}
        if not isinstance(st, dict):
            _die(f"datasets[{ds_id}].storage must be a mapping")

        subdir = _resolve_subdir(
            ds_id=ds_id,
            label=label,
            st=st,
            meta=meta,
            dataset_prefix=dataset_prefix,
        )

        extra = _as_str_list(dl.get("wget_extra_args"), f"datasets[{ds_id}].download.wget_extra_args")
        wget_args = _merge_wget_args(global_wget, extra)

        dest = Path(root.rstrip("/")) / subdir
        manifest = {
            "schema": "tumor_niche.dataset_manifest/v1",
            "written_at": datetime.now(timezone.utc).isoformat(),
            "dataset_id": ds_id,
            "label": label,
            "metadata": meta,
            "download": {"url": url},
            "storage": {
                "root": root,
                "subdir": subdir,
                "resolved_path": str(dest),
            },
            "registry_source": str(cfg_path.resolve()),
        }
        jobs.append(DownloadJob(ds_id, label, dest, url, wget_args, manifest))

    if want is not None:
        missing = want - {j.dataset_id for j in jobs}
        if missing:
            _die(f"unknown or disabled dataset id(s): {', '.join(sorted(missing))}")

    return jobs


def _list_datasets(cfg_path: Path) -> None:
    with cfg_path.open(encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    if not isinstance(cfg, dict):
        _die("YAML root must be a mapping")
    raw_ds = cfg.get("datasets")
    if not isinstance(raw_ds, list):
        _die("datasets must be a list")
    for item in raw_ds:
        if not isinstance(item, dict):
            continue
        ds_id = item.get("id", "?")
        lbl = item.get("label", "")
        en = item.get("enabled", True)
        print(f"{ds_id}\tenabled={'true' if en else 'false'}\tlabel={lbl}")


_print_lock = threading.Lock()


def _execute_download(job: DownloadJob) -> None:
    """Create dest dir, write manifest, run wget (raises on failure)."""
    dest = job.dest
    dest.mkdir(parents=True, exist_ok=True)
    manifest_path = dest / _MANIFEST_NAME
    with manifest_path.open("w", encoding="utf-8") as mf:
        yaml.safe_dump(
            job.manifest,
            mf,
            default_flow_style=False,
            allow_unicode=True,
            sort_keys=False,
        )
    with _print_lock:
        print()
        print(f"[{job.dataset_id}] label={job.label} -> {dest}")
        print(f"  {job.url}")
    cmd = ["wget", *job.wget_args, "-P", str(dest), "--", job.url]
    subprocess.run(cmd, check=True)


def main() -> None:
    script_dir = Path(__file__).resolve().parent
    # With no yaml argument, options start at argv[1] (e.g. `python3 download_data.py --list`).
    # Wrapper always passes: python3 download_data.py <path/to/data.yaml> [args...]
    rest = sys.argv[1:]
    if rest and not rest[0].startswith("-"):
        default_yaml = Path(rest[0])
        user_argv = rest[1:]
    else:
        default_yaml = script_dir / "data.yaml"
        user_argv = rest

    cfg_path, list_only, id_filters, max_workers = _parse_args(user_argv, default_yaml)
    if list_only:
        if id_filters:
            _die("do not pass dataset ids with --list")
        _list_datasets(cfg_path)
        return

    data_root_override = os.environ.get("DATA_ROOT")
    download_jobs = _load_jobs(cfg_path, id_filters, data_root_override)

    if not download_jobs:
        print("No datasets to download (empty list or all disabled).", file=sys.stderr)
        raise SystemExit(0)

    print(f"Config: {cfg_path.resolve()}")
    print(f"Parallel workers: {max_workers}")

    if max_workers <= 1:
        for job in download_jobs:
            _execute_download(job)
    else:
        errors: list[tuple[str, Exception]] = []
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            future_to_job = {pool.submit(_execute_download, j): j for j in download_jobs}
            for fut in as_completed(future_to_job):
                j = future_to_job[fut]
                try:
                    fut.result()
                except Exception as exc:
                    errors.append((j.dataset_id, exc))
        for ds_id, exc in errors:
            print(f"error: [{ds_id}] {exc}", file=sys.stderr)
        if errors:
            raise SystemExit(1)

    print()
    print("Done.")


if __name__ == "__main__":
    main()
