#!/usr/bin/env python3
"""Parse data.yaml and run wget for selected datasets (parallel optional).

Each file is saved with ``wget -O <dest>/<sensible_name>`` (not ``-P`` + URL basename).
Names are inferred (GEO → ``{GSE}_GEO_series_supplement.tar``, Zenodo API → ``zenodo_{id}_{file}``)
or set per dataset as ``download.output_filename`` in YAML.

Rename already-downloaded blobs (no re-fetch): scan manifests under ``storage.root``,
pick the largest archive that passes integrity checks, rename to the same inferred name
as fresh downloads: ``--rename-existing`` (dry-run), then ``--rename-existing --apply``.
Optional ``--delete-partials`` removes other files in that folder after a successful rename.

Intended entrypoints:
  ./download_data.sh …           (checks wget, passes default data.yaml)
  python3 download_data.py /path/to/data.yaml [--list] [-j N] [dataset_id ...]
"""

from __future__ import annotations

import os
import re
import subprocess
import sys
import tarfile
import threading
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, NamedTuple

import yaml

_MANIFEST_NAME = "dataset_manifest.yaml"
_SKIP_ARTIFACT_SUFFIXES = (".part", ".tmp", ".crdownload")
_LABEL_RE = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9_.-]{1,120}$")
_ZENODO_FILE_RE = re.compile(
    r"zenodo\.org/api/records/(\d+)/files/([^/?#]+)/content",
    re.IGNORECASE,
)
_FIGSHARE_SHARE_RE = re.compile(r"figshare\.com/s/([a-zA-Z0-9]+)", re.IGNORECASE)


def _die(msg: str, code: int = 1) -> None:
    print(f"error: {msg}", file=sys.stderr)
    raise SystemExit(code)


class _Cli(NamedTuple):
    """Parsed CLI options for download_data.py."""

    config_path: Path
    list_only: bool
    id_filters: list[str]
    max_workers: int
    rename_existing: bool
    apply_renames: bool
    delete_partials: bool


def _parse_args(argv: list[str], default_yaml: Path) -> _Cli:
    """Parse argv into structured CLI options."""
    path = default_yaml
    list_only = False
    ids: list[str] = []
    max_workers = 1
    rename_existing = False
    apply_renames = False
    delete_partials = False
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
        if a == "--rename-existing":
            rename_existing = True
            i += 1
            continue
        if a == "--apply":
            apply_renames = True
            i += 1
            continue
        if a == "--delete-partials":
            delete_partials = True
            i += 1
            continue
        if a.startswith("-"):
            _die(f"unknown option: {a}")
        ids.append(a)
        i += 1
    return _Cli(path, list_only, ids, max_workers, rename_existing, apply_renames, delete_partials)


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


def _sanitize_filename(name: str) -> str:
    """Keep a single path segment (no dirs, minimal metacharacters)."""
    base = Path(name).name.replace("\x00", "")
    out = re.sub(r"[^\w.\-()+]", "_", base)
    return out if out else "download.bin"


def _infer_output_filename(
    ds_id: str,
    url: str,
    meta: dict[str, Any],
    dl: dict[str, Any],
) -> str:
    """Pick a stable on-disk name; override with download.output_filename in YAML."""
    explicit = dl.get("output_filename")
    if isinstance(explicit, str) and explicit.strip():
        return _sanitize_filename(explicit.strip())

    u = url.lower()
    geo = meta.get("geo_accession")
    if isinstance(geo, str) and geo.strip() and ("ncbi.nlm.nih.gov/geo" in u or "/geo/download/" in u):
        return f"{geo.strip()}_GEO_series_supplement.tar"

    m = _ZENODO_FILE_RE.search(url)
    if m:
        rid, fname = m.group(1), m.group(2)
        return _sanitize_filename(f"zenodo_{rid}_{fname}")

    m2 = re.search(r"zenodo\.org/records/(\d+)", url, re.IGNORECASE)
    if m2 and "zenodo.org" in u:
        return f"zenodo_{m2.group(1)}_download.zip"

    fs = _FIGSHARE_SHARE_RE.search(url)
    if fs:
        return f"figshare_{fs.group(1)}_download.bin"

    return _sanitize_filename(f"{ds_id}_download.bin")


def _strip_wget_output_location_flags(args: list[str]) -> list[str]:
    """Remove -O/-P (and values) so our -O path is authoritative."""
    skip_next = False
    out: list[str] = []
    i = 0
    while i < len(args):
        if skip_next:
            skip_next = False
            i += 1
            continue
        a = args[i]
        if a in ("-O", "--output-document", "-P", "--directory-prefix"):
            skip_next = True
            i += 1
            continue
        if a.startswith("--output-document=") or a.startswith("--directory-prefix="):
            i += 1
            continue
        out.append(a)
        i += 1
    return out


def _registry_download_by_id(cfg_path: Path) -> dict[str, dict[str, Any]]:
    """Load ``datasets[].download`` blocks keyed by dataset id (for output_filename overrides)."""
    with cfg_path.open(encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    if not isinstance(cfg, dict):
        _die("YAML root must be a mapping")
    raw_ds = cfg.get("datasets")
    if not isinstance(raw_ds, list):
        _die("datasets must be a list")
    out: dict[str, dict[str, Any]] = {}
    for item in raw_ds:
        if not isinstance(item, dict):
            continue
        ds_id = item.get("id")
        if not isinstance(ds_id, str) or not ds_id:
            continue
        dl = item.get("download")
        out[ds_id] = dl if isinstance(dl, dict) else {}
    return out


def _looks_like_html(path: Path, sample: int = 4096) -> bool:
    """Return True if the file begins like an HTML error or landing page."""
    try:
        head = path.read_bytes()[:sample].lower()
    except OSError:
        return False
    if not head.strip():
        return False
    return head.lstrip().startswith((b"<!doctype", b"<html", b"<head"))


def _list_artifact_candidates(dest: Path) -> list[Path]:
    """List regular files in ``dest`` that are not the manifest or obvious temp names."""
    if not dest.is_dir():
        return []
    found: list[Path] = []
    for p in dest.iterdir():
        if not p.is_file():
            continue
        if p.name == _MANIFEST_NAME:
            continue
        if p.name.startswith("."):
            continue
        low = p.name.lower()
        if any(low.endswith(sfx) for sfx in _SKIP_ARTIFACT_SUFFIXES):
            continue
        found.append(p)
    found.sort(key=lambda x: x.stat().st_size, reverse=True)
    return found


def _detect_archive_format(path: Path) -> str:
    """Return a coarse format label: ``html``, ``zip``, ``tar``, ``tar_gz``, ``unknown``."""
    if _looks_like_html(path):
        return "html"
    if zipfile.is_zipfile(path):
        return "zip"
    if tarfile.is_tarfile(path):
        head = path.read_bytes()[:4]
        if head[:2] == b"\x1f\x8b":
            return "tar_gz"
        return "tar"
    head2 = path.read_bytes()[:2]
    if head2 == b"\x1f\x8b":
        return "gzip"
    return "unknown"


def _plain_tar_eof_marker_ok(path: Path) -> bool:
    """Return True if the file ends with 1024 zero bytes (POSIX tar end-of-archive).

    GEO/NCBI series tarballs use this trailer; it distinguishes intact archives from
    wget-numbered partials without scanning the whole file.
    """
    try:
        size = path.stat().st_size
        if size < 1024:
            return False
        with path.open("rb") as f:
            f.seek(-1024, 2)
            return f.read(1024) == b"\x00" * 1024
    except OSError:
        return False


def _verify_archive_complete(path: Path, fmt: str) -> bool:
    """Check whether the archive looks fully readable (not a truncated download)."""
    if fmt == "html":
        return False
    if fmt == "zip":
        try:
            with zipfile.ZipFile(path, "r") as zf:
                # Central-directory parse only (fast); catches truncated tail from wget.
                zf.infolist()
            return True
        except zipfile.BadZipFile:
            return False
    if fmt == "tar":
        if _plain_tar_eof_marker_ok(path):
            return True
        r = subprocess.run(
            ["tar", "-tf", str(path)],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )
        return r.returncode == 0
    if fmt == "tar_gz":
        r = subprocess.run(
            ["gzip", "-t", str(path)],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )
        return r.returncode == 0
    if fmt == "gzip":
        r = subprocess.run(
            ["gzip", "-t", str(path)],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )
        return r.returncode == 0
    return False


def _final_output_filename(inferred: str, fmt: str) -> str:
    """Align extension with detected format when ``_infer_output_filename`` was generic."""
    p = Path(inferred)
    stem, suf = p.stem, p.suffix.lower()
    if fmt == "zip" and suf != ".zip":
        return str(p.with_suffix(".zip"))
    if fmt == "tar_gz":
        if suf == ".tar":
            return f"{stem}.tar.gz"
        if not inferred.endswith(".tar.gz"):
            return str(p.with_suffix(".tar.gz"))
    if fmt == "gzip" and suf not in (".gz", ".gzip"):
        return str(p.with_suffix(".gz"))
    if fmt == "tar" and suf not in (".tar", ".tar.gz"):
        return str(p.with_suffix(".tar"))
    return inferred


def _discover_manifest_paths(root: Path, dataset_prefix: str) -> list[Path]:
    """Return paths to ``dataset_manifest.yaml`` under ``root/dataset_prefix/*/``."""
    base = root / dataset_prefix.strip().strip("/")
    if not base.is_dir():
        return []
    return sorted(base.glob("*/" + _MANIFEST_NAME))


def _pick_complete_artifact(candidates: list[Path]) -> tuple[Path | None, str, list[Path]]:
    """Choose the largest candidate that passes integrity checks; return (path, fmt, rest)."""
    for p in candidates:
        fmt = _detect_archive_format(p)
        if fmt == "unknown":
            continue
        if _verify_archive_complete(p, fmt):
            rest = [c for c in candidates if c != p]
            return p, fmt, rest
    return None, "unknown", list(candidates)


def _rewrite_manifest_download(
    manifest_path: Path,
    manifest: dict[str, Any],
    *,
    output_filename: str,
    resolved_file: Path,
    renamed_from: str | None,
) -> None:
    """Update manifest download section and write atomically."""
    dl = manifest.get("download")
    if not isinstance(dl, dict):
        dl = {}
        manifest["download"] = dl
    dl["output_filename"] = output_filename
    dl["resolved_path"] = str(resolved_file)
    if renamed_from:
        prev = dl.get("renamed_from")
        names: list[str]
        if prev is None:
            names = []
        elif isinstance(prev, str):
            names = [prev]
        elif isinstance(prev, list):
            names = [x for x in prev if isinstance(x, str)]
        else:
            names = []
        if renamed_from not in names:
            names.append(renamed_from)
        dl["renamed_from"] = names
    dl["renamed_at"] = datetime.now(timezone.utc).isoformat()
    tmp = manifest_path.with_suffix(".yaml.tmp")
    with tmp.open("w", encoding="utf-8") as mf:
        yaml.safe_dump(
            manifest,
            mf,
            default_flow_style=False,
            allow_unicode=True,
            sort_keys=False,
        )
    tmp.replace(manifest_path)


def run_rename_existing_downloads(
    cfg_path: Path,
    *,
    data_root_override: str | None,
    id_filters: list[str],
    apply_renames: bool,
    delete_partials: bool,
) -> None:
    """Scan saved datasets, verify downloads, rename to inferred names, refresh manifests.

    By default (``apply_renames=False``) only prints the planned actions. Pass
    ``--rename-existing --apply`` to perform renames. Use ``--delete-partials`` to
    remove smaller duplicate/partial files after a successful rename.
    """
    if not cfg_path.is_file():
        _die(f"config file not found: {cfg_path}")

    with cfg_path.open(encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    if not isinstance(cfg, dict):
        _die("YAML root must be a mapping")

    storage = cfg.get("storage") or {}
    root_s = data_root_override if data_root_override else storage.get("root", "/mnt/data")
    if not isinstance(root_s, str):
        _die("storage.root must be a string")
    root = Path(root_s.rstrip("/"))

    dataset_prefix = storage.get("dataset_prefix", "tumor_niche")
    if not isinstance(dataset_prefix, str) or not dataset_prefix.strip():
        _die("storage.dataset_prefix must be a non-empty string")

    want: set[str] | None = set(id_filters) if id_filters else None
    reg_dl = _registry_download_by_id(cfg_path)

    manifests = _discover_manifest_paths(root, dataset_prefix)
    if not manifests:
        print(f"No manifests under {root / dataset_prefix}", file=sys.stderr)
        return

    matched_filter = 0
    for mf_path in manifests:
        with mf_path.open(encoding="utf-8") as f:
            manifest = yaml.safe_load(f)
        if not isinstance(manifest, dict):
            print(f"skip (bad manifest): {mf_path}", file=sys.stderr)
            continue
        ds_id = manifest.get("dataset_id")
        if not isinstance(ds_id, str) or not ds_id:
            print(f"skip (missing dataset_id): {mf_path}", file=sys.stderr)
            continue
        if want is not None and ds_id not in want:
            continue
        matched_filter += 1

        dl_existing = manifest.get("download")
        dl_m: dict[str, Any] = dl_existing if isinstance(dl_existing, dict) else {}
        url = dl_m.get("url")
        if not isinstance(url, str) or not url.strip():
            print(f"[{ds_id}] skip: no download.url in manifest", file=sys.stderr)
            continue

        meta = manifest.get("metadata")
        meta_m: dict[str, Any] = meta if isinstance(meta, dict) else {}

        st = manifest.get("storage")
        if isinstance(st, dict) and isinstance(st.get("resolved_path"), str):
            dest = Path(st["resolved_path"])
        elif isinstance(st, dict) and isinstance(st.get("subdir"), str):
            dest = root / st["subdir"].strip().strip("/")
        else:
            dest = mf_path.parent

        reg = reg_dl.get(ds_id, {})
        inferred = _infer_output_filename(ds_id, url, meta_m, reg)
        candidates = _list_artifact_candidates(dest)
        if not candidates:
            print(f"[{ds_id}] no artifact files in {dest}")
            continue

        best, fmt, rest = _pick_complete_artifact(candidates)
        if best is None:
            print(
                f"[{ds_id}] no complete archive found among {len(candidates)} file(s) "
                f"(still downloading or corrupt). Candidates: {[p.name for p in candidates]}"
            )
            continue

        target_name = _final_output_filename(inferred, fmt)
        target_path = dest / target_name

        if best.resolve() == target_path.resolve():
            print(f"[{ds_id}] already at target name: {target_name}")
            continue

        if target_path.exists() and best.resolve() != target_path.resolve():
            print(
                f"[{ds_id}] skip: target exists {target_path.name!r} "
                f"(remove or merge manually); best source {best.name!r}"
            )
            continue

        action = "rename" if apply_renames else "would rename"
        print(f"[{ds_id}] {action}: {best.name!r} -> {target_name!r} ({fmt}, {best.stat().st_size} bytes)")
        if rest:
            print(f"    other files in folder: {[p.name for p in rest]}")

        if not apply_renames:
            continue

        renamed_from = best.name
        best.rename(target_path)
        _rewrite_manifest_download(
            mf_path,
            manifest,
            output_filename=target_name,
            resolved_file=target_path,
            renamed_from=renamed_from,
        )

        if delete_partials and rest:
            for p in rest:
                try:
                    p.unlink()
                    print(f"    deleted partial/duplicate: {p.name!r}")
                except OSError as exc:
                    print(f"    could not delete {p.name!r}: {exc}", file=sys.stderr)

    if want is not None and matched_filter == 0:
        _die(
            "no dataset_manifest.yaml matched the given id(s) "
            f"(under {root / dataset_prefix}); check dataset id spelling"
        )


@dataclass(frozen=True)
class DownloadJob:
    """One dataset download: destination, URL, wget flags, manifest payload."""

    dataset_id: str
    label: str
    dest: Path
    output_path: Path
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
        wget_args = _strip_wget_output_location_flags(_merge_wget_args(global_wget, extra))

        dest = Path(root.rstrip("/")) / subdir
        out_name = _infer_output_filename(ds_id, url, meta, dl)
        output_path = dest / out_name
        manifest = {
            "schema": "tumor_niche.dataset_manifest/v1",
            "written_at": datetime.now(timezone.utc).isoformat(),
            "dataset_id": ds_id,
            "label": label,
            "metadata": meta,
            "download": {
                "url": url,
                "output_filename": out_name,
                "resolved_path": str(output_path),
            },
            "storage": {
                "root": root,
                "subdir": subdir,
                "resolved_path": str(dest),
            },
            "registry_source": str(cfg_path.resolve()),
        }
        jobs.append(DownloadJob(ds_id, label, dest, output_path, url, wget_args, manifest))

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
        print(f"  -> {job.output_path.name}")
    cmd = ["wget", *job.wget_args, "-O", str(job.output_path), "--", job.url]
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

    cli = _parse_args(user_argv, default_yaml)
    if cli.list_only:
        if cli.id_filters:
            _die("do not pass dataset ids with --list")
        if cli.rename_existing:
            _die("do not combine --list with --rename-existing")
        _list_datasets(cli.config_path)
        return

    if cli.apply_renames and not cli.rename_existing:
        _die("--apply is only valid with --rename-existing")
    if cli.delete_partials and not cli.apply_renames:
        _die("--delete-partials requires --apply (with --rename-existing)")

    data_root_override = os.environ.get("DATA_ROOT")

    if cli.rename_existing:
        run_rename_existing_downloads(
            cli.config_path,
            data_root_override=data_root_override,
            id_filters=cli.id_filters,
            apply_renames=cli.apply_renames,
            delete_partials=cli.delete_partials,
        )
        print()
        print("Done (rename scan).")
        return

    download_jobs = _load_jobs(cli.config_path, cli.id_filters, data_root_override)

    if not download_jobs:
        print("No datasets to download (empty list or all disabled).", file=sys.stderr)
        raise SystemExit(0)

    print(f"Config: {cli.config_path.resolve()}")
    print(f"Parallel workers: {cli.max_workers}")

    if cli.max_workers <= 1:
        for job in download_jobs:
            _execute_download(job)
    else:
        errors: list[tuple[str, Exception]] = []
        with ThreadPoolExecutor(max_workers=cli.max_workers) as pool:
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
