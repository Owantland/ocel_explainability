"""Maintainer-only: package the data the dashboard needs into one downloadable archive.

A fresh clone has none of files/ (gitignored: checkpoints, graph caches, SQLite DBs, event-log
CSVs, precomputed aggregates). run_dashboard.py downloads this archive from a GitHub Release
and unpacks it at the repo root. This script builds it:

  1. Trace. Runs dashboard/app.py headless (Streamlit AppTest) for every KPI -- single-trace
     explanation, aggregate tab and the curated counterfactual -- with an audit hook recording
     every file it reads under files/. The union is the bundle's file list, so nothing is
     guessed. Tracked files (files/config.yml, files/model_parameters.csv) are left out: the
     clone already has them.
  2. Pack. Writes dist/<asset> (tar.gz, repo-relative paths) and dashboard/data_manifest.json
     (asset URL, sha256, per-file sizes and sha256s), which the launcher checks against.

The dashboard loads graph caches test-only (Modelling(test_only=True)), so each cache needs a
feature_stats.pt beside it; --write-stats regenerates those from a full load first.

Usage (from the repo root, with every KPI's caches in place -- see dashboard/kpis.py):
    PYTHONPATH=. python3 scripts/build_dashboard_bundle.py [--write-stats] [--trace-only]
Then upload dist/<asset> to the release named in the manifest:
    gh release create dashboard-data-v1 dist/ocel_dashboard_data_v1.tar.gz ...
"""
import argparse
import hashlib
import json
import os
import subprocess
import sys
import tarfile

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RELEASE_TAG = "dashboard-data-v1"
ASSET = "ocel_dashboard_data_v1.tar.gz"
ASSET_URL = f"https://github.com/Owantland/ocel_explainability/releases/download/{RELEASE_TAG}/{ASSET}"
MANIFEST = os.path.join(REPO, "dashboard", "data_manifest.json")
DIST = os.path.join(REPO, "dist")


def sha256(path):
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        for block in iter(lambda: f.read(1 << 20), b''):
            h.update(block)
    return h.hexdigest()


def tracked_files():
    out = subprocess.run(["git", "ls-files", "files"], cwd=REPO, capture_output=True,
                         text=True, check=True).stdout
    return set(out.split())


def trace_reads():
    """Repo-relative paths of every existing file under files/ the dashboard reads."""
    files_root = os.path.join(REPO, "files") + os.sep
    seen = set()

    def hook(event, args):
        if event == 'open':
            path, mode = args[0], args[1]
            if not isinstance(path, (str, bytes, os.PathLike)) or (mode and any(c in str(mode) for c in 'wax+')):
                return
        elif event == 'sqlite3.connect':
            path = args[0]
        else:
            return
        try:
            p = os.path.realpath(os.fsdecode(path))
        except (TypeError, ValueError):
            return
        if p.startswith(files_root) and os.path.isfile(p):
            seen.add(os.path.relpath(p, REPO))

    import tempfile
    import time

    from streamlit.testing.v1 import AppTest
    from dashboard import cache as dc
    from dashboard import kpis

    # A cache hit would skip the explain_* calls and hide what they read: point the
    # single-trace result cache at an empty scratch dir for the trace.
    dc.CACHE_ROOT = tempfile.mkdtemp(prefix="dashboard_cache_trace_")
    started = time.time()
    sys.addaudithook(hook)

    for slug in kpis.KPIS:
        print(f"[trace] {slug}", flush=True)
        at = AppTest.from_file(os.path.join(REPO, "dashboard", "app.py"), default_timeout=1800)
        at.run()
        at.sidebar.selectbox[0].set_value(slug).run()
        _raise_if_failed(at, slug, "load")  # also renders the Aggregate tab
        _button(at, "Explain this trace").click().run()
        _raise_if_failed(at, slug, "single trace")
        at.sidebar.radio[0].set_value('cf').run()
        _button(at, "Load curated example", sidebar=True).click().run()
        _button(at, "Explain this trace").click().run()
        _raise_if_failed(at, slug, "counterfactual")
    # Drop anything the run itself wrote and read back (its own output PNGs/CSVs).
    return {p for p in seen if os.path.getmtime(os.path.join(REPO, p)) < started}


def _button(at, label, sidebar=False):
    for b in (at.sidebar.button if sidebar else at.button):
        if b.label == label:
            return b
    raise RuntimeError(f"no button labelled {label!r}")


def _raise_if_failed(at, slug, step):
    if at.exception:
        raise RuntimeError(f"{slug} / {step}: {at.exception[0].message}")
    errors = [e.value for e in at.error]
    if errors:
        raise RuntimeError(f"{slug} / {step}: st.error shown: {errors}")


def write_stats():
    from dashboard import kpis
    for slug in kpis.KPIS:
        e = kpis.make_explainer(slug, test_only=False)
        print(f"[stats] {slug} -> {e.save_feature_stats()}", flush=True)
        del e


def pack(paths):
    os.makedirs(DIST, exist_ok=True)
    out = os.path.join(DIST, ASSET)
    entries = []
    with tarfile.open(out, "w:gz", compresslevel=6) as tar:
        for rel in sorted(paths):
            full = os.path.join(REPO, rel)
            entries.append({'path': rel, 'size': os.path.getsize(full), 'sha256': sha256(full)})
            tar.add(full, arcname=rel, recursive=False)
            print(f"[pack] {rel} ({entries[-1]['size'] / 1e6:.1f} MB)", flush=True)
    manifest = {
        'release_tag': RELEASE_TAG,
        'url': ASSET_URL,
        'asset': ASSET,
        'sha256': sha256(out),
        'size': os.path.getsize(out),
        'files': entries,
    }
    with open(MANIFEST, "w") as f:
        json.dump(manifest, f, indent=1)
        f.write("\n")
    print(f"[pack] {out}: {manifest['size'] / 1e6:.0f} MB archive, "
          f"{sum(e['size'] for e in entries) / 1e6:.0f} MB unpacked, {len(entries)} files")
    print(f"[pack] manifest -> {os.path.relpath(MANIFEST, REPO)}")


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--write-stats", action="store_true",
                    help="regenerate each cache's feature_stats.pt from a full load first")
    ap.add_argument("--trace-only", action="store_true",
                    help="print the traced file list and stop (no archive, no manifest)")
    args = ap.parse_args()
    os.chdir(REPO)
    if args.write_stats:
        write_stats()
    paths = sorted(trace_reads() - tracked_files())
    if args.trace_only:
        print("\n".join(paths))
        return
    pack(paths)


if __name__ == "__main__":
    main()
