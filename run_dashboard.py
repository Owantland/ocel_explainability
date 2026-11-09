#!/usr/bin/env python3
"""Set up and start the explainability dashboard from a fresh clone, in one command:

    python3 run_dashboard.py

On first run it
  1. creates a virtual environment in .venv/ (Python 3.11) and installs
     requirements-dashboard.txt into it,
  2. downloads the trained models and test-set graph caches the dashboard reads (one archive
     from this repo's GitHub Releases, listed in dashboard/data_manifest.json) and unpacks them
     under files/,
  3. starts the dashboard (Streamlit) at http://localhost:8501.
Later runs skip whatever is already in place and start in seconds. Uses only the standard
library, so it runs before anything is installed.

Options:
  --check         set everything up and verify it (checksums, imports), but don't start
  --data-only     only download/verify the data (for a self-managed environment)
  --skip-install  don't create or update .venv; run with the current interpreter
  anything after --  is passed to `streamlit run`, e.g.  -- --server.port 8502
"""
import argparse
import hashlib
import json
import os
import shutil
import subprocess
import sys
import tarfile
import tempfile
import urllib.request

REPO = os.path.dirname(os.path.abspath(__file__))
VENV = os.path.join(REPO, ".venv")
REQUIREMENTS = os.path.join(REPO, "requirements-dashboard.txt")
MANIFEST = os.path.join(REPO, "dashboard", "data_manifest.json")
APP = os.path.join("dashboard", "app.py")
PY_VERSION = (3, 11)  # the version every pin in requirements-dashboard.txt was resolved on


def say(msg):
    print(f"[run_dashboard] {msg}", flush=True)


def fail(msg):
    print(f"[run_dashboard] ERROR: {msg}", file=sys.stderr, flush=True)
    sys.exit(1)


def sha256(path):
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        for block in iter(lambda: f.read(1 << 20), b''):
            h.update(block)
    return h.hexdigest()


# ---------------------------------------------------------------------------
# 1. Python environment
# ---------------------------------------------------------------------------

def venv_python():
    sub = ("Scripts", "python.exe") if os.name == "nt" else ("bin", "python")
    return os.path.join(VENV, *sub)


def python_version(exe):
    out = subprocess.run([exe, "-c", "import sys; print(*sys.version_info[:2])"],
                         capture_output=True, text=True)
    return tuple(int(x) for x in out.stdout.split()) if out.returncode == 0 else None


def find_base_python():
    """A Python 3.11 interpreter to build .venv with: this one, or python3.11 on PATH."""
    if sys.version_info[:2] == PY_VERSION:
        return sys.executable
    for name in ("python3.11", "python3", "python", "py"):
        exe = shutil.which(name)
        if exe and python_version(exe) == PY_VERSION:
            return exe
    fail(f"Python {PY_VERSION[0]}.{PY_VERSION[1]} is required (found "
         f"{sys.version_info[0]}.{sys.version_info[1]}). Install it from python.org or with "
         f"your package manager (e.g. `brew install python@3.11`), then rerun this script with "
         f"it: python3.11 run_dashboard.py")


def ensure_venv():
    """Create .venv if needed and install the pinned requirements; returns its python."""
    py = venv_python()
    if os.path.exists(py):
        if python_version(py) != PY_VERSION:
            fail(f"{VENV} uses Python {python_version(py)}, but {PY_VERSION[0]}.{PY_VERSION[1]} "
                 f"is required. Delete .venv and rerun.")
    else:
        base = find_base_python()
        say(f"Creating virtual environment in .venv (Python {PY_VERSION[0]}.{PY_VERSION[1]}) ...")
        subprocess.run([base, "-m", "venv", VENV], check=True)

    stamp = os.path.join(VENV, ".dashboard-requirements.sha256")
    wanted = sha256(REQUIREMENTS)
    if os.path.exists(stamp) and open(stamp).read().strip() == wanted:
        return py
    say("Installing dependencies into .venv (first run only; PyTorch is large, this can take "
        "several minutes) ...")
    subprocess.run([py, "-m", "pip", "install", "--upgrade", "pip"], check=True)
    subprocess.run([py, "-m", "pip", "install", "-r", REQUIREMENTS], check=True)
    with open(stamp, "w") as f:
        f.write(wanted + "\n")
    return py


# ---------------------------------------------------------------------------
# 2. Data
# ---------------------------------------------------------------------------

def load_manifest():
    with open(MANIFEST) as f:
        return json.load(f)


def missing_files(manifest, verify_hashes=False):
    missing = []
    for entry in manifest['files']:
        path = os.path.join(REPO, entry['path'])
        if (not os.path.isfile(path) or os.path.getsize(path) != entry['size']
                or (verify_hashes and sha256(path) != entry['sha256'])):
            missing.append(entry)
    return missing


def _progress(done, total):
    if total:
        pct = 100 * done / total
        sys.stdout.write(f"\r[run_dashboard]   {done / 1e6:7.0f} / {total / 1e6:.0f} MB ({pct:5.1f}%)")
    else:
        sys.stdout.write(f"\r[run_dashboard]   {done / 1e6:7.0f} MB")
    sys.stdout.flush()


def download(url, dest, expected_size):
    req = urllib.request.Request(url, headers={"User-Agent": "ocel-explainability-dashboard-setup"})
    with urllib.request.urlopen(req) as resp, open(dest, "wb") as out:
        total = int(resp.headers.get("Content-Length") or expected_size or 0)
        done = 0
        while True:
            block = resp.read(1 << 20)
            if not block:
                break
            out.write(block)
            done += len(block)
            _progress(done, total)
    print()


def ensure_data(verify_hashes=False):
    manifest = load_manifest()
    todo = missing_files(manifest, verify_hashes)
    if not todo:
        say(f"Data in place ({len(manifest['files'])} files under files/).")
        return
    say(f"{len(todo)} of {len(manifest['files'])} data files missing or changed; downloading "
        f"the data archive ({manifest['size'] / 1e6:.0f} MB) from {manifest['url']} ...")
    wanted = {e['path']: e for e in todo}
    with tempfile.TemporaryDirectory(dir=REPO) as tmp:
        archive = os.path.join(tmp, manifest['asset'])
        try:
            download(manifest['url'], archive, manifest['size'])
        except OSError as ex:
            fail(f"download failed ({ex}). Check your connection and rerun; the archive can "
                 f"also be fetched by hand from {manifest['url']} -- see README.md.")
        say("Verifying checksum ...")
        if sha256(archive) != manifest['sha256']:
            fail("the downloaded archive's checksum doesn't match dashboard/data_manifest.json "
                 "(incomplete download?). Rerun to try again.")
        say("Unpacking ...")
        with tarfile.open(archive) as tar:
            for member in tar.getmembers():
                # Only the manifest's own files, only regular files, never outside the repo.
                if member.name not in wanted or not member.isfile():
                    continue
                target = os.path.realpath(os.path.join(REPO, member.name))
                if not target.startswith(os.path.realpath(REPO) + os.sep):
                    fail(f"refusing to unpack {member.name!r} outside the repository")
                try:
                    tar.extract(member, REPO, filter="data")
                except TypeError:  # Python without extraction filters (< 3.10.12 / 3.11.4)
                    tar.extract(member, REPO)
    still = missing_files(manifest, verify_hashes=True)
    if still:
        fail(f"{len(still)} files still missing or corrupt after unpacking, e.g. {still[0]['path']}")
    say("Data ready.")


# ---------------------------------------------------------------------------
# 3. Launch
# ---------------------------------------------------------------------------

def check_imports(py):
    env = dict(os.environ, PYTHONPATH=REPO)
    code = "import streamlit, torch, torch_geometric, captum; import dashboard.kpis; print(torch.__version__)"
    out = subprocess.run([py, "-c", code], cwd=REPO, env=env, capture_output=True, text=True)
    if out.returncode != 0:
        fail(f"the dashboard's imports failed:\n{out.stderr[-2000:]}")
    say(f"Imports OK (torch {out.stdout.strip()}).")


def main():
    argv = sys.argv[1:]
    streamlit_args = []
    if "--" in argv:
        i = argv.index("--")
        argv, streamlit_args = argv[:i], argv[i + 1:]
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0],
                                 formatter_class=argparse.RawDescriptionHelpFormatter,
                                 epilog="Anything after -- is passed to `streamlit run`.")
    ap.add_argument("--check", action="store_true", help="set up and verify, don't start")
    ap.add_argument("--data-only", action="store_true", help="only download/verify the data")
    ap.add_argument("--skip-install", action="store_true",
                    help="don't create/update .venv; use the current interpreter")
    args = ap.parse_args(argv)
    os.chdir(REPO)

    if args.data_only:
        ensure_data(verify_hashes=True)
        return
    py = sys.executable if args.skip_install else ensure_venv()
    ensure_data(verify_hashes=args.check)
    if args.check:
        check_imports(py)
        say("Everything is set up. Start the dashboard with: python3 run_dashboard.py")
        return

    port = "8501"
    for i, a in enumerate(streamlit_args):
        if a.startswith("--server.port="):
            port = a.split("=", 1)[1]
        elif a == "--server.port" and i + 1 < len(streamlit_args):
            port = streamlit_args[i + 1]
    say(f"Starting the dashboard at http://localhost:{port} (Ctrl+C to stop) ...")
    env = dict(os.environ, PYTHONPATH=REPO + os.pathsep + os.environ.get("PYTHONPATH", ""))
    cmd = [py, "-m", "streamlit", "run", APP, "--browser.gatherUsageStats", "false",
           *streamlit_args]
    try:
        sys.exit(subprocess.call(cmd, cwd=REPO, env=env))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
