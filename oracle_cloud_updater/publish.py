"""Pluggable publish destinations for the Oracle Cloud F1 data updater.

A *publisher* is the data store the updater both READS (to learn what already
exists, so it can skip unchanged sessions) and WRITES (new/updated session
zips, season schedules, and a refreshed MANIFEST.json).

Two backends are provided; select with the DEST env var:

    DEST=github           -> GithubPublisher (git clone + commit + push)
    DEST=oracle           -> OracleObjectStoragePublisher (S3-compatible bucket)
    DEST=github,oracle     -> publish to BOTH (comma-separated)

Both keep an identical on-store layout so the frontend can read either one:

    sessions/{year}/{round}/{type}.zip
    seasons/{year}/schedule.json        (gzipped JSON)
    MANIFEST.json                       (list of available sessions)

The MANIFEST entry format matches the existing dataset:

    {"year": 2024, "round": 1, "type": "R",
     "name": "01 - Bahrain Grand Prix",
     "zip": "sessions/2024/1/R.zip", "bytes": 17488573}
"""
from __future__ import annotations

import gzip
import json
import logging
import os
import subprocess
import zipfile

logger = logging.getLogger("publish")

# Order used when sorting MANIFEST entries within a round.
_TYPE_ORDER = {"FP1": 0, "FP2": 1, "FP3": 2, "S": 3, "SQ": 4, "Q": 5, "R": 6}


def _manifest_bytes(entries: dict[tuple[int, int, str], dict]) -> bytes:
    sessions = sorted(
        entries.values(),
        key=lambda e: (e["year"], e["round"], _TYPE_ORDER.get(e["type"], 9)),
    )
    return json.dumps({"sessions": sessions}, separators=(",", ":")).encode()


class Publisher:
    """Interface every destination implements."""

    name = "publisher"

    def prepare(self) -> None:
        """One-time setup before any reads/writes (e.g. clone/pull)."""

    def has(self, year: int, rnd: int, stype: str) -> bool:
        raise NotImplementedError

    def put_session(self, year: int, rnd: int, stype: str, name: str, zip_bytes: bytes) -> None:
        raise NotImplementedError

    def put_schedule(self, year: int, gz_bytes: bytes) -> None:
        raise NotImplementedError

    def finalize(self, summary: str) -> None:
        """Flush MANIFEST + commit/upload. Called once at the end of a run."""


# --------------------------------------------------------------------------- #
# GitHub (git clone + push)                                                    #
# --------------------------------------------------------------------------- #
class GithubPublisher(Publisher):
    name = "github"

    def __init__(self) -> None:
        self.repo = os.environ["GITHUB_DATA_REPO"].strip()          # https://github.com/owner/repo.git
        self.token = os.environ.get("GITHUB_TOKEN", "").strip()
        self.branch = os.environ.get("GITHUB_BRANCH", "main").strip()
        self.clone_dir = os.path.abspath(os.environ.get("GITHUB_CLONE_DIR", "./_data_repo"))
        self.author_name = os.environ.get("GIT_AUTHOR_NAME", "f1-oracle-updater")
        self.author_email = os.environ.get("GIT_AUTHOR_EMAIL", "f1-oracle-updater@users.noreply.github.com")
        self.entries: dict[tuple[int, int, str], dict] = {}
        self._dirty = False

    # --- helpers --------------------------------------------------------- #
    def _auth_url(self) -> str:
        if self.token and self.repo.startswith("https://"):
            return self.repo.replace("https://", f"https://x-access-token:{self.token}@", 1)
        return self.repo

    def _git(self, *args: str, check: bool = True) -> subprocess.CompletedProcess:
        cp = subprocess.run(["git", "-C", self.clone_dir, *args],
                            capture_output=True, text=True)
        if check and cp.returncode != 0:
            # never echo the token if it ever leaks into a URL in stderr
            err = cp.stderr.replace(self.token, "<TOKEN>") if self.token else cp.stderr
            raise RuntimeError(f"git {' '.join(args)} failed: {err.strip()}")
        return cp

    # --- interface ------------------------------------------------------- #
    def prepare(self) -> None:
        if not os.path.isdir(os.path.join(self.clone_dir, ".git")):
            logger.info("cloning data repo -> %s", self.clone_dir)
            cp = subprocess.run(["git", "clone", "--depth", "1", "--branch", self.branch,
                                 self._auth_url(), self.clone_dir],
                                capture_output=True, text=True)
            if cp.returncode != 0:
                err = cp.stderr.replace(self.token, "<TOKEN>") if self.token else cp.stderr
                raise RuntimeError(f"git clone failed: {err.strip()}")
        else:
            logger.info("updating existing clone %s", self.clone_dir)
            self._git("remote", "set-url", "origin", self._auth_url())
            self._git("fetch", "--depth", "1", "origin", self.branch)
            self._git("reset", "--hard", f"origin/{self.branch}")
        self._git("config", "user.name", self.author_name)
        self._git("config", "user.email", self.author_email)
        self._load_manifest()

    def _load_manifest(self) -> None:
        mpath = os.path.join(self.clone_dir, "MANIFEST.json")
        self.entries = {}
        if os.path.exists(mpath):
            try:
                for e in json.load(open(mpath, encoding="utf-8")).get("sessions", []):
                    self.entries[(e["year"], e["round"], e["type"])] = e
            except Exception as exc:
                logger.warning("could not parse existing MANIFEST: %s", exc)

    def has(self, year: int, rnd: int, stype: str) -> bool:
        zip_rel = f"sessions/{year}/{rnd}/{stype}.zip"
        return (year, rnd, stype) in self.entries or \
            os.path.exists(os.path.join(self.clone_dir, zip_rel))

    def put_session(self, year: int, rnd: int, stype: str, name: str, zip_bytes: bytes) -> None:
        zip_rel = f"sessions/{year}/{rnd}/{stype}.zip"
        dest = os.path.join(self.clone_dir, zip_rel)
        os.makedirs(os.path.dirname(dest), exist_ok=True)
        with open(dest, "wb") as fh:
            fh.write(zip_bytes)
        self.entries[(year, rnd, stype)] = {
            "year": year, "round": rnd, "type": stype,
            "name": name, "zip": zip_rel, "bytes": len(zip_bytes),
        }
        self._dirty = True

    def put_schedule(self, year: int, gz_bytes: bytes) -> None:
        dest = os.path.join(self.clone_dir, "seasons", str(year), "schedule.json")
        os.makedirs(os.path.dirname(dest), exist_ok=True)
        old = open(dest, "rb").read() if os.path.exists(dest) else None
        if old != gz_bytes:
            with open(dest, "wb") as fh:
                fh.write(gz_bytes)
            self._dirty = True

    def finalize(self, summary: str) -> None:
        with open(os.path.join(self.clone_dir, "MANIFEST.json"), "wb") as fh:
            fh.write(_manifest_bytes(self.entries))
        self._git("add", "-A", "sessions", "seasons", "MANIFEST.json")
        status = self._git("status", "--porcelain").stdout.strip()
        if not status:
            logger.info("[github] nothing changed; skipping commit")
            return
        self._git("commit", "-m", f"Auto-update: {summary}")
        logger.info("[github] pushing to %s (%s)", self.repo, self.branch)
        self._git("push", "origin", f"HEAD:{self.branch}")
        logger.info("[github] pushed")


# --------------------------------------------------------------------------- #
# Oracle Object Storage (S3-compatible)                                        #
# --------------------------------------------------------------------------- #
class OracleObjectStoragePublisher(Publisher):
    name = "oracle"

    def __init__(self) -> None:
        self.endpoint = os.environ["ORACLE_S3_ENDPOINT"].strip()      # https://<ns>.compat.objectstorage.<region>.oraclecloud.com
        self.region = os.environ.get("ORACLE_REGION", "us-ashburn-1").strip()
        self.bucket = os.environ["ORACLE_BUCKET"].strip()
        self.access_key = os.environ["ORACLE_S3_ACCESS_KEY"].strip()
        self.secret_key = os.environ["ORACLE_S3_SECRET_KEY"].strip()
        self.entries: dict[tuple[int, int, str], dict] = {}
        self._client = None

    def _c(self):
        if self._client is None:
            import boto3
            from botocore.config import Config
            self._client = boto3.client(
                "s3",
                endpoint_url=self.endpoint,
                region_name=self.region,
                aws_access_key_id=self.access_key,
                aws_secret_access_key=self.secret_key,
                config=Config(retries={"max_attempts": 3, "mode": "standard"},
                              s3={"addressing_style": "path"}),
            )
        return self._client

    def prepare(self) -> None:
        # Load existing MANIFEST (source of truth for what's already uploaded).
        from botocore.exceptions import ClientError
        self.entries = {}
        try:
            obj = self._c().get_object(Bucket=self.bucket, Key="MANIFEST.json")
            for e in json.loads(obj["Body"].read()).get("sessions", []):
                self.entries[(e["year"], e["round"], e["type"])] = e
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "")
            if code in ("NoSuchKey", "404", "NotFound"):
                logger.info("[oracle] no existing MANIFEST.json; starting fresh")
            else:
                raise

    def has(self, year: int, rnd: int, stype: str) -> bool:
        return (year, rnd, stype) in self.entries

    def put_session(self, year: int, rnd: int, stype: str, name: str, zip_bytes: bytes) -> None:
        key = f"sessions/{year}/{rnd}/{stype}.zip"
        self._c().put_object(Bucket=self.bucket, Key=key, Body=zip_bytes,
                             ContentType="application/zip")
        self.entries[(year, rnd, stype)] = {
            "year": year, "round": rnd, "type": stype,
            "name": name, "zip": key, "bytes": len(zip_bytes),
        }

    def put_schedule(self, year: int, gz_bytes: bytes) -> None:
        self._c().put_object(Bucket=self.bucket, Key=f"seasons/{year}/schedule.json",
                            Body=gz_bytes, ContentType="application/json",
                            ContentEncoding="gzip")

    def finalize(self, summary: str) -> None:
        self._c().put_object(Bucket=self.bucket, Key="MANIFEST.json",
                            Body=_manifest_bytes(self.entries),
                            ContentType="application/json")
        logger.info("[oracle] uploaded MANIFEST.json (%d sessions)", len(self.entries))


# --------------------------------------------------------------------------- #
def build_publishers() -> list[Publisher]:
    dests = [d.strip().lower() for d in os.environ.get("DEST", "github").split(",") if d.strip()]
    factory = {"github": GithubPublisher, "oracle": OracleObjectStoragePublisher}
    pubs: list[Publisher] = []
    for d in dests:
        if d not in factory:
            raise SystemExit(f"Unknown DEST '{d}'. Use github, oracle, or 'github,oracle'.")
        pubs.append(factory[d]())
    if not pubs:
        raise SystemExit("DEST is empty. Set DEST=github | oracle | github,oracle")
    logger.info("publishing to: %s", ", ".join(p.name for p in pubs))
    return pubs


def pack_session_zip(session_dir: str) -> bytes:
    """Bundle a processed session dir into an in-memory STORED zip.

    Files inside are already compressed (zstd/gzip), so STORED just bundles
    them — no wasteful re-compression. Matches scripts/pack_sessions.py.
    """
    import io
    files = [os.path.join(root, f) for root, _, fs in os.walk(session_dir) for f in fs]
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_STORED) as z:
        for f in files:
            z.write(f, os.path.relpath(f, session_dir).replace(os.sep, "/"))
    return buf.getvalue()
