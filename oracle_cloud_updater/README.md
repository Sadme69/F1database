# Oracle Cloud F1 Data Updater

An hourly job that keeps the F1 telemetry dataset in sync with FastF1. Each run
compares what FastF1 has against what's already in your store, then **downloads
→ bakes telemetry → compresses → zips → publishes** only what's new or recently
updated.

It's the Oracle-Cloud replacement for the old GitHub Actions updater
(`github_data/builder/`), so you're no longer limited by Actions minutes or the
per-run cap.

## What it does each run

1. Pull every season's schedule from FastF1, write `seasons/{year}/schedule.json`.
2. For each **available** session (`R`, `Q`, `S`, `SQ`, `FP1`, `FP2`, `FP3`):
   process it **only if** a destination is missing it, or it finished within
   the last `REFRESH_DAYS` days (to pick up late data corrections).
3. Pack each processed session into `sessions/{year}/{round}/{type}.zip`
   (`ZIP_STORED` — contents are already zstd/gzip-compressed).
4. Refresh `MANIFEST.json` and commit/upload.

## Destinations (pick one or both)

Set `DEST` in `.env`:

| `DEST`          | Where data goes                                              |
|-----------------|--------------------------------------------------------------|
| `github`        | `git push` to your data repo (frontend/jsDelivr unchanged)   |
| `oracle`        | Oracle Object Storage bucket (S3-compatible)                 |
| `github,oracle` | both, in one run                                             |

Both write the **same layout** (`sessions/…`, `seasons/…`, `MANIFEST.json`), so
the frontend can read from either.

## Layout

```
oracle_cloud_updater/
├── updater.py        # the hourly job (orchestration)
├── publish.py        # destination backends: GitHub push + Oracle Object Storage
├── requirements.txt  # updater extras (backend deps installed separately)
├── .env.example      # all config — copy to .env
├── setup.sh          # one-shot VM provisioning (Ubuntu / Ampere A1)
├── systemd/
│   └── f1-updater.timer
└── _work/ _fastf1cache/ _data_repo/   # runtime (gitignored)
```

It reuses the app's `backend/services` (FastF1 download + telemetry baking +
storage), so it must live inside a checkout of the app repo with the backend
deps installed — `setup.sh` handles that.

---

## Oracle Cloud Free Tier setup (Ampere A1 / Ubuntu 22.04)

### 1. Create the instance
- OCI Console → **Compute → Instances → Create**.
- Shape: **VM.Standard.A1.Flex** (Always Free eligible — e.g. 2 OCPU / 12 GB,
  or up to 4 OCPU / 24 GB).
- Image: **Canonical Ubuntu 22.04**.
- Add your SSH public key. No inbound ports needed (the job is outbound-only).

### 2. SSH in and grab the code
```bash
ssh ubuntu@<your-instance-ip>
sudo apt-get update -y && sudo apt-get install -y git
git clone https://github.com/Sadme69/f1replaytiming.git
cd f1replaytiming/oracle_cloud_updater
```

### 3. Provision
```bash
chmod +x setup.sh
./setup.sh
```
This creates a venv, installs `backend/requirements.txt` + the updater extras,
writes `.env`, and installs+enables the hourly `systemd` timer.

### 4. Configure
```bash
nano .env
```
- **GitHub:** set `GITHUB_TOKEN` to a fine-grained PAT with **Contents: Read &
  Write** on the data repo, and confirm `GITHUB_DATA_REPO` / `GITHUB_BRANCH`.
- **Oracle Object Storage:** create a bucket, then under
  *Identity → Users → your user → Customer Secret Keys* generate a key and set
  `ORACLE_S3_ACCESS_KEY` / `ORACLE_S3_SECRET_KEY`. Set `ORACLE_S3_ENDPOINT` to
  `https://<namespace>.compat.objectstorage.<region>.oraclecloud.com`
  (namespace is on the bucket details page), plus `ORACLE_REGION` /
  `ORACLE_BUCKET`. Make the bucket **Public** (read) if the frontend fetches
  zips directly.

### 5. Test one run, then let the timer drive it
```bash
.venv/bin/python updater.py          # foreground, watch the logs
systemctl list-timers f1-updater.timer
journalctl -u f1-updater.service -f  # follow scheduled runs
```

The **first** run is a full backfill of everything missing — that can take a
while and use a few GB of disk under `_work/` (cleaned up at the end). Every run
after that only touches new/changed sessions, so it's quick.

### Tuning
- `MAX_PER_RUN` — leave `0` on a VM. Set a number only if you want to spread a
  huge backfill over several hourly runs.
- `TELEMETRY_WORKERS` — telemetry threads per session. `4` suits a 4-OCPU A1;
  lower it to `2` on a smaller shape (e.g. E2.1.Micro) to avoid OOM.
- `REFRESH_DAYS` — how long after a session to keep re-fetching it for
  corrections (default `2`).

### Pointing the frontend at Oracle Object Storage (optional)
If you switch the store to Oracle, set the frontend's data base URL to the
bucket (`GITHUB_DATA_BASE_URL` / equivalent) — e.g.
`https://<namespace>.objectstorage.<region>.oci.customer-oci.com/n/<ns>/b/<bucket>/o`
or your bucket's public URL. The on-store layout is identical, so only the base
URL changes.
