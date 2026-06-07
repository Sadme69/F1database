# F1 Data Updater — Oracle Cloud bundle

This is a **self-contained bundle**. Everything the hourly F1 data updater needs
is in this folder — no `git clone` required.

```
oracle_f1_updater/
├── backend/                 # FastF1 download + telemetry + storage code (dependency)
└── oracle_cloud_updater/    # the updater, setup script, and hourly timer
```

## Setup on the Oracle Cloud VM (Ampere A1 / Ubuntu 22.04)

1. Copy this whole folder to the VM and unzip it (e.g. `~/oracle_f1_updater`).
2. Provision:
   ```bash
   cd oracle_f1_updater/oracle_cloud_updater
   chmod +x setup.sh
   ./setup.sh
   ```
   This creates a Python venv, installs all dependencies
   (`../backend/requirements.txt` + the updater's), and installs + enables the
   hourly `systemd` timer.
3. Configure credentials:
   ```bash
   nano .env
   ```
   Set `DEST` (`github`, `oracle`, or `github,oracle`) and the matching
   credentials. See `oracle_cloud_updater/README.md` for every option.
4. Test one run, then let the timer take over:
   ```bash
   .venv/bin/python updater.py
   journalctl -u f1-updater.service -f
   ```

Full details, tuning, and how each destination works are in
**`oracle_cloud_updater/README.md`**.

> Note: the bundled `backend/` folder has the venv, caches, and large data
> directories stripped out — `setup.sh` rebuilds the venv from scratch on the VM.
