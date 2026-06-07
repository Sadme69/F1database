#!/usr/bin/env bash
# Provision the F1 data updater on an Oracle Cloud Free Tier VM (Ampere A1 / Ubuntu).
# Run once from inside this folder:  ./setup.sh
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$HERE")"
VENV="$HERE/.venv"

echo "==> Installing system packages (git, python venv, build basics)"
sudo apt-get update -y
sudo apt-get install -y python3-venv python3-pip git

echo "==> Creating virtualenv at $VENV"
python3 -m venv "$VENV"
"$VENV/bin/python" -m pip install --upgrade pip wheel

echo "==> Installing Python deps (backend + updater extras)"
"$VENV/bin/pip" install -r "$REPO_ROOT/backend/requirements.txt"
"$VENV/bin/pip" install -r "$HERE/requirements.txt"

if [ ! -f "$HERE/.env" ]; then
  echo "==> Creating .env from .env.example (EDIT IT before enabling the timer)"
  cp "$HERE/.env.example" "$HERE/.env"
fi

echo "==> Rendering systemd units with absolute paths"
USER_NAME="$(whoami)"
sudo tee /etc/systemd/system/f1-updater.service >/dev/null <<UNIT
[Unit]
Description=F1 data updater (download + compress + publish)
After=network-online.target
Wants=network-online.target

[Service]
Type=oneshot
User=$USER_NAME
WorkingDirectory=$HERE
ExecStart=$VENV/bin/python $HERE/updater.py
# Keep one run from overlapping the next; let a long backfill finish.
TimeoutStartSec=0
Nice=10
UNIT

sudo cp "$HERE/systemd/f1-updater.timer" /etc/systemd/system/f1-updater.timer

echo "==> Enabling hourly timer"
sudo systemctl daemon-reload
sudo systemctl enable --now f1-updater.timer

cat <<DONE

Setup complete.

  1. Edit your config:        nano $HERE/.env
  2. Run once now to test:    $VENV/bin/python $HERE/updater.py
  3. Timer status:            systemctl status f1-updater.timer
  4. Next/last runs:          systemctl list-timers f1-updater.timer
  5. Logs:                    journalctl -u f1-updater.service -f

DONE
