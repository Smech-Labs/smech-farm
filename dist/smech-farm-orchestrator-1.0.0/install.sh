#!/usr/bin/env bash
set -e
echo "[*] Installing smech-farm-orchestrator"
install -m 755 bin/smech-farm-orchestrator /usr/local/bin/
mkdir -p /var/lib/smech-farm /srv/smech-farm/packages/jobs
cp systemd/smech-farm-daemon.service /etc/systemd/system/
cp systemd/smech-farm-api.service    /etc/systemd/system/
systemctl daemon-reload
systemctl enable --now smech-farm-daemon smech-farm-api
echo "[+] Done. Web UI: http://localhost:7734/ui"
echo "[+] Shell container: docker build -f Dockerfile.shell -t smech-farm-shell ."
