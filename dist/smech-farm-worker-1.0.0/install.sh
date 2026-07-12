#!/usr/bin/env bash
set -e
ORCHESTRATOR="${1:-orchestrator:7734}"
echo "[*] Installing smech-farm-worker (orchestrator: ${ORCHESTRATOR})"
install -m 755 bin/smech-farm-worker /usr/local/bin/
mkdir -p /etc/smech-farm
# Inject orchestrator address
sed "s|orchestrator:7734|${ORCHESTRATOR}|" \
    systemd/smech-farm-worker.service > /etc/systemd/system/smech-farm-worker.service
systemctl daemon-reload
systemctl enable --now smech-farm-worker
echo "[+] Worker registered. Check status: smech-farm-worker status"
