#!/usr/bin/env bash
set -e
echo "[*] Installing smech-farm-client"
install -m 755 bin/smech-farm-client /usr/local/bin/
echo "[+] Done. Set SMECH_ORCHESTRATOR=<host>:7734 or use --orchestrator flag."
echo "    Examples:"
echo "      smech-farm-client status"
echo "      smech-farm-client push compile ./myfile.tar.gz"
echo "      smech-farm-client shell --host orchestrator"
