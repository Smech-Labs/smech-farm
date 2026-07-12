#!/usr/bin/env bash
set -euo pipefail

VERSION="1.0.0"
DIST="dist"
TARGET="x86_64-unknown-linux-gnu"

IMAGE="smech-farm-shell"
IMAGE_TAG="${IMAGE}:${VERSION}"

echo "==> Building smech-farm v${VERSION}"

# Docker is required — the orchestrator shell runs inside a container
if ! command -v docker &>/dev/null; then
    echo "[-] Docker is not installed or not in PATH."
    echo "    The smech-farm shell runs as a locked-down Docker container."
    echo "    Install Docker Engine: https://docs.docker.com/engine/install/"
    exit 1
fi
if ! docker info &>/dev/null; then
    echo "[-] Docker daemon is not running or current user lacks permission."
    echo "    Start dockerd or add yourself to the 'docker' group."
    exit 1
fi

# Build all 3 binaries
cargo build --release --target "${TARGET}" 2>&1

strip "target/${TARGET}/release/smech-farm-orchestrator"
strip "target/${TARGET}/release/smech-farm-worker"
strip "target/${TARGET}/release/smech-farm-client"

# Build the Docker shell image with the orchestrator binary baked in
echo "==> Building Docker image ${IMAGE_TAG}"
cp "target/${TARGET}/release/smech-farm-orchestrator" smech-farm-orchestrator
if ! docker build -f Dockerfile.shell -t "${IMAGE_TAG}" -t "${IMAGE}:latest" . ; then
    rm -f smech-farm-orchestrator
    echo "[-] Docker image build failed."
    exit 1
fi
rm -f smech-farm-orchestrator
echo "[+] Docker image built: ${IMAGE_TAG}"

# Save the Docker image as a tarball so it can be shipped / loaded offline
echo "==> Saving Docker image to smech-farm-shell-${VERSION}.tar"
docker save "${IMAGE_TAG}" -o "smech-farm-shell-${VERSION}.tar"

echo "==> Packaging tarballs"
rm -rf "${DIST}" && mkdir "${DIST}"
cp "smech-farm-shell-${VERSION}.tar" "${DIST}/"

# ── Orchestrator tarball ──────────────────────────────────────────────────────
ORC_DIR="${DIST}/smech-farm-orchestrator-${VERSION}"
mkdir -p "${ORC_DIR}/bin" "${ORC_DIR}/systemd"

cp "target/${TARGET}/release/smech-farm-orchestrator" "${ORC_DIR}/bin/"
cp Dockerfile.shell "${ORC_DIR}/"

cat > "${ORC_DIR}/systemd/smech-farm-daemon.service" <<'EOF'
[Unit]
Description=SmechFarm Orchestrator Daemon
After=network.target

[Service]
ExecStart=/usr/local/bin/smech-farm-orchestrator daemon
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

cat > "${ORC_DIR}/systemd/smech-farm-api.service" <<'EOF'
[Unit]
Description=SmechFarm API and Web UI
After=network.target

[Service]
ExecStart=/usr/local/bin/smech-farm-orchestrator serve
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

cat > "${ORC_DIR}/install.sh" <<'EOF'
#!/usr/bin/env bash
set -e

IMAGE="smech-farm-shell"
CONTAINER="smech-farm-shell"

# ── Preflight ──────────────────────────────────────────────────────────────────
if ! command -v docker &>/dev/null; then
    echo "[-] Docker is required but not found. Install Docker Engine first."
    exit 1
fi
if ! docker info &>/dev/null; then
    echo "[-] Docker daemon is not running or you lack permission."
    exit 1
fi

echo "[*] Installing smech-farm-orchestrator"
install -m 755 bin/smech-farm-orchestrator /usr/local/bin/
mkdir -p /var/lib/smech-farm /srv/smech-farm/packages/jobs

# ── Docker image ───────────────────────────────────────────────────────────────
if docker image inspect "${IMAGE}:latest" &>/dev/null; then
    echo "[*] Docker image ${IMAGE}:latest already present — skipping build"
else
    echo "[-] Docker image ${IMAGE}:latest not found."
    echo "    Run build.sh first to build the image, or load it from the saved tar:"
    echo "      docker load -i smech-farm-shell.tar"
    exit 1
fi

# ── Stop + remove existing container if present ────────────────────────────────
if docker ps -a --format '{{.Names}}' | grep -q "^${CONTAINER}$"; then
    echo "[*] Removing existing container ${CONTAINER}"
    docker rm -f "${CONTAINER}"
fi

# ── Spin up the container ──────────────────────────────────────────────────────
echo "[*] Starting smech-farm shell container"
docker run -d \
    --name "${CONTAINER}" \
    --restart unless-stopped \
    --pid=host \
    -p 22:22 \
    -p 7734:7734 \
    -v /var/lib/smech-farm:/var/lib/smech-farm \
    -v /srv/smech-farm:/srv/smech-farm \
    "${IMAGE}:latest"

echo ""
echo "[+] smech-farm is running."
echo "    SSH shell:  ssh smech-farm@$(hostname -I | awk '{print $1}')"
echo "    Web UI:     http://$(hostname -I | awk '{print $1}'):7734/ui"
echo ""
echo "    Add your SSH public key:"
echo "      docker exec ${CONTAINER} sh -c 'echo \"<pubkey>\" >> /home/smech-farm/.ssh/authorized_keys'"
EOF
chmod +x "${ORC_DIR}/install.sh"

tar -czf "${DIST}/smech-farm-orchestrator-${VERSION}-linux-x86_64.tar.gz" \
    -C "${DIST}" "smech-farm-orchestrator-${VERSION}"

# ── Worker tarball ────────────────────────────────────────────────────────────
WRK_DIR="${DIST}/smech-farm-worker-${VERSION}"
mkdir -p "${WRK_DIR}/bin" "${WRK_DIR}/systemd"

cp "target/${TARGET}/release/smech-farm-worker" "${WRK_DIR}/bin/"

cat > "${WRK_DIR}/systemd/smech-farm-worker.service" <<'EOF'
[Unit]
Description=SmechFarm Worker Agent
After=network.target

[Service]
Environment=SMECH_ORCHESTRATOR=orchestrator:7734
ExecStart=/usr/local/bin/smech-farm-worker daemon
Restart=on-failure
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF

cat > "${WRK_DIR}/install.sh" <<'EOF'
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
EOF
chmod +x "${WRK_DIR}/install.sh"

tar -czf "${DIST}/smech-farm-worker-${VERSION}-linux-x86_64.tar.gz" \
    -C "${DIST}" "smech-farm-worker-${VERSION}"

# ── Client tarball ────────────────────────────────────────────────────────────
CLI_DIR="${DIST}/smech-farm-client-${VERSION}"
mkdir -p "${CLI_DIR}/bin"

cp "target/${TARGET}/release/smech-farm-client" "${CLI_DIR}/bin/"

cat > "${CLI_DIR}/install.sh" <<'EOF'
#!/usr/bin/env bash
set -e
echo "[*] Installing smech-farm-client"
install -m 755 bin/smech-farm-client /usr/local/bin/
echo "[+] Done. Set SMECH_ORCHESTRATOR=<host>:7734 or use --orchestrator flag."
echo "    Examples:"
echo "      smech-farm-client status"
echo "      smech-farm-client push compile ./myfile.tar.gz"
echo "      smech-farm-client shell --host orchestrator"
EOF
chmod +x "${CLI_DIR}/install.sh"

tar -czf "${DIST}/smech-farm-client-${VERSION}-linux-x86_64.tar.gz" \
    -C "${DIST}" "smech-farm-client-${VERSION}"

# ── Final ZIP ─────────────────────────────────────────────────────────────────
echo "==> Creating smech-farm-${VERSION}.zip"
cd "${DIST}"
zip -q "smech-farm-${VERSION}.zip" \
    "smech-farm-orchestrator-${VERSION}-linux-x86_64.tar.gz" \
    "smech-farm-worker-${VERSION}-linux-x86_64.tar.gz" \
    "smech-farm-client-${VERSION}-linux-x86_64.tar.gz" \
    "smech-farm-shell-${VERSION}.tar"
cd ..

echo ""
echo "==> Build complete:"
ls -lh "${DIST}/smech-farm-${VERSION}.zip"
echo ""
echo "    Tarballs in dist/:"
ls -lh "${DIST}"/*.tar.gz
