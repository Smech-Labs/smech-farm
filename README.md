# smech-farm

A self-hosted, distributed build farm with a web UI, REST API, and an SSH-accessible shell — all shipped as a single zip. Works on bare metal, VMs, and cloud instances (GCP, AWS, Azure, Hetzner, Contabo) via optional WireGuard tunneling.

## Architecture

```
┌─────────────────────────────────────────────┐
│  Orchestrator host                          │
│                                             │
│  ┌──────────────────────────────────────┐   │
│  │  Docker container (smech-farm-shell) │   │
│  │  ├─ smech-farm-orchestrator daemon   │   │
│  │  ├─ smech-farm-orchestrator serve    │   │◄── SSH :22  (shell access)
│  │  └─ sshd (ForceCommand → shell)     │   │◄── HTTP :7734 (web UI / API)
│  └──────────────────────────────────────┘   │
│                                             │
│  NFS share: /srv/smech-farm/packages/       │────► Workers (LAN or WireGuard)
│  SQLite DB: /var/lib/smech-farm/queue.db    │
└─────────────────────────────────────────────┘

── LAN workers ──────────────────────────────────────────────────────
Worker 1 ──100GbE──┐
Worker 2 ──100GbE──┤──► Orchestrator
Worker N ──100GbE──┘

── Cloud/VPS workers (WireGuard, optional) ──────────────────────────
GCP VM    ──WireGuard──┐
AWS VM    ──WireGuard──┤──► Orchestrator wg0 (10.99.0.1)
Hetzner   ──WireGuard──┘
Contabo   ──WireGuard──┘
```

- **Orchestrator**: runs inside a locked-down Alpine Docker container. Exposes an SSH shell and an HTTP API + web UI.
- **Worker**: lightweight daemon installed on each build node. Sends heartbeats, picks up jobs from the NFS share, and reports results back.
- **Client**: CLI tool for developers to submit jobs and check status. Connects directly — no WireGuard needed.
- **WireGuard**: optional tunnel for cloud/VPS workers. LAN workers work without it — disabled by default.

## Quick start

### Requirements

- Docker Engine (orchestrator host)
- Rust toolchain (`cargo`) to build from source
- `zip` utility
- `wireguard-tools` on orchestrator and workers **only if** using WireGuard cloud mode

### Build

```bash
git clone https://github.com/Smech-Labs/smech-farm
cd smech-farm
bash build.sh
```

### Deploy orchestrator

```bash
cd dist
tar xf smech-farm-orchestrator-2.0.0-linux-x86_64.tar.gz
cd smech-farm-orchestrator-2.0.0
docker load -i smech-farm-shell-2.0.0.tar
sudo bash install.sh
```

Add your SSH public key:
```bash
docker exec smech-farm-shell sh -c \
  'echo "ssh-ed25519 AAAA..." >> /home/smech-farm/.ssh/authorized_keys'
```

### Deploy workers (LAN)

```bash
tar xf smech-farm-worker-2.0.0-linux-x86_64.tar.gz
cd smech-farm-worker-2.0.0
sudo bash install.sh <orchestrator-ip>:7734
```

### Deploy workers (Cloud / VPS via WireGuard)

**1. Enable WireGuard on the orchestrator** — copy `smech-farm.example.toml` to `smech-farm.toml`:

```toml
[wireguard]
enabled         = true
server_endpoint = "YOUR_PUBLIC_IP:51820"
```

Restart the orchestrator. It auto-generates its key pair.

**2. Open UDP port 51820** on your firewall/security group.

**3. On each cloud worker:**

```bash
apt install wireguard-tools   # or: dnf install wireguard-tools

sudo smech-farm-worker wg-setup \
  --orchestrator <public-ip>:7734 \
  --name worker-gcp-1

sudo smech-farm-worker daemon --orchestrator 10.99.0.1:7734
```

The worker generates its own key pair, registers with the orchestrator, gets assigned a WireGuard IP (10.99.0.x), and all traffic flows through the encrypted tunnel.

### Install client

```bash
tar xf smech-farm-client-2.0.0-linux-x86_64.tar.gz
cd smech-farm-client-2.0.0
sudo bash install.sh

export SMECH_ORCHESTRATOR=<orchestrator-ip>:7734
```

## Configuration (smech-farm.toml)

Copy `smech-farm.example.toml` to `smech-farm.toml` next to the binary or at `/etc/smech-farm/smech-farm.toml`.

```toml
[farm]
name         = "My Build Farm"
tagline      = "Distributed Build Farm"
accent_color = "#58a6ff"
logo_url     = ""          # URL or base64 data URI for custom logo

[wireguard]
enabled         = false    # set true for cloud workers
server_endpoint = ""       # your public IP:port
listen_port     = 51820
server_wg_ip    = "10.99.0.1/24"
worker_ip_pool  = "10.99.0.0/24"
```

WireGuard is **disabled by default**. With `enabled = false` the farm is identical to v1.x behavior.

## Shell commands

| Command | Description |
|---|---|
| `push compile <file>` | Submit a compile job |
| `sync-network-gateway` | Trigger gateway sync |
| `download-queued-packages` | Force-process the package download queue |
| `push-packages` | Push NFS packages out to workers |
| `orchestrator-shell` | Drop into the host OS shell (privileged, logged) |
| `status` | Farm status summary |
| `workers` | List registered workers |
| `queue` | Show package download queue |
| `jobs` | Show compile job queue |
| `wg-status` | WireGuard interface status *(WireGuard only)* |
| `wg-list-peers` | List registered WireGuard peers *(WireGuard only)* |
| `wg-add-worker <name> <pubkey>` | Manually register a WireGuard peer *(WireGuard only)* |
| `help` | Show all commands |

## REST API

| Endpoint | Method | Description |
|---|---|---|
| `/api/status` | GET | Farm-wide status |
| `/api/workers` | GET | All registered workers |
| `/api/queue` | GET | Package download queue |
| `/api/jobs` | GET | Compile job queue |
| `/api/queue/add` | POST | Queue a package download |
| `/api/jobs/submit` | POST | Submit a compile job |
| `/api/gateway/sync` | POST | Trigger gateway sync |
| `/api/workers/heartbeat` | POST | Worker heartbeat |
| `/api/wg/server-info` | GET | WireGuard server public key + endpoint |
| `/api/wg/peers` | GET | Registered WireGuard peers |
| `/api/wg/register-worker` | POST | Register a worker's WireGuard public key |
| `/ui` | GET | Web dashboard |

## License

MIT
