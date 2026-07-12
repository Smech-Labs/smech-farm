use clap::{Parser, Subcommand};
use serde::{Deserialize, Serialize};
use std::fs;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::Path;
use std::process::Command;
use std::thread;
use std::time::Duration;

const VERSION:          &str = "2.0.0";
const HEARTBEAT_SECS:   u64  = 15;
const NFS_PKG_PATH:     &str = "/srv/smech-farm/packages";
const NFS_JOBS_PATH:    &str = "/srv/smech-farm/packages/jobs";
const WORKER_ID_PATH:   &str = "/etc/smech-farm/worker-id";
const WG_KEY_PATH:      &str = "/etc/smech-farm/worker-wg.key";
const WG_PUB_PATH:      &str = "/etc/smech-farm/worker-wg.pub";
const WG_CONF_PATH:     &str = "/etc/wireguard/smech-farm.conf";
const ORCHESTRATOR_ENV: &str = "SMECH_ORCHESTRATOR";

#[derive(Parser)]
#[command(name = "smech-farm-worker", version = VERSION,
          about = "SmechFarm worker agent")]
struct Cli {
    #[command(subcommand)]
    command: Cmd,
    /// Orchestrator address (host:port), overrides SMECH_ORCHESTRATOR env
    #[arg(long, global = true)]
    orchestrator: Option<String>,
}

#[derive(Subcommand)]
enum Cmd {
    /// Run the worker heartbeat daemon (register + stay online)
    Daemon,
    /// Request a package from the orchestrator download queue
    DownloadPackage {
        package: String,
        #[arg(long)]
        version: Option<String>,
    },
    /// Check if a package is available on the NFS share
    CheckPackage { package: String },
    /// Show worker status and orchestrator connection
    Status,
    /// List packages available on the NFS share
    ListPackages,
    /// Poll for and execute compile jobs from the NFS job queue
    RunJobs,
    /// Set up WireGuard tunnel to the orchestrator (for cloud/VPS deployment)
    WgSetup {
        /// Worker name shown in the orchestrator dashboard
        #[arg(long)]
        name: Option<String>,
        /// WireGuard interface name (default: smech-farm)
        #[arg(long, default_value = "smech-farm")]
        iface: String,
    },
    /// Show WireGuard tunnel status
    WgStatus {
        #[arg(long, default_value = "smech-farm")]
        iface: String,
    },
}

#[derive(Serialize, Deserialize)]
struct HeartbeatPayload { id: String, hostname: String, ip_addr: String, cores: u32 }

#[derive(Deserialize)]
struct WgRegisterResp {
    assigned_ip:       String,
    server_public_key: String,
    server_endpoint:   String,
    allowed_ips:       String,
    keepalive:         u32,
}

fn orchestrator_addr(cli_override: &Option<String>) -> String {
    cli_override.clone()
        .or_else(|| std::env::var(ORCHESTRATOR_ENV).ok())
        .unwrap_or_else(|| "127.0.0.1:7734".to_string())
}

fn worker_id() -> String {
    if let Ok(id) = fs::read_to_string(WORKER_ID_PATH) {
        return id.trim().to_string();
    }
    let id = format!("worker-{}-{}", hostname(), std::process::id());
    fs::create_dir_all(Path::new(WORKER_ID_PATH).parent().unwrap()).ok();
    fs::write(WORKER_ID_PATH, &id).ok();
    id
}

fn hostname() -> String {
    fs::read_to_string("/etc/hostname")
        .unwrap_or_else(|_| "unknown".to_string())
        .trim().to_string()
}

fn local_ip() -> String {
    if let Ok(stream) = TcpStream::connect("8.8.8.8:80") {
        if let Ok(addr) = stream.local_addr() {
            return addr.ip().to_string();
        }
    }
    "0.0.0.0".to_string()
}

fn cpu_cores() -> u32 {
    fs::read_to_string("/proc/cpuinfo")
        .unwrap_or_default()
        .lines()
        .filter(|l| l.starts_with("processor"))
        .count() as u32
}

// ── HTTP client (stdlib only) ─────────────────────────────────────────────────

fn http_post(addr: &str, path: &str, body: &str) -> Result<String, String> {
    let req = format!(
        "POST {} HTTP/1.0\r\nHost: {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
        path, addr, body.len(), body
    );
    let mut s = TcpStream::connect(addr).map_err(|e| format!("connect failed: {}", e))?;
    s.set_write_timeout(Some(Duration::from_secs(10))).ok();
    s.set_read_timeout(Some(Duration::from_secs(30))).ok();
    s.write_all(req.as_bytes()).map_err(|e| format!("write failed: {}", e))?;
    let mut resp = String::new();
    s.read_to_string(&mut resp).map_err(|e| format!("read failed: {}", e))?;
    Ok(resp.splitn(2, "\r\n\r\n").nth(1).unwrap_or("").to_string())
}

fn http_get(addr: &str, path: &str) -> Result<String, String> {
    let req = format!("GET {} HTTP/1.0\r\nHost: {}\r\n\r\n", path, addr);
    let mut s = TcpStream::connect(addr).map_err(|e| format!("connect failed: {}", e))?;
    s.set_read_timeout(Some(Duration::from_secs(10))).ok();
    s.write_all(req.as_bytes()).map_err(|e| format!("write failed: {}", e))?;
    let mut resp = String::new();
    s.read_to_string(&mut resp).map_err(|e| format!("read failed: {}", e))?;
    Ok(resp.splitn(2, "\r\n\r\n").nth(1).unwrap_or("").to_string())
}

// ── WireGuard setup ───────────────────────────────────────────────────────────

fn cmd_wg_setup(addr: &str, name: Option<String>, iface: &str) {
    println!("[*] SmechFarm WireGuard setup");
    println!("[*] Orchestrator: {}", addr);

    // Check WireGuard is enabled on the orchestrator
    match http_get(addr, "/api/wg/server-info") {
        Err(e) => { eprintln!("[-] Cannot reach orchestrator: {}", e); return; }
        Ok(r) if r.contains("not enabled") => {
            eprintln!("[-] WireGuard is not enabled on this orchestrator.");
            eprintln!("    Set wireguard.enabled = true in smech-farm.toml on the orchestrator.");
            return;
        }
        Ok(_) => {}
    }

    fs::create_dir_all("/etc/smech-farm").ok();
    fs::create_dir_all("/etc/wireguard").ok();

    // Generate worker key pair if not present
    if !Path::new(WG_KEY_PATH).exists() {
        let privkey = Command::new("wg").arg("genkey").output();
        match privkey {
            Ok(o) if o.status.success() => {
                fs::write(WG_KEY_PATH, &o.stdout).ok();
                // Derive public key
                let pubkey = Command::new("wg").arg("pubkey")
                    .stdin(std::process::Stdio::from(fs::File::open(WG_KEY_PATH).unwrap()))
                    .output();
                if let Ok(p) = pubkey {
                    fs::write(WG_PUB_PATH, &p.stdout).ok();
                }
                println!("[+] Generated WireGuard key pair");
            }
            _ => {
                eprintln!("[-] wg binary not found. Install wireguard-tools first:");
                eprintln!("    apt install wireguard-tools   (Debian/Ubuntu)");
                eprintln!("    dnf install wireguard-tools   (Fedora/RHEL)");
                return;
            }
        }
    }

    let pubkey = fs::read_to_string(WG_PUB_PATH)
        .unwrap_or_default()
        .trim()
        .to_string();
    let privkey = fs::read_to_string(WG_KEY_PATH)
        .unwrap_or_default()
        .trim()
        .to_string();

    let worker_name = name.unwrap_or_else(|| hostname());
    println!("[*] Registering as '{}'...", worker_name);

    let body = format!(r#"{{"name":"{}","public_key":"{}"}}"#, worker_name, pubkey);
    let resp = match http_post(addr, "/api/wg/register-worker", &body) {
        Ok(r)  => r,
        Err(e) => { eprintln!("[-] Registration failed: {}", e); return; }
    };

    let reg: WgRegisterResp = match serde_json::from_str(&resp) {
        Ok(r)  => r,
        Err(_) => { eprintln!("[-] Unexpected response: {}", resp); return; }
    };

    println!("[+] Assigned WireGuard IP: {}", reg.assigned_ip);

    // Write WireGuard config
    let conf = format!(
        "[Interface]\nAddress = {}/32\nPrivateKey = {}\n\n[Peer]\nPublicKey = {}\nEndpoint = {}\nAllowedIPs = {}\nPersistentKeepalive = {}\n",
        reg.assigned_ip,
        privkey,
        reg.server_public_key,
        reg.server_endpoint,
        reg.allowed_ips,
        reg.keepalive,
    );
    if let Err(e) = fs::write(WG_CONF_PATH, &conf) {
        eprintln!("[-] Cannot write WireGuard config to {}: {}", WG_CONF_PATH, e);
        eprintln!("    Try running as root.");
        return;
    }
    // Set permissions — private key is in the file
    let _ = Command::new("chmod").args(["600", WG_CONF_PATH]).status();

    // Bring up the interface
    let up = Command::new("wg-quick").args(["up", WG_CONF_PATH]).status();
    match up {
        Ok(s) if s.success() => println!("[+] WireGuard interface {} is up", iface),
        _ => {
            eprintln!("[!] wg-quick up failed — you may need to run this as root, or bring");
            eprintln!("    up the interface manually: wg-quick up {}", WG_CONF_PATH);
        }
    }

    // Extract orchestrator WireGuard IP to use as the new orchestrator address
    let wg_host = reg.allowed_ips.split('/').next()
        .and_then(|net| {
            let parts: Vec<&str> = net.split('.').collect();
            if parts.len() == 4 {
                Some(format!("{}.{}.{}.1", parts[0], parts[1], parts[2]))
            } else { None }
        })
        .unwrap_or_else(|| "10.99.0.1".to_string());

    let port = addr.split(':').last().unwrap_or("7734");
    println!();
    println!("[+] WireGuard setup complete!");
    println!("    Set SMECH_ORCHESTRATOR={}:{} in your environment", wg_host, port);
    println!("    or pass --orchestrator {}:{} to worker commands", wg_host, port);
    println!();
    println!("    To start the worker daemon via WireGuard:");
    println!("    smech-farm-worker daemon --orchestrator {}:{}", wg_host, port);

    // Persist the WireGuard orchestrator address for the daemon
    fs::write("/etc/smech-farm/wg-orchestrator", format!("{}:{}", wg_host, port)).ok();
}

fn cmd_wg_status(iface: &str) {
    println!("[*] WireGuard interface: {}", iface);
    if Path::new(WG_CONF_PATH).exists() {
        println!("[+] Config: {}", WG_CONF_PATH);
    }
    if Path::new(WG_PUB_PATH).exists() {
        let pub_key = fs::read_to_string(WG_PUB_PATH).unwrap_or_default();
        println!("[+] Worker public key: {}", pub_key.trim());
    }
    if let Ok(addr) = fs::read_to_string("/etc/smech-farm/wg-orchestrator") {
        println!("[+] WireGuard orchestrator: {}", addr.trim());
    }
    let _ = Command::new("wg").args(["show", iface]).status();
}

// ── Daemon ────────────────────────────────────────────────────────────────────

fn cmd_daemon(addr: &str) {
    eprintln!("[smech-farm-worker] starting heartbeat daemon → {}", addr);
    let id = worker_id();
    loop {
        send_heartbeat(addr, &id);
        check_packages_ready();
        thread::sleep(Duration::from_secs(HEARTBEAT_SECS));
    }
}

fn send_heartbeat(addr: &str, id: &str) {
    let payload = serde_json::json!({
        "id":       id,
        "hostname": hostname(),
        "ip_addr":  local_ip(),
        "cores":    cpu_cores(),
    }).to_string();
    match http_post(addr, "/api/workers/heartbeat", &payload) {
        Ok(_)  => {}
        Err(e) => eprintln!("[smech-farm-worker] heartbeat failed: {}", e),
    }
}

fn check_packages_ready() {
    let flag = format!("{}/PACKAGES_READY", NFS_PKG_PATH);
    if Path::new(&flag).exists() {
        eprintln!("[smech-farm-worker] packages updated on NFS share");
        fs::remove_file(&flag).ok();
    }
}

fn cmd_download_package(addr: &str, package: &str, version: Option<String>) {
    let id = worker_id();
    println!("[*] Requesting package '{}' from orchestrator queue...", package);
    let body = serde_json::json!({
        "package_name": package,
        "version":      version,
        "worker_id":    id,
    }).to_string();
    match http_post(addr, "/api/queue/add", &body) {
        Ok(r)  => println!("[+] {}", r.trim()),
        Err(e) => eprintln!("[-] Failed: {}", e),
    }
    let dest = format!("{}/{}.tar.xz", NFS_PKG_PATH, package);
    for i in 0..24 {
        if Path::new(&dest).exists() {
            println!("[+] Package available at: {}", dest);
            return;
        }
        println!("    waiting... ({}/24)", i + 1);
        thread::sleep(Duration::from_secs(5));
    }
    eprintln!("[-] Timed out waiting for package.");
}

fn cmd_check_package(package: &str) {
    let dest = format!("{}/{}.tar.xz", NFS_PKG_PATH, package);
    if Path::new(&dest).exists() {
        println!("[+] {} — available at {}", package, dest);
    } else {
        println!("[-] {} — not found on NFS share", package);
    }
}

fn cmd_status(addr: &str) {
    println!("  Worker ID    : {}", worker_id());
    println!("  Hostname     : {}", hostname());
    println!("  IP           : {}", local_ip());
    println!("  Cores        : {}", cpu_cores());
    println!("  NFS share    : {}", NFS_PKG_PATH);
    println!("  Orchestrator : {}", addr);
    if Path::new("/etc/smech-farm/wg-orchestrator").exists() {
        let wg_addr = fs::read_to_string("/etc/smech-farm/wg-orchestrator").unwrap_or_default();
        println!("  WireGuard    : active ({})", wg_addr.trim());
    }
    match http_get(addr, "/api/status") {
        Ok(r)  => println!("  Farm status  : {}", r.trim()),
        Err(e) => println!("  Farm status  : unreachable ({})", e),
    }
}

fn cmd_list_packages() {
    match fs::read_dir(NFS_PKG_PATH) {
        Err(_) => { println!("[-] NFS share not mounted at {}", NFS_PKG_PATH); return; }
        Ok(entries) => {
            let mut pkgs: Vec<String> = entries
                .filter_map(|e| e.ok())
                .map(|e| e.file_name().to_string_lossy().to_string())
                .filter(|n| n.ends_with(".tar.xz"))
                .collect();
            pkgs.sort();
            if pkgs.is_empty() {
                println!("  No packages on NFS share yet.");
            } else {
                println!("  Packages on NFS share ({}):", pkgs.len());
                for p in pkgs { println!("    {}", p); }
            }
        }
    }
}

fn cmd_run_jobs() {
    eprintln!("[smech-farm-worker] polling for compile jobs in {}", NFS_JOBS_PATH);
    let id = worker_id();
    loop {
        if let Ok(entries) = fs::read_dir(NFS_JOBS_PATH) {
            for entry in entries.filter_map(|e| e.ok()) {
                let path = entry.path();
                let name = path.file_name().unwrap_or_default().to_string_lossy().to_string();
                if name.starts_with('.') { continue; }
                eprintln!("[smech-farm-worker] picked up job: {}", name);
                let taken = format!("{}/{}.{}", NFS_JOBS_PATH, name, id);
                if fs::rename(&path, &taken).is_err() { continue; }
                let result = Command::new("icecc")
                    .arg("make").arg("-C")
                    .arg(Path::new(&taken).parent().unwrap_or(Path::new(".")))
                    .status()
                    .or_else(|_| Command::new("sh")
                        .args(["-c", &format!("cd /tmp && tar xf '{}' && make", taken)])
                        .status());
                match result {
                    Ok(s) if s.success() => eprintln!("[smech-farm-worker] job {} done", name),
                    _ => eprintln!("[smech-farm-worker] job {} failed", name),
                }
            }
        }
        thread::sleep(Duration::from_secs(3));
    }
}

fn main() {
    let cli  = Cli::parse();
    let addr = orchestrator_addr(&cli.orchestrator);
    match cli.command {
        Cmd::Daemon                               => cmd_daemon(&addr),
        Cmd::DownloadPackage { package, version } => cmd_download_package(&addr, &package, version),
        Cmd::CheckPackage    { package }          => cmd_check_package(&package),
        Cmd::Status                               => cmd_status(&addr),
        Cmd::ListPackages                         => cmd_list_packages(),
        Cmd::RunJobs                              => cmd_run_jobs(),
        Cmd::WgSetup { name, iface }              => cmd_wg_setup(&addr, name, &iface),
        Cmd::WgStatus { iface }                   => cmd_wg_status(&iface),
    }
}
