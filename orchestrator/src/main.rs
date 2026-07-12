use clap::{Parser, Subcommand};
use once_cell::sync::Lazy;
use rusqlite::{Connection, params};
use serde::{Deserialize, Serialize};
use std::fs;
use std::io::{self, BufRead, Write};
use std::path::Path;
use std::process::Command;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tiny_http::{Response, Server};

const DB_PATH:       &str = "/var/lib/smech-farm/queue.db";
const NFS_PKG_PATH:  &str = "/srv/smech-farm/packages";
const API_PORT:      u16  = 7734;
const DAEMON_POLL_S: u64  = 5;
const VERSION:       &str = "2.0.0";
const CONFIG_PATHS:  &[&str] = &["./smech-farm.toml", "/etc/smech-farm/smech-farm.toml"];

// ── Config ────────────────────────────────────────────────────────────────────

#[derive(Deserialize, Default)]
struct Config {
    #[serde(default)]
    farm: FarmConfig,
    #[serde(default)]
    wireguard: WireGuardConfig,
}

#[derive(Deserialize)]
struct FarmConfig {
    #[serde(default = "default_farm_name")]
    name: String,
    #[serde(default)]
    logo_url: String,
    #[serde(default = "default_accent")]
    accent_color: String,
    #[serde(default = "default_tagline")]
    tagline: String,
}

impl Default for FarmConfig {
    fn default() -> Self {
        Self {
            name: default_farm_name(),
            logo_url: String::new(),
            accent_color: default_accent(),
            tagline: default_tagline(),
        }
    }
}

#[derive(Deserialize, Default)]
struct WireGuardConfig {
    #[serde(default)]
    enabled: bool,
    #[serde(default = "default_wg_iface")]
    interface: String,
    #[serde(default)]
    server_endpoint: String,
    #[serde(default = "default_wg_port")]
    listen_port: u16,
    #[serde(default = "default_wg_server_ip")]
    server_wg_ip: String,
    #[serde(default = "default_wg_pool")]
    worker_ip_pool: String,
}

fn default_farm_name()   -> String { "SmechFarm".to_string() }
fn default_accent()      -> String { "#58a6ff".to_string() }
fn default_tagline()     -> String { "Distributed Build Farm".to_string() }
fn default_wg_iface()    -> String { "wg0".to_string() }
fn default_wg_port()     -> u16    { 51820 }
fn default_wg_server_ip() -> String { "10.99.0.1/24".to_string() }
fn default_wg_pool()     -> String { "10.99.0.0/24".to_string() }

static CONFIG: Lazy<Config> = Lazy::new(|| {
    for path in CONFIG_PATHS {
        if let Ok(content) = fs::read_to_string(path) {
            if let Ok(cfg) = toml::from_str::<Config>(&content) {
                eprintln!("[smech-farm] config loaded from {}", path);
                return cfg;
            }
        }
    }
    eprintln!("[smech-farm] no config file found, using defaults");
    Config::default()
});

// ── CLI ───────────────────────────────────────────────────────────────────────

#[derive(Parser)]
#[command(name = "smech-farm-orchestrator", version = VERSION,
          about = "SmechFarm orchestrator — daemon, shell, and API server")]
struct Cli {
    #[command(subcommand)]
    command: Mode,
}

#[derive(Subcommand)]
enum Mode {
    /// Run the background daemon (package queue processor + worker pusher)
    Daemon,
    /// Start the interactive smech-farm shell (used as SSH ForceCommand)
    Shell,
    /// Start the HTTP API and Web UI on port 7734
    Serve,
    /// Run all three modes together (recommended for production)
    All,
}

// ── Database ──────────────────────────────────────────────────────────────────

fn open_db() -> Connection {
    fs::create_dir_all(Path::new(DB_PATH).parent().unwrap()).ok();
    let conn = Connection::open(DB_PATH).expect("cannot open smech-farm DB");
    conn.execute_batch("
        CREATE TABLE IF NOT EXISTS package_queue (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            package_name TEXT    NOT NULL,
            version      TEXT    DEFAULT '',
            worker_id    TEXT    NOT NULL,
            status       TEXT    DEFAULT 'pending',
            requested_at INTEGER NOT NULL,
            completed_at INTEGER
        );
        CREATE TABLE IF NOT EXISTS compile_jobs (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            filename     TEXT    NOT NULL,
            content      BLOB    NOT NULL,
            submitted_by TEXT    NOT NULL,
            status       TEXT    DEFAULT 'queued',
            submitted_at INTEGER NOT NULL,
            completed_at INTEGER,
            worker_id    TEXT    DEFAULT NULL
        );
        CREATE TABLE IF NOT EXISTS workers (
            id        TEXT    PRIMARY KEY,
            hostname  TEXT    NOT NULL,
            ip_addr   TEXT    NOT NULL,
            cores     INTEGER DEFAULT 0,
            last_seen INTEGER NOT NULL,
            status    TEXT    DEFAULT 'online'
        );
        CREATE TABLE IF NOT EXISTS gateway_sync_log (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            triggered_by TEXT    NOT NULL,
            started_at   INTEGER NOT NULL,
            result       TEXT    DEFAULT 'pending'
        );
        CREATE TABLE IF NOT EXISTS wg_peers (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            worker_name TEXT    NOT NULL UNIQUE,
            public_key  TEXT    NOT NULL UNIQUE,
            assigned_ip TEXT    NOT NULL UNIQUE,
            added_at    INTEGER NOT NULL
        );
    ").expect("DB schema init failed");
    conn
}

fn now() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64
}

// ── WireGuard ─────────────────────────────────────────────────────────────────

fn wg_enabled() -> bool { CONFIG.wireguard.enabled }

fn wg_key_path() -> String {
    format!("/etc/smech-farm/{}.key", CONFIG.wireguard.interface)
}

fn wg_pubkey_path() -> String {
    format!("/etc/smech-farm/{}.pub", CONFIG.wireguard.interface)
}

fn setup_wireguard() {
    if !wg_enabled() { return; }
    let wg = &CONFIG.wireguard;
    let iface = &wg.interface;

    fs::create_dir_all("/etc/smech-farm").ok();

    // Generate server key pair if not present
    if !Path::new(&wg_key_path()).exists() {
        let privkey = Command::new("wg").arg("genkey").output();
        match privkey {
            Ok(o) if o.status.success() => {
                fs::write(&wg_key_path(), &o.stdout).ok();
                let pubkey = Command::new("wg").arg("pubkey")
                    .stdin(std::process::Stdio::from(
                        fs::File::open(&wg_key_path()).unwrap()
                    )).output();
                if let Ok(p) = pubkey {
                    fs::write(&wg_pubkey_path(), &p.stdout).ok();
                }
                eprintln!("[wg] generated server key pair");
            }
            _ => {
                eprintln!("[wg] WARNING: wg binary not found — WireGuard will not be active");
                return;
            }
        }
    }

    // Create interface (ignore error if already exists)
    let _ = Command::new("ip")
        .args(["link", "add", iface, "type", "wireguard"])
        .status();

    // Apply private key and listen port
    let _ = Command::new("wg")
        .args(["set", iface,
               "private-key", &wg_key_path(),
               "listen-port", &wg.listen_port.to_string()])
        .status();

    // Assign server WireGuard IP
    let _ = Command::new("ip")
        .args(["addr", "add", &wg.server_wg_ip, "dev", iface])
        .status();

    // Bring interface up
    let _ = Command::new("ip")
        .args(["link", "set", iface, "up"])
        .status();

    // Re-add any previously registered peers from DB
    let conn = open_db();
    let mut stmt = conn.prepare(
        "SELECT public_key, assigned_ip FROM wg_peers"
    ).unwrap();
    let peers: Vec<(String, String)> = stmt.query_map([], |r| {
        Ok((r.get(0)?, r.get(1)?))
    }).unwrap().filter_map(|r| r.ok()).collect();

    for (pubkey, ip) in peers {
        let allowed = format!("{}/32", ip);
        let _ = Command::new("wg")
            .args(["set", iface, "peer", &pubkey, "allowed-ips", &allowed])
            .status();
    }

    eprintln!("[wg] interface {} is up ({})", iface, wg.server_wg_ip);
}

fn wg_server_pubkey() -> String {
    fs::read_to_string(wg_pubkey_path())
        .unwrap_or_default()
        .trim()
        .to_string()
}

fn wg_next_ip(conn: &Connection) -> Option<String> {
    let pool = &CONFIG.wireguard.worker_ip_pool;
    let base = pool.split('/').next()?;
    let parts: Vec<u8> = base.split('.').filter_map(|p| p.parse().ok()).collect();
    if parts.len() != 4 { return None; }

    let mut stmt = conn.prepare("SELECT assigned_ip FROM wg_peers").ok()?;
    let existing: std::collections::HashSet<String> = stmt
        .query_map([], |r| r.get::<_, String>(0))
        .ok()?
        .filter_map(|r| r.ok())
        .collect();

    for i in 2u8..=254 {
        let ip = format!("{}.{}.{}.{}", parts[0], parts[1], parts[2], i);
        if !existing.contains(&ip) {
            return Some(ip);
        }
    }
    None
}

fn wg_register_worker(name: &str, pubkey: &str) -> Result<String, String> {
    if !wg_enabled() {
        return Err("WireGuard is not enabled on this orchestrator".to_string());
    }
    let conn = open_db();
    let ip = wg_next_ip(&conn).ok_or("IP pool exhausted")?;
    let allowed = format!("{}/32", ip);

    // Add peer to live WireGuard interface
    let status = Command::new("wg")
        .args(["set", &CONFIG.wireguard.interface,
               "peer", pubkey,
               "allowed-ips", &allowed])
        .status();
    if status.map(|s| !s.success()).unwrap_or(true) {
        return Err("wg set peer failed — is wireguard-tools installed?".to_string());
    }

    // Persist to DB
    conn.execute(
        "INSERT INTO wg_peers (worker_name, public_key, assigned_ip, added_at)
         VALUES (?1, ?2, ?3, ?4)
         ON CONFLICT(public_key) DO UPDATE SET worker_name=?1, assigned_ip=assigned_ip",
        params![name, pubkey, ip, now()]
    ).map_err(|e| e.to_string())?;

    Ok(ip)
}

// ── Daemon ────────────────────────────────────────────────────────────────────

pub fn run_daemon() {
    eprintln!("[smech-farmd] starting — polling every {}s", DAEMON_POLL_S);
    loop {
        let conn = open_db();
        process_package_queue(&conn);
        push_packages_to_workers(&conn);
        expire_stale_workers(&conn);
        drop(conn);
        thread::sleep(Duration::from_secs(DAEMON_POLL_S));
    }
}

fn process_package_queue(conn: &Connection) {
    let mut stmt = conn.prepare(
        "SELECT id, package_name, version FROM package_queue WHERE status = 'pending' LIMIT 20"
    ).unwrap();
    let rows: Vec<(i64, String, String)> = stmt.query_map([], |r| {
        Ok((r.get(0)?, r.get(1)?, r.get(2)?))
    }).unwrap().filter_map(|r| r.ok()).collect();

    for (id, pkg, ver) in rows {
        eprintln!("[smech-farmd] downloading package: {} {}", pkg, ver);
        conn.execute(
            "UPDATE package_queue SET status = 'downloading' WHERE id = ?1",
            params![id]
        ).ok();
        let ok = download_package_to_nfs(&pkg, &ver);
        let status = if ok { "available" } else { "failed" };
        conn.execute(
            "UPDATE package_queue SET status = ?1, completed_at = ?2 WHERE id = ?3",
            params![status, now(), id]
        ).ok();
        if ok {
            eprintln!("[smech-farmd] package {} now on NFS share", pkg);
        } else {
            eprintln!("[smech-farmd] failed to download {}", pkg);
        }
    }
}

fn download_package_to_nfs(pkg: &str, _ver: &str) -> bool {
    fs::create_dir_all(NFS_PKG_PATH).ok();
    let urls = [
        format!("https://download.kde.org/stable/frameworks/6.27/{}-6.27.0.tar.xz", pkg),
        format!("https://download.kde.org/stable/plasma/6.7.2/{}-6.7.2.tar.xz", pkg),
        format!("https://github.com/{}/{}/releases/latest/download/{}.tar.xz", pkg, pkg, pkg),
    ];
    let dest = format!("{}/{}.tar.xz", NFS_PKG_PATH, pkg);
    for url in &urls {
        let status = Command::new("curl")
            .args(["-sfL", "--max-time", "120", "-o", &dest, url])
            .status();
        if let Ok(s) = status {
            if s.success() && Path::new(&dest).exists() {
                return true;
            }
        }
    }
    false
}

fn push_packages_to_workers(conn: &Connection) {
    let count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM package_queue WHERE status = 'available'",
        [], |r| r.get(0)
    ).unwrap_or(0);
    if count > 0 {
        let flag = format!("{}/PACKAGES_READY", NFS_PKG_PATH);
        fs::write(&flag, now().to_string()).ok();
    }
}

fn expire_stale_workers(conn: &Connection) {
    let cutoff = now() - 30;
    conn.execute(
        "UPDATE workers SET status = 'offline' WHERE last_seen < ?1 AND status = 'online'",
        params![cutoff]
    ).ok();
}

// ── Shell ─────────────────────────────────────────────────────────────────────

pub fn run_shell() {
    println!("smech-farm v{} — {} shell", VERSION, CONFIG.farm.name);
    println!("Type 'help' for available commands.\n");
    let stdin = io::stdin();
    loop {
        print!("smech-farm> ");
        io::stdout().flush().ok();
        let mut line = String::new();
        match stdin.lock().read_line(&mut line) {
            Ok(0) | Err(_) => break,
            _ => {}
        }
        let line = line.trim();
        if line.is_empty() { continue; }
        let parts: Vec<&str> = line.splitn(4, ' ').collect();
        match parts.as_slice() {
            ["exit"] | ["quit"] => break,
            ["help"] => print_help(),
            ["status"] => cmd_status(),
            ["workers"] => cmd_workers(),
            ["queue"] => cmd_queue(),
            ["jobs"] => cmd_jobs(),
            ["push", "compile", file] => cmd_push_compile(file, "shell"),
            ["sync-network-gateway"] => cmd_sync_gateway("shell"),
            ["download-queued-packages"] => cmd_download_queued(),
            ["push-packages"] => cmd_push_packages(),
            ["orchestrator-shell"] => cmd_orchestrator_shell(),
            ["wg-status"] => cmd_wg_status(),
            ["wg-list-peers"] => cmd_wg_list_peers(),
            ["wg-add-worker", name, pubkey] => cmd_wg_add_worker(name, pubkey),
            ["clear"] => { print!("\x1b[2J\x1b[H"); io::stdout().flush().ok(); }
            _ => println!("Unknown command: '{}'. Type 'help'.", line),
        }
    }
}

fn print_help() {
    let wg_section = if wg_enabled() {
        "
  wg-status                  Show WireGuard interface status
  wg-list-peers              List registered WireGuard worker peers
  wg-add-worker <name> <pubkey>  Register a new WireGuard worker peer"
    } else {
        "\n  (WireGuard not enabled — set wireguard.enabled = true in smech-farm.toml)"
    };
    println!("
  push compile <file>        Push a compile job to the farm
  sync-network-gateway       Sync DNS and upstream/downstream connections
  download-queued-packages   Force-process the package download queue
  push-packages              Force-push NFS packages to workers
  orchestrator-shell         Drop into the host OS shell (privileged)
  status                     Show farm status summary
  workers                    List registered workers
  queue                      Show package download queue
  jobs                       Show compile job queue
  clear                      Clear terminal
  help                       Show this help
  exit                       Exit the shell
{}
", wg_section);
}

fn api_get(path: &str) -> Option<String> {
    let url = format!("http://127.0.0.1:{}{}", API_PORT, path);
    let output = Command::new("curl").args(["-sf", &url]).output().ok()?;
    String::from_utf8(output.stdout).ok()
}

fn api_post(path: &str, body: &str) -> Option<String> {
    let url = format!("http://127.0.0.1:{}{}", API_PORT, path);
    let output = Command::new("curl")
        .args(["-sf", "-X", "POST", "-H", "Content-Type: application/json", "-d", body, &url])
        .output().ok()?;
    String::from_utf8(output.stdout).ok()
}

fn cmd_status() {
    match api_get("/api/status") {
        Some(j) => pretty_print_json(&j),
        None    => println!("  [!] Could not reach API"),
    }
}

fn cmd_workers() {
    match api_get("/api/workers") {
        Some(j) => pretty_print_json(&j),
        None    => println!("  [!] Could not reach API"),
    }
}

fn cmd_queue() {
    match api_get("/api/queue") {
        Some(j) => pretty_print_json(&j),
        None    => println!("  [!] Could not reach API"),
    }
}

fn cmd_jobs() {
    match api_get("/api/jobs") {
        Some(j) => pretty_print_json(&j),
        None    => println!("  [!] Could not reach API"),
    }
}

fn cmd_push_compile(file: &str, submitted_by: &str) {
    let path = Path::new(file);
    if !path.exists() {
        println!("  [!] File not found: {}", file);
        return;
    }
    let content = match fs::read(path) {
        Ok(b) => b,
        Err(e) => { println!("  [!] Cannot read file: {}", e); return; }
    };
    let encoded = base64_encode(&content);
    let filename = path.file_name().unwrap_or_default().to_string_lossy();
    let body = format!(
        r#"{{"filename":"{}","content":"{}","submitted_by":"{}"}}"#,
        filename, encoded, submitted_by
    );
    match api_post("/api/jobs/submit", &body) {
        Some(r) => println!("  [+] Job submitted: {}", r.trim()),
        None    => println!("  [!] Could not submit job"),
    }
}

fn cmd_sync_gateway(triggered_by: &str) {
    println!("  [*] Syncing network gateway...");
    let body = format!(r#"{{"triggered_by":"{}"}}"#, triggered_by);
    match api_post("/api/gateway/sync", &body) {
        Some(r) => println!("  [+] {}", r.trim()),
        None    => run_gateway_sync(triggered_by),
    }
}

fn cmd_download_queued() {
    println!("  [*] Processing package download queue...");
    match api_post("/api/queue/process", "{}") {
        Some(r) => println!("  [+] {}", r.trim()),
        None    => {
            let conn = open_db();
            process_package_queue(&conn);
            println!("  [+] Queue processed (direct)");
        }
    }
}

fn cmd_push_packages() {
    println!("  [*] Pushing packages to workers...");
    match api_post("/api/packages/push", "{}") {
        Some(r) => println!("  [+] {}", r.trim()),
        None    => {
            let conn = open_db();
            push_packages_to_workers(&conn);
            println!("  [+] Push signal written (direct)");
        }
    }
}

fn cmd_orchestrator_shell() {
    println!("  [!] Entering host OS shell. This session is logged.");
    let conn = open_db();
    conn.execute(
        "INSERT INTO gateway_sync_log (triggered_by, started_at, result) VALUES (?1, ?2, 'orchestrator-shell-access')",
        params!["orchestrator-shell", now()]
    ).ok();
    drop(conn);
    let shell = std::env::var("HOST_SHELL").unwrap_or_else(|_| "/bin/bash".to_string());
    let _ = Command::new("nsenter")
        .args(["--target", "1", "--mount", "--uts", "--ipc", "--net", "--pid", "--", &shell])
        .status()
        .or_else(|_| Command::new(&shell).status());
}

fn cmd_wg_status() {
    if !wg_enabled() {
        println!("  [!] WireGuard is not enabled. Set wireguard.enabled = true in smech-farm.toml");
        return;
    }
    println!("  WireGuard interface : {}", CONFIG.wireguard.interface);
    println!("  Server WireGuard IP : {}", CONFIG.wireguard.server_wg_ip);
    println!("  Listen port         : {}", CONFIG.wireguard.listen_port);
    println!("  Server endpoint     : {}", CONFIG.wireguard.server_endpoint);
    println!("  Server public key   : {}", wg_server_pubkey());
    println!();
    let _ = Command::new("wg").args(["show", &CONFIG.wireguard.interface]).status();
}

fn cmd_wg_list_peers() {
    if !wg_enabled() {
        println!("  [!] WireGuard is not enabled.");
        return;
    }
    let conn = open_db();
    let mut stmt = conn.prepare(
        "SELECT worker_name, assigned_ip, public_key, added_at FROM wg_peers ORDER BY added_at"
    ).unwrap();
    let peers: Vec<(String, String, String, i64)> = stmt.query_map([], |r| {
        Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?))
    }).unwrap().filter_map(|r| r.ok()).collect();

    if peers.is_empty() {
        println!("  No WireGuard peers registered.");
        return;
    }
    println!("  {:20} {:16} {}", "Name", "IP", "Public Key");
    println!("  {}", "-".repeat(80));
    for (name, ip, key, _) in peers {
        println!("  {:20} {:16} {}", name, ip, &key[..32.min(key.len())]);
    }
}

fn cmd_wg_add_worker(name: &str, pubkey: &str) {
    match wg_register_worker(name, pubkey) {
        Ok(ip) => {
            println!("  [+] Worker '{}' registered", name);
            println!("  Assigned WireGuard IP : {}", ip);
            println!("  Server public key     : {}", wg_server_pubkey());
            println!("  Server endpoint       : {}", CONFIG.wireguard.server_endpoint);
            println!("  Server WireGuard IP   : {}", CONFIG.wireguard.server_wg_ip.split('/').next().unwrap_or(""));
            println!();
            println!("  Worker WireGuard config:");
            println!("  ─────────────────────────────────────────");
            println!("  [Interface]");
            println!("  Address    = {}/32", ip);
            println!("  PrivateKey = <worker private key here>");
            println!("  DNS        = {}", CONFIG.wireguard.server_wg_ip.split('/').next().unwrap_or(""));
            println!();
            println!("  [Peer]");
            println!("  PublicKey  = {}", wg_server_pubkey());
            println!("  Endpoint   = {}", CONFIG.wireguard.server_endpoint);
            println!("  AllowedIPs = {}", CONFIG.wireguard.server_wg_ip.split('/').next()
                .map(|ip| {
                    let parts: Vec<&str> = ip.split('.').collect();
                    if parts.len() == 4 {
                        format!("{}.{}.{}.0/24", parts[0], parts[1], parts[2])
                    } else { "10.99.0.0/24".to_string() }
                }).unwrap_or_else(|| "10.99.0.0/24".to_string()));
            println!("  PersistentKeepalive = 25");
            println!("  ─────────────────────────────────────────");
        }
        Err(e) => println!("  [!] Failed to register worker: {}", e),
    }
}

// ── API / Web UI ──────────────────────────────────────────────────────────────

#[derive(Serialize, Deserialize)]
struct QueueRequest { package_name: String, version: Option<String>, worker_id: String }

#[derive(Serialize, Deserialize)]
struct JobSubmit { filename: String, content: String, submitted_by: String }

#[derive(Serialize, Deserialize)]
struct WorkerHeartbeat { id: String, hostname: String, ip_addr: String, cores: u32 }

#[derive(Serialize, Deserialize)]
struct GatewaySyncReq { triggered_by: String }

#[derive(Deserialize)]
struct WgRegisterReq { name: String, public_key: String }

pub fn run_serve(db: Arc<Mutex<()>>) {
    let server = Server::http(format!("0.0.0.0:{}", API_PORT))
        .expect("cannot bind API port");
    eprintln!("[smech-farm API] listening on :{}", API_PORT);
    for req in server.incoming_requests() {
        let _guard = db.lock();
        handle_request(req);
    }
}

fn handle_request(mut req: tiny_http::Request) {
    let url    = req.url().to_string();
    let method = req.method().to_string();

    let (status, body) = match (method.as_str(), url.as_str()) {
        ("GET",  "/api/status")               => api_status(),
        ("GET",  "/api/workers")              => api_workers(),
        ("GET",  "/api/queue")                => api_queue(),
        ("GET",  "/api/jobs")                 => api_jobs(),
        ("GET",  "/api/wg/server-info")       => api_wg_server_info(),
        ("GET",  "/api/wg/peers")             => api_wg_peers(),
        ("POST", "/api/queue/add")            => {
            let mut buf = String::new();
            req.as_reader().read_to_string(&mut buf).ok();
            api_queue_add(&buf)
        }
        ("POST", "/api/queue/process")        => {
            let conn = open_db();
            process_package_queue(&conn);
            push_packages_to_workers(&conn);
            (200, r#"{"ok":true,"msg":"queue processed"}"#.to_string())
        }
        ("POST", "/api/jobs/submit")          => {
            let mut buf = String::new();
            req.as_reader().read_to_string(&mut buf).ok();
            api_job_submit(&buf)
        }
        ("POST", "/api/packages/push")        => {
            let conn = open_db();
            push_packages_to_workers(&conn);
            (200, r#"{"ok":true,"msg":"push signal written"}"#.to_string())
        }
        ("POST", "/api/gateway/sync")         => {
            let mut buf = String::new();
            req.as_reader().read_to_string(&mut buf).ok();
            let by = serde_json::from_str::<GatewaySyncReq>(&buf)
                .map(|r| r.triggered_by).unwrap_or_else(|_| "api".to_string());
            run_gateway_sync(&by);
            (200, r#"{"ok":true,"msg":"gateway sync complete"}"#.to_string())
        }
        ("POST", "/api/workers/heartbeat")    => {
            let mut buf = String::new();
            req.as_reader().read_to_string(&mut buf).ok();
            api_worker_heartbeat(&buf)
        }
        ("POST", "/api/wg/register-worker")   => {
            let mut buf = String::new();
            req.as_reader().read_to_string(&mut buf).ok();
            api_wg_register_worker(&buf)
        }
        ("GET",  "/ui") | ("GET", "/")        => (200, webui_html()),
        _                                     => (404, r#"{"error":"not found"}"#.to_string()),
    };

    let content_type = if url.starts_with("/ui") || url == "/" {
        "text/html; charset=utf-8"
    } else {
        "application/json"
    };
    let response = Response::from_string(body)
        .with_status_code(status)
        .with_header(tiny_http::Header::from_bytes(
            b"Content-Type", content_type.as_bytes()
        ).unwrap());
    let _ = req.respond(response);
}

fn api_status() -> (i32, String) {
    let conn = open_db();
    let workers:   i64 = conn.query_row("SELECT COUNT(*) FROM workers WHERE status='online'", [], |r| r.get(0)).unwrap_or(0);
    let pending:   i64 = conn.query_row("SELECT COUNT(*) FROM package_queue WHERE status='pending'", [], |r| r.get(0)).unwrap_or(0);
    let jobs:      i64 = conn.query_row("SELECT COUNT(*) FROM compile_jobs WHERE status='queued'", [], |r| r.get(0)).unwrap_or(0);
    let done_jobs: i64 = conn.query_row("SELECT COUNT(*) FROM compile_jobs WHERE status='done'", [], |r| r.get(0)).unwrap_or(0);
    let wg_peers:  i64 = if wg_enabled() {
        conn.query_row("SELECT COUNT(*) FROM wg_peers", [], |r| r.get(0)).unwrap_or(0)
    } else { -1 };
    (200, format!(
        r#"{{"version":"{}","online_workers":{},"pending_packages":{},"queued_jobs":{},"completed_jobs":{},"wg_peers":{}}}"#,
        VERSION, workers, pending, jobs, done_jobs, wg_peers
    ))
}

fn api_workers() -> (i32, String) {
    let conn = open_db();
    let mut stmt = conn.prepare(
        "SELECT id, hostname, ip_addr, cores, last_seen, status FROM workers ORDER BY hostname"
    ).unwrap();
    let rows: Vec<String> = stmt.query_map([], |r| {
        Ok(format!(
            r#"{{"id":"{}","hostname":"{}","ip":"{}","cores":{},"last_seen":{},"status":"{}"}}"#,
            r.get::<_,String>(0)?, r.get::<_,String>(1)?, r.get::<_,String>(2)?,
            r.get::<_,i64>(3)?, r.get::<_,i64>(4)?, r.get::<_,String>(5)?
        ))
    }).unwrap().filter_map(|r| r.ok()).collect();
    (200, format!("[{}]", rows.join(",")))
}

fn api_queue() -> (i32, String) {
    let conn = open_db();
    let mut stmt = conn.prepare(
        "SELECT id, package_name, version, worker_id, status, requested_at FROM package_queue ORDER BY requested_at DESC LIMIT 100"
    ).unwrap();
    let rows: Vec<String> = stmt.query_map([], |r| {
        Ok(format!(
            r#"{{"id":{},"package":"{}","version":"{}","worker":"{}","status":"{}","requested_at":{}}}"#,
            r.get::<_,i64>(0)?, r.get::<_,String>(1)?, r.get::<_,String>(2)?,
            r.get::<_,String>(3)?, r.get::<_,String>(4)?, r.get::<_,i64>(5)?
        ))
    }).unwrap().filter_map(|r| r.ok()).collect();
    (200, format!("[{}]", rows.join(",")))
}

fn api_jobs() -> (i32, String) {
    let conn = open_db();
    let mut stmt = conn.prepare(
        "SELECT id, filename, submitted_by, status, submitted_at, worker_id FROM compile_jobs ORDER BY submitted_at DESC LIMIT 100"
    ).unwrap();
    let rows: Vec<String> = stmt.query_map([], |r| {
        let wid: Option<String> = r.get(5).ok();
        Ok(format!(
            r#"{{"id":{},"filename":"{}","submitted_by":"{}","status":"{}","submitted_at":{},"worker":{}}}"#,
            r.get::<_,i64>(0)?, r.get::<_,String>(1)?, r.get::<_,String>(2)?,
            r.get::<_,String>(3)?, r.get::<_,i64>(4)?,
            wid.map(|w| format!("\"{}\"", w)).unwrap_or_else(|| "null".to_string())
        ))
    }).unwrap().filter_map(|r| r.ok()).collect();
    (200, format!("[{}]", rows.join(",")))
}

fn api_queue_add(body: &str) -> (i32, String) {
    let req: QueueRequest = match serde_json::from_str(body) {
        Ok(r) => r,
        Err(_) => return (400, r#"{"error":"bad request"}"#.to_string()),
    };
    let conn = open_db();
    let existing: i64 = conn.query_row(
        "SELECT COUNT(*) FROM package_queue WHERE package_name=?1 AND worker_id=?2 AND status IN ('pending','downloading','available')",
        params![req.package_name, req.worker_id], |r| r.get(0)
    ).unwrap_or(0);
    if existing > 0 {
        return (200, r#"{"ok":true,"msg":"already queued"}"#.to_string());
    }
    conn.execute(
        "INSERT INTO package_queue (package_name, version, worker_id, requested_at) VALUES (?1,?2,?3,?4)",
        params![req.package_name, req.version.unwrap_or_default(), req.worker_id, now()]
    ).unwrap();
    (200, r#"{"ok":true,"msg":"queued"}"#.to_string())
}

fn api_job_submit(body: &str) -> (i32, String) {
    let req: JobSubmit = match serde_json::from_str(body) {
        Ok(r) => r,
        Err(_) => return (400, r#"{"error":"bad request"}"#.to_string()),
    };
    let content = base64_decode(&req.content);
    let conn = open_db();
    conn.execute(
        "INSERT INTO compile_jobs (filename, content, submitted_by, submitted_at) VALUES (?1,?2,?3,?4)",
        params![req.filename, content, req.submitted_by, now()]
    ).unwrap();
    let id: i64 = conn.last_insert_rowid();
    let jobs_dir = format!("{}/jobs", NFS_PKG_PATH);
    fs::create_dir_all(&jobs_dir).ok();
    fs::write(format!("{}/{}-{}", jobs_dir, id, req.filename), &content).ok();
    (200, format!(r#"{{"ok":true,"job_id":{}}}"#, id))
}

fn api_worker_heartbeat(body: &str) -> (i32, String) {
    let hb: WorkerHeartbeat = match serde_json::from_str(body) {
        Ok(r) => r,
        Err(_) => return (400, r#"{"error":"bad request"}"#.to_string()),
    };
    let conn = open_db();
    conn.execute(
        "INSERT INTO workers (id, hostname, ip_addr, cores, last_seen, status)
         VALUES (?1,?2,?3,?4,?5,'online')
         ON CONFLICT(id) DO UPDATE SET hostname=?2, ip_addr=?3, cores=?4, last_seen=?5, status='online'",
        params![hb.id, hb.hostname, hb.ip_addr, hb.cores, now()]
    ).unwrap();
    (200, r#"{"ok":true}"#.to_string())
}

fn api_wg_server_info() -> (i32, String) {
    if !wg_enabled() {
        return (404, r#"{"error":"WireGuard not enabled"}"#.to_string());
    }
    (200, format!(
        r#"{{"enabled":true,"public_key":"{}","endpoint":"{}","server_ip":"{}","listen_port":{}}}"#,
        wg_server_pubkey(),
        CONFIG.wireguard.server_endpoint,
        CONFIG.wireguard.server_wg_ip,
        CONFIG.wireguard.listen_port
    ))
}

fn api_wg_peers() -> (i32, String) {
    if !wg_enabled() {
        return (404, r#"{"error":"WireGuard not enabled"}"#.to_string());
    }
    let conn = open_db();
    let mut stmt = conn.prepare(
        "SELECT worker_name, assigned_ip, public_key, added_at FROM wg_peers ORDER BY added_at"
    ).unwrap();
    let rows: Vec<String> = stmt.query_map([], |r| {
        Ok(format!(
            r#"{{"name":"{}","ip":"{}","public_key":"{}","added_at":{}}}"#,
            r.get::<_,String>(0)?, r.get::<_,String>(1)?,
            r.get::<_,String>(2)?, r.get::<_,i64>(3)?
        ))
    }).unwrap().filter_map(|r| r.ok()).collect();
    (200, format!("[{}]", rows.join(",")))
}

fn api_wg_register_worker(body: &str) -> (i32, String) {
    let req: WgRegisterReq = match serde_json::from_str(body) {
        Ok(r) => r,
        Err(_) => return (400, r#"{"error":"bad request — expected {\"name\":\"...\",\"public_key\":\"...\"}"}"#.to_string()),
    };
    match wg_register_worker(&req.name, &req.public_key) {
        Ok(ip) => {
            let server_ip = CONFIG.wireguard.server_wg_ip.split('/').next().unwrap_or("10.99.0.1");
            let pool_net  = {
                let parts: Vec<&str> = server_ip.split('.').collect();
                if parts.len() == 4 { format!("{}.{}.{}.0/24", parts[0], parts[1], parts[2]) }
                else { "10.99.0.0/24".to_string() }
            };
            (200, format!(
                r#"{{"ok":true,"assigned_ip":"{}","server_public_key":"{}","server_endpoint":"{}","server_wg_ip":"{}","allowed_ips":"{}","keepalive":25}}"#,
                ip,
                wg_server_pubkey(),
                CONFIG.wireguard.server_endpoint,
                server_ip,
                pool_net
            ))
        }
        Err(e) => (500, format!(r#"{{"error":"{}"}}"#, e)),
    }
}

fn run_gateway_sync(triggered_by: &str) {
    eprintln!("[smech-farm] gateway sync triggered by {}", triggered_by);
    let conn = open_db();
    conn.execute(
        "INSERT INTO gateway_sync_log (triggered_by, started_at) VALUES (?1, ?2)",
        params![triggered_by, now()]
    ).ok();
    let id = conn.last_insert_rowid();
    drop(conn);
    let _ = Command::new("resolvectl").arg("flush-caches").status();
    let _ = Command::new("ip").args(["route", "flush", "cache"]).status();
    let _ = Command::new("touch").arg(format!("{}/.sync", NFS_PKG_PATH)).status();
    let conn = open_db();
    conn.execute(
        "UPDATE gateway_sync_log SET result = 'ok' WHERE id = ?1",
        params![id]
    ).ok();
    eprintln!("[smech-farm] gateway sync complete");
}

// ── Web UI ────────────────────────────────────────────────────────────────────

fn webui_html() -> String {
    let cfg      = &CONFIG.farm;
    let name     = &cfg.name;
    let accent   = &cfg.accent_color;
    let tagline  = &cfg.tagline;
    let logo_html = if cfg.logo_url.is_empty() {
        format!(r#"<span class="logo-icon">&#9881;</span> <span class="logo-text">{}</span>"#, name)
    } else {
        format!(r#"<img src="{}" alt="{}" class="logo-img"> <span class="logo-text">{}</span>"#, cfg.logo_url, name, name)
    };
    let wg_section = if wg_enabled() {
        r#"
  <section>
    <h2>WireGuard Peers</h2>
    <table><thead><tr><th>Name</th><th>Assigned IP</th><th>Public Key</th><th>Added</th></tr></thead>
    <tbody id="wg-body"></tbody></table>
  </section>"#
    } else { "" };
    let wg_js = if wg_enabled() {
        r#"
  if (document.getElementById('wg-body')) {
    fetch('/api/wg/peers').then(r=>r.json()).then(peers=>{
      document.getElementById('wg-body').innerHTML = peers.map(p=>
        `<tr><td>${p.name}</td><td class="ts">${p.ip}</td><td class="ts">${p.public_key.slice(0,24)}...</td><td class="ts">${ts(p.added_at)}</td></tr>`
      ).join('') || '<tr><td colspan="4" style="color:var(--muted);text-align:center">No WireGuard peers registered</td></tr>';
    }).catch(()=>{});
  }"#
    } else { "" };

    format!(r#"<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>{name} Dashboard</title>
<style>
:root {{
  --bg: #0d1117; --surface: #161b22; --border: #30363d;
  --text: #e6edf3; --muted: #8b949e; --green: #3fb950;
  --yellow: #d29922; --red: #f85149; --blue: {accent};
  --accent: {accent}; --font: 'SF Mono','Fira Code',monospace;
}}
* {{ box-sizing: border-box; margin: 0; padding: 0; }}
body {{ background: var(--bg); color: var(--text); font-family: var(--font); font-size: 13px; }}
header {{
  background: var(--surface); border-bottom: 1px solid var(--border);
  padding: 12px 24px; display: flex; align-items: center; gap: 16px;
}}
.logo-icon {{ font-size: 18px; }}
.logo-img  {{ height: 28px; object-fit: contain; }}
.logo-text {{ font-size: 16px; font-weight: 600; color: var(--accent); }}
header .tagline {{ color: var(--muted); font-size: 11px; }}
header .ver {{ color: var(--muted); font-size: 11px; margin-left: auto; }}
.badge {{
  padding: 2px 8px; border-radius: 12px; font-size: 11px; font-weight: 600;
}}
.badge.green  {{ background: #1a3a22; color: var(--green);  border: 1px solid var(--green); }}
.badge.yellow {{ background: #2d2208; color: var(--yellow); border: 1px solid var(--yellow); }}
.badge.red    {{ background: #2d0e0e; color: var(--red);    border: 1px solid var(--red); }}
.badge.blue   {{ background: #0d2149; color: var(--blue);   border: 1px solid var(--blue); }}
main {{ padding: 24px; display: grid; gap: 20px; }}
.cards {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(160px,1fr)); gap: 12px; }}
.card {{
  background: var(--surface); border: 1px solid var(--border);
  border-radius: 8px; padding: 16px;
}}
.card .label {{ color: var(--muted); font-size: 11px; text-transform: uppercase; letter-spacing: .05em; }}
.card .value {{ font-size: 28px; font-weight: 700; margin-top: 4px; color: var(--accent); }}
section {{ background: var(--surface); border: 1px solid var(--border); border-radius: 8px; overflow: hidden; }}
section h2 {{ padding: 12px 16px; font-size: 13px; border-bottom: 1px solid var(--border); color: var(--muted); text-transform: uppercase; letter-spacing: .06em; }}
table {{ width: 100%; border-collapse: collapse; }}
th, td {{ padding: 9px 16px; text-align: left; border-bottom: 1px solid var(--border); }}
th {{ color: var(--muted); font-size: 11px; font-weight: 400; text-transform: uppercase; }}
tr:last-child td {{ border-bottom: none; }}
tr:hover td {{ background: rgba(255,255,255,.02); }}
.ts {{ color: var(--muted); font-size: 11px; }}
.refresh {{ background: none; border: 1px solid var(--border);
  color: var(--muted); padding: 4px 12px; border-radius: 6px; cursor: pointer; font: inherit; }}
.refresh:hover {{ color: var(--text); border-color: var(--accent); }}
</style>
</head>
<body>
<header>
  {logo_html}
  <span class="tagline">{tagline}</span>
  <span class="ver">v{ver} &nbsp; <button class="refresh" onclick="load()">&#8635; Refresh</button></span>
</header>
<main>
  <div class="cards" id="cards">
    <div class="card"><div class="label">Online Workers</div><div class="value" id="c-workers">—</div></div>
    <div class="card"><div class="label">Pending Packages</div><div class="value" id="c-pkgs">—</div></div>
    <div class="card"><div class="label">Queued Jobs</div><div class="value" id="c-jobs">—</div></div>
    <div class="card"><div class="label">Completed Jobs</div><div class="value" id="c-done">—</div></div>
  </div>
  <section>
    <h2>Workers</h2>
    <table><thead><tr><th>ID</th><th>Hostname</th><th>IP</th><th>Cores</th><th>Status</th><th>Last Seen</th></tr></thead>
    <tbody id="workers-body"></tbody></table>
  </section>
  <section>
    <h2>Package Queue</h2>
    <table><thead><tr><th>#</th><th>Package</th><th>Version</th><th>Worker</th><th>Status</th><th>Requested</th></tr></thead>
    <tbody id="queue-body"></tbody></table>
  </section>
  <section>
    <h2>Compile Jobs</h2>
    <table><thead><tr><th>#</th><th>File</th><th>Submitted By</th><th>Status</th><th>Worker</th><th>Submitted</th></tr></thead>
    <tbody id="jobs-body"></tbody></table>
  </section>
  {wg_section}
</main>
<script>
const badge = (s) => {{
  const cls = {{online:'green',available:'green',done:'green',pending:'yellow',downloading:'yellow',queued:'blue',failed:'red',offline:'red'}}[s]||'blue';
  return `<span class="badge ${{cls}}">${{s}}</span>`;
}};
const ts = (t) => new Date(t*1000).toLocaleString();
async function load() {{
  const [status, workers, queue, jobs] = await Promise.all([
    fetch('/api/status').then(r=>r.json()),
    fetch('/api/workers').then(r=>r.json()),
    fetch('/api/queue').then(r=>r.json()),
    fetch('/api/jobs').then(r=>r.json()),
  ]).catch(()=>[null,[],[],[]]);
  if (status) {{
    document.getElementById('c-workers').textContent = status.online_workers;
    document.getElementById('c-pkgs').textContent    = status.pending_packages;
    document.getElementById('c-jobs').textContent    = status.queued_jobs;
    document.getElementById('c-done').textContent    = status.completed_jobs;
  }}
  document.getElementById('workers-body').innerHTML = workers.map(w=>
    `<tr><td class="ts">${{w.id}}</td><td>${{w.hostname}}</td><td class="ts">${{w.ip}}</td><td>${{w.cores}}</td><td>${{badge(w.status)}}</td><td class="ts">${{ts(w.last_seen)}}</td></tr>`
  ).join('') || '<tr><td colspan="6" style="color:var(--muted);text-align:center">No workers registered</td></tr>';
  document.getElementById('queue-body').innerHTML = queue.map(q=>
    `<tr><td class="ts">${{q.id}}</td><td>${{q.package}}</td><td class="ts">${{q.version||'—'}}</td><td class="ts">${{q.worker}}</td><td>${{badge(q.status)}}</td><td class="ts">${{ts(q.requested_at)}}</td></tr>`
  ).join('') || '<tr><td colspan="6" style="color:var(--muted);text-align:center">Queue empty</td></tr>';
  document.getElementById('jobs-body').innerHTML = jobs.map(j=>
    `<tr><td class="ts">${{j.id}}</td><td>${{j.filename}}</td><td class="ts">${{j.submitted_by}}</td><td>${{badge(j.status)}}</td><td class="ts">${{j.worker||'—'}}</td><td class="ts">${{ts(j.submitted_at)}}</td></tr>`
  ).join('') || '<tr><td colspan="6" style="color:var(--muted);text-align:center">No jobs submitted</td></tr>';
  {wg_js}
}}
load();
setInterval(load, 10000);
</script>
</body>
</html>"#,
        name    = name,
        accent  = accent,
        tagline = tagline,
        ver     = VERSION,
        logo_html   = logo_html,
        wg_section  = wg_section,
        wg_js       = wg_js,
    )
}

// ── Utilities ─────────────────────────────────────────────────────────────────

fn pretty_print_json(json: &str) {
    let mut indent = 0i32;
    let mut in_str = false;
    for c in json.chars() {
        match c {
            '"' => { in_str = !in_str; print!("{}", c); }
            '{' | '[' if !in_str => {
                indent += 1;
                println!("{}", c);
                print!("{}", "  ".repeat(indent as usize));
            }
            '}' | ']' if !in_str => {
                indent -= 1;
                println!();
                print!("{}", "  ".repeat(indent as usize));
                print!("{}", c);
            }
            ',' if !in_str => {
                println!("{}", c);
                print!("{}", "  ".repeat(indent as usize));
            }
            ':' if !in_str => print!(": "),
            _  => print!("{}", c),
        }
    }
    println!();
}

fn base64_encode(data: &[u8]) -> String {
    const CHARS: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::new();
    for chunk in data.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = if chunk.len() > 1 { chunk[1] as u32 } else { 0 };
        let b2 = if chunk.len() > 2 { chunk[2] as u32 } else { 0 };
        let n = (b0 << 16) | (b1 << 8) | b2;
        out.push(CHARS[((n >> 18) & 63) as usize] as char);
        out.push(CHARS[((n >> 12) & 63) as usize] as char);
        out.push(if chunk.len() > 1 { CHARS[((n >> 6) & 63) as usize] as char } else { '=' });
        out.push(if chunk.len() > 2 { CHARS[(n & 63) as usize] as char } else { '=' });
    }
    out
}

fn base64_decode(s: &str) -> Vec<u8> {
    let val = |c: u8| -> u8 {
        match c {
            b'A'..=b'Z' => c - b'A',
            b'a'..=b'z' => c - b'a' + 26,
            b'0'..=b'9' => c - b'0' + 52,
            b'+' => 62, b'/' => 63, _ => 0,
        }
    };
    let bytes: Vec<u8> = s.bytes().filter(|&b| b != b'=').collect();
    let mut out = Vec::new();
    for chunk in bytes.chunks(4) {
        let n = ((val(chunk[0]) as u32) << 18)
              | ((val(chunk[1]) as u32) << 12)
              | (if chunk.len() > 2 { (val(chunk[2]) as u32) << 6 } else { 0 })
              | (if chunk.len() > 3 { val(chunk[3]) as u32 } else { 0 });
        out.push(((n >> 16) & 0xff) as u8);
        if chunk.len() > 2 { out.push(((n >> 8) & 0xff) as u8); }
        if chunk.len() > 3 { out.push((n & 0xff) as u8); }
    }
    out
}

// ── Entry point ───────────────────────────────────────────────────────────────

fn main() {
    // Force config load and optional WireGuard setup at startup
    let _ = &*CONFIG;
    setup_wireguard();

    let cli = Cli::parse();
    match cli.command {
        Mode::Daemon => run_daemon(),
        Mode::Shell  => run_shell(),
        Mode::Serve  => {
            let lock = Arc::new(Mutex::new(()));
            run_serve(lock);
        }
        Mode::All => {
            thread::spawn(run_daemon);
            let lock  = Arc::new(Mutex::new(()));
            let lock2 = Arc::clone(&lock);
            thread::spawn(move || run_serve(lock2));
            run_shell();
        }
    }
}
