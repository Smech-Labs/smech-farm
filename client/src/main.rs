use clap::{Parser, Subcommand};
use std::fs;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::Path;
use std::process::Command;
use std::time::Duration;

const VERSION: &str = "1.0.0";
const ORCHESTRATOR_ENV: &str = "SMECH_ORCHESTRATOR";

#[derive(Parser)]
#[command(name = "smech-farm-client", version = VERSION,
          about = "SmechFarm client — submit jobs and manage the farm remotely")]
struct Cli {
    #[command(subcommand)]
    command: Cmd,
    /// Orchestrator address (host:port), overrides SMECH_ORCHESTRATOR env
    #[arg(long, global = true, default_value = "127.0.0.1:7734")]
    orchestrator: String,
}

#[derive(Subcommand)]
enum Cmd {
    /// Push a compile job to the farm
    Push {
        #[command(subcommand)]
        action: PushAction,
    },
    /// Sync the network gateway on the orchestrator
    SyncNetworkGateway,
    /// Show farm status (workers, queue, jobs)
    Status,
    /// Open the smech-farm interactive shell via SSH
    Shell {
        /// SSH host (defaults to orchestrator host)
        #[arg(long)]
        host: Option<String>,
        /// SSH user
        #[arg(long, default_value = "smech-farm")]
        user: String,
        /// SSH port
        #[arg(long, default_value = "22")]
        port: u16,
    },
    /// List workers
    Workers,
    /// Show the package download queue
    Queue,
    /// Show compile jobs
    Jobs,
    /// Download a package through the orchestrator queue
    DownloadPackage {
        package: String,
        #[arg(long)]
        version: Option<String>,
    },
}

#[derive(Subcommand)]
enum PushAction {
    /// Push a file as a compile job
    Compile {
        file: String,
        /// Label for this submission (defaults to username)
        #[arg(long)]
        label: Option<String>,
    },
}

fn orchestrator_addr(cli: &str) -> String {
    std::env::var(ORCHESTRATOR_ENV).unwrap_or_else(|_| cli.to_string())
}

// ── Minimal HTTP client ───────────────────────────────────────────────────────

fn http_post(addr: &str, path: &str, body: &str) -> Result<String, String> {
    let req = format!(
        "POST {} HTTP/1.0\r\nHost: {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
        path, addr, body.len(), body
    );
    let mut s = TcpStream::connect(addr).map_err(|e| e.to_string())?;
    s.set_write_timeout(Some(Duration::from_secs(10))).ok();
    s.set_read_timeout(Some(Duration::from_secs(30))).ok();
    s.write_all(req.as_bytes()).map_err(|e| e.to_string())?;
    let mut resp = String::new();
    s.read_to_string(&mut resp).map_err(|e| e.to_string())?;
    Ok(resp.splitn(2, "\r\n\r\n").nth(1).unwrap_or("").to_string())
}

fn http_get(addr: &str, path: &str) -> Result<String, String> {
    let req = format!("GET {} HTTP/1.0\r\nHost: {}\r\n\r\n", path, addr);
    let mut s = TcpStream::connect(addr).map_err(|e| e.to_string())?;
    s.set_read_timeout(Some(Duration::from_secs(10))).ok();
    s.write_all(req.as_bytes()).map_err(|e| e.to_string())?;
    let mut resp = String::new();
    s.read_to_string(&mut resp).map_err(|e| e.to_string())?;
    Ok(resp.splitn(2, "\r\n\r\n").nth(1).unwrap_or("").to_string())
}

// ── Base64 (stdlib only) ──────────────────────────────────────────────────────

fn base64_encode(data: &[u8]) -> String {
    const C: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::new();
    for chunk in data.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = if chunk.len() > 1 { chunk[1] as u32 } else { 0 };
        let b2 = if chunk.len() > 2 { chunk[2] as u32 } else { 0 };
        let n = (b0 << 16) | (b1 << 8) | b2;
        out.push(C[((n >> 18) & 63) as usize] as char);
        out.push(C[((n >> 12) & 63) as usize] as char);
        out.push(if chunk.len() > 1 { C[((n >> 6) & 63) as usize] as char } else { '=' });
        out.push(if chunk.len() > 2 { C[(n & 63) as usize] as char } else { '=' });
    }
    out
}

// ── Pretty printer ────────────────────────────────────────────────────────────

fn print_json_table(json: &str) {
    // For arrays, print each element on its own line, key: value format
    let trimmed = json.trim();
    if trimmed.starts_with('[') {
        // Split on },{
        let items: Vec<&str> = trimmed
            .trim_matches(|c| c == '[' || c == ']')
            .split("},{")
            .collect();
        for (i, item) in items.iter().enumerate() {
            if i > 0 { println!("  ---"); }
            let clean = item.trim_matches(|c| c == '{' || c == '}');
            for pair in clean.split(',') {
                let pair = pair.trim().trim_matches('"');
                let parts: Vec<&str> = pair.splitn(2, "\":").collect();
                if parts.len() == 2 {
                    let key = parts[0].trim_matches('"');
                    let val = parts[1].trim_matches('"').trim_matches('\\');
                    println!("  {:16} {}", format!("{}:", key), val);
                }
            }
        }
        if items.is_empty() || items[0].trim().is_empty() {
            println!("  (empty)");
        }
    } else {
        // Object: one key per line
        let clean = trimmed.trim_matches(|c| c == '{' || c == '}');
        for pair in clean.split(',') {
            let pair = pair.trim();
            let parts: Vec<&str> = pair.splitn(2, "\":").collect();
            if parts.len() == 2 {
                let key = parts[0].trim_matches('"').trim_matches('{');
                let val = parts[1].trim_matches('"');
                println!("  {:24} {}", format!("{}:", key), val);
            }
        }
    }
}

// ── Commands ──────────────────────────────────────────────────────────────────

fn cmd_push_compile(addr: &str, file: &str, label: Option<String>) {
    let path = Path::new(file);
    if !path.exists() {
        eprintln!("[-] File not found: {}", file);
        std::process::exit(1);
    }
    let content = match fs::read(path) {
        Ok(b)  => b,
        Err(e) => { eprintln!("[-] Cannot read {}: {}", file, e); std::process::exit(1); }
    };
    let submitter = label.unwrap_or_else(|| {
        std::env::var("USER").unwrap_or_else(|_| "client".to_string())
    });
    let filename = path.file_name().unwrap_or_default().to_string_lossy();
    let body = format!(
        r#"{{"filename":"{}","content":"{}","submitted_by":"{}"}}"#,
        filename, base64_encode(&content), submitter
    );
    println!("[*] Submitting compile job: {} ({} bytes)", filename, content.len());
    match http_post(addr, "/api/jobs/submit", &body) {
        Ok(r)  => println!("[+] {}", r.trim()),
        Err(e) => eprintln!("[-] Failed to submit job: {}", e),
    }
}

fn cmd_sync_gateway(addr: &str) {
    let user = std::env::var("USER").unwrap_or_else(|_| "client".to_string());
    println!("[*] Triggering gateway sync on orchestrator...");
    let body = format!(r#"{{"triggered_by":"{}"}}"#, user);
    match http_post(addr, "/api/gateway/sync", &body) {
        Ok(r)  => println!("[+] {}", r.trim()),
        Err(e) => eprintln!("[-] Failed: {}", e),
    }
}

fn cmd_status(addr: &str) {
    println!("\n  SmechFarm Status — {}\n", addr);
    match http_get(addr, "/api/status") {
        Ok(r) => { print_json_table(&r); println!(); }
        Err(e) => { eprintln!("  [-] Orchestrator unreachable: {}", e); return; }
    }
    println!("  Workers:");
    if let Ok(r) = http_get(addr, "/api/workers") { print_json_table(&r); }
    println!();
}

fn cmd_workers(addr: &str) {
    match http_get(addr, "/api/workers") {
        Ok(r)  => { println!("\n  Workers:\n"); print_json_table(&r); println!(); }
        Err(e) => eprintln!("[-] {}", e),
    }
}

fn cmd_queue(addr: &str) {
    match http_get(addr, "/api/queue") {
        Ok(r)  => { println!("\n  Package Queue:\n"); print_json_table(&r); println!(); }
        Err(e) => eprintln!("[-] {}", e),
    }
}

fn cmd_jobs(addr: &str) {
    match http_get(addr, "/api/jobs") {
        Ok(r)  => { println!("\n  Compile Jobs:\n"); print_json_table(&r); println!(); }
        Err(e) => eprintln!("[-] {}", e),
    }
}

fn cmd_shell(addr: &str, host: Option<String>, user: &str, port: u16) {
    let ssh_host = host.unwrap_or_else(|| {
        addr.split(':').next().unwrap_or("orchestrator").to_string()
    });
    println!("[*] Opening smech-farm shell on {}@{}:{}", user, ssh_host, port);
    let _ = Command::new("ssh")
        .args([
            "-p", &port.to_string(),
            &format!("{}@{}", user, ssh_host),
        ])
        .status();
}

fn cmd_download_package(addr: &str, package: &str, version: Option<String>) {
    let user = std::env::var("USER").unwrap_or_else(|_| "client".to_string());
    println!("[*] Requesting package '{}' via orchestrator...", package);
    let body = serde_json::json!({
        "package_name": package,
        "version":      version,
        "worker_id":    format!("client-{}", user),
    }).to_string();
    match http_post(addr, "/api/queue/add", &body) {
        Ok(r)  => println!("[+] {}", r.trim()),
        Err(e) => eprintln!("[-] {}", e),
    }
}

fn main() {
    let cli = Cli::parse();
    let addr = orchestrator_addr(&cli.orchestrator);
    match cli.command {
        Cmd::Push { action: PushAction::Compile { file, label } }
            => cmd_push_compile(&addr, &file, label),
        Cmd::SyncNetworkGateway
            => cmd_sync_gateway(&addr),
        Cmd::Status
            => cmd_status(&addr),
        Cmd::Shell { host, user, port }
            => cmd_shell(&addr, host, &user, port),
        Cmd::Workers
            => cmd_workers(&addr),
        Cmd::Queue
            => cmd_queue(&addr),
        Cmd::Jobs
            => cmd_jobs(&addr),
        Cmd::DownloadPackage { package, version }
            => cmd_download_package(&addr, &package, version),
    }
}
