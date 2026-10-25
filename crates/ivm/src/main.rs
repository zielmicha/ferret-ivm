//! ivm server binary.

use ivm::coord::{self, Config};
use ivm::server::{self, ServerConfig};
use std::path::PathBuf;
use std::time::Duration;

fn usage() -> ! {
    eprintln!(
        "usage: ivm [--data-dir DIR] [--listen ADDR] [--password PW] [--cache-mb N]\n\
         \x20          [--checkpoint-secs N] [--retention-gsns N] [--tick-ms N] [--no-sync]\n\
         \x20          [--http-listen ADDR]\n\
         \n\
         Incremental view maintenance engine speaking the PostgreSQL protocol.\n\
         Connect with: psql -h 127.0.0.1 -p 6875\n\
         \n\
         --http-listen ADDR  serve the web UI and JSON API on ADDR (e.g. 127.0.0.1:6876);\n\
         \x20                   off by default. With --password, it requires HTTP Basic\n\
         \x20                   auth with that password (any user name)."
    );
    std::process::exit(2)
}

fn main() {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    let mut cfg = Config::default();
    let mut scfg = ServerConfig { listen: "127.0.0.1:6875".into(), password: None };
    let mut http_listen: Option<String> = None;
    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        let mut val = || args.next().unwrap_or_else(|| usage());
        match a.as_str() {
            "--data-dir" => cfg.data_dir = PathBuf::from(val()),
            "--listen" => scfg.listen = val(),
            "--password" => scfg.password = Some(val()),
            "--cache-mb" => cfg.cache_bytes = val().parse::<usize>().unwrap_or_else(|_| usage()) << 20,
            "--checkpoint-secs" => cfg.checkpoint_interval = Duration::from_secs(val().parse().unwrap_or_else(|_| usage())),
            "--retention-gsns" => cfg.retention_gsns = val().parse().unwrap_or_else(|_| usage()),
            "--tick-ms" => {
                let ms: u64 = val().parse().unwrap_or_else(|_| usage());
                cfg.tick = if ms == 0 { None } else { Some(Duration::from_millis(ms)) };
            }
            "--no-sync" => cfg.sync = false,
            "--http-listen" => http_listen = Some(val()),
            "-h" | "--help" => usage(),
            _ => usage(),
        }
    }
    let running = match coord::start(cfg) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("ivm: failed to start: {e}");
            std::process::exit(1);
        }
    };
    let handle = running.handle.clone();
    let http = match http_listen {
        Some(listen) => match ivm::http::start(ivm::http::HttpConfig { listen, password: scfg.password.clone() }, handle.clone()) {
            Ok(h) => Some(h),
            Err(e) => {
                eprintln!("ivm: cannot start the web UI: {e}");
                running.shutdown();
                std::process::exit(1);
            }
        },
        None => None,
    };
    let h2 = handle.clone();
    ctrlc_like(move || h2.shutdown());
    if let Err(e) = server::serve(scfg, handle) {
        eprintln!("ivm: server error: {e}");
    }
    drop(http);
    running.shutdown();
}

/// Shut down cleanly on SIGINT/SIGTERM without extra dependencies.
fn ctrlc_like(f: impl Fn() + Send + 'static) {
    std::thread::spawn(move || {
        // poll for a stop file as a portable fallback
        loop {
            std::thread::sleep(Duration::from_millis(500));
            if std::path::Path::new(".ivm-stop").exists() {
                let _ = std::fs::remove_file(".ivm-stop");
                f();
                std::process::exit(0);
            }
        }
    });
}
