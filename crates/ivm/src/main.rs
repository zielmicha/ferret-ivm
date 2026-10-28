//! ivm server binary.
//!
//! Exit status: 0 after a requested shutdown (SIGINT, SIGTERM or the stop
//! file) whose final checkpoint succeeded; 1 when the server cannot start
//! (the listen address, the data directory, recovery, the web UI), when it
//! stops because of an error, or on a second shutdown signal; 2 for bad
//! command-line arguments.

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use ivm::coord::{self, Config};
use ivm::server::{self, ServerConfig};
use std::path::PathBuf;
use std::time::Duration;

fn usage() -> ! {
    eprintln!(
        "usage: ivm [--data-dir DIR] [--listen ADDR] [--password PW] [--memory-mb N]\n\
         \x20          [--checkpoint-secs N] [--tick-ms N] [--no-sync]\n\
         \x20          [--http-listen ADDR]\n\
         \n\
         Incremental view maintenance engine speaking the PostgreSQL protocol.\n\
         Connect with: psql -h 127.0.0.1 -p 6875\n\
         \n\
         --http-listen ADDR  serve the web UI and JSON API on ADDR (e.g. 127.0.0.1:6876);\n\
         \x20                   off by default. With --password, it requires HTTP Basic\n\
         \x20                   auth with that password (any user name).\n\
         --memory-mb N       memory budget in MiB (default 2048): the buffer pool of\n\
         \x20                   index nodes, aggregation caches and queues together\n\
         \x20                   (--cache-mb is an older name of it).\n\
         \n\
         SIGINT, SIGTERM or a file named .ivm-stop in the working directory stop the\n\
         server after a final checkpoint; a second signal exits at once."
    );
    std::process::exit(2)
}

/// Why the server stops.
enum Stop {
    /// A shutdown was requested (signal or stop file).
    Requested(&'static str),
    /// The server failed.
    Failed(String),
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
            "--memory-mb" | "--cache-mb" => cfg.memory_bytes = val().parse::<usize>().unwrap_or_else(|_| usage()) << 20,
            "--checkpoint-secs" => cfg.checkpoint_interval = Duration::from_secs(val().parse().unwrap_or_else(|_| usage())),
            "--retention-gsns" => {
                eprintln!("ivm: --retention-gsns was removed: the engine keeps no history (AS OF <gsn> reads are gone)");
                std::process::exit(2);
            }
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
    // shutdown requests, from now on (also during recovery)
    let (stop_tx, stop_rx) = crossbeam_channel::unbounded::<Stop>();
    if let Err(e) = handle_signals(stop_tx.clone()) {
        eprintln!("ivm: cannot install signal handlers: {e}");
        std::process::exit(1);
    }
    watch_stop_file(stop_tx.clone());
    // the listen address before recovery, which may take long
    let listener = match std::net::TcpListener::bind(&scfg.listen) {
        Ok(l) => l,
        Err(e) => {
            eprintln!("ivm: cannot listen on {}: {e}", scfg.listen);
            std::process::exit(1);
        }
    };
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
    log::info!("listening on {}", scfg.listen);
    let h = handle.clone();
    let spawned = std::thread::Builder::new().name("ivm-listener".into()).spawn(move || {
        let r = server::serve_listener(listener, scfg, h);
        let _ = stop_tx.send(Stop::Failed(match r {
            Ok(()) => "the listener stopped".into(),
            Err(e) => format!("server error: {e}"),
        }));
    });
    if let Err(e) = spawned {
        eprintln!("ivm: cannot start the listener: {e}");
        running.shutdown();
        std::process::exit(1);
    }
    // (an engine that stops by itself, after an error, ends the process)
    let stop = loop {
        match stop_rx.recv_timeout(Duration::from_secs(1)) {
            Ok(s) => break s,
            Err(crossbeam_channel::RecvTimeoutError::Timeout) if running.alive() => {}
            Err(_) => break Stop::Failed("the engine stopped".into()),
        }
    };
    let mut status = match stop {
        Stop::Requested(why) => {
            log::info!("{why}: shutting down (final checkpoint)");
            0
        }
        Stop::Failed(e) => {
            eprintln!("ivm: {e}");
            1
        }
    };
    drop(http);
    running.shutdown();
    if handle.shared.failed.load(std::sync::atomic::Ordering::Acquire) {
        // (logged by the failing thread, e.g. the final checkpoint)
        status = 1;
    }
    std::process::exit(status);
}

/// SIGINT and SIGTERM request a shutdown; a second one exits at once (the
/// input log makes that safe: the next start replays it).
fn handle_signals(stop: crossbeam_channel::Sender<Stop>) -> std::io::Result<()> {
    use signal_hook::consts::{SIGINT, SIGTERM};
    let mut signals = signal_hook::iterator::Signals::new([SIGINT, SIGTERM])?;
    std::thread::Builder::new().name("ivm-signals".into()).spawn(move || {
        let mut requested = false;
        for sig in signals.forever() {
            if requested {
                eprintln!("ivm: second signal, exiting without a final checkpoint");
                std::process::exit(1);
            }
            requested = true;
            let _ = stop.send(Stop::Requested(if sig == SIGINT { "SIGINT" } else { "SIGTERM" }));
        }
    })?;
    Ok(())
}

/// A file named `.ivm-stop` in the working directory requests a shutdown
/// (polled every 500 ms; the file is removed).
fn watch_stop_file(stop: crossbeam_channel::Sender<Stop>) {
    let _ = std::thread::Builder::new().name("ivm-stop-file".into()).spawn(move || {
        loop {
            std::thread::sleep(Duration::from_millis(500));
            if std::path::Path::new(".ivm-stop").exists() {
                let _ = std::fs::remove_file(".ivm-stop");
                let _ = stop.send(Stop::Requested("stop file"));
                return;
            }
        }
    });
}
