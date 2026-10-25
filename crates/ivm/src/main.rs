//! ivm server binary.
//!
//! Exit status: 0 after a requested shutdown (SIGINT, SIGTERM or the stop
//! file) whose final checkpoint succeeded; 1 when the server cannot start
//! (a listen address, the data directory, recovery, the web UI), when it
//! stops because of an error, or on a second shutdown signal; 2 for bad
//! command-line arguments.
//!
//! The files of Unix-domain sockets are removed before exiting (except on a
//! second signal: the next start replaces them).

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use ivm::coord::{self, Config};
use ivm::server::{Server, ServerConfig};
use std::path::PathBuf;
use std::time::Duration;

fn usage() -> ! {
    eprintln!(
        "usage: ivm [--data-dir DIR] [--listen ADDR]... [--password PW] [--cache-mb N]\n\
         \x20          [--checkpoint-secs N] [--retention-gsns N] [--tick-ms N] [--no-sync]\n\
         \x20          [--http-listen ADDR]...\n\
         \n\
         Incremental view maintenance engine speaking the PostgreSQL protocol.\n\
         Connect with: psql -h 127.0.0.1 -p 6875\n\
         \n\
         --listen ADDR       accept PostgreSQL connections on ADDR (default 127.0.0.1:6875).\n\
         --http-listen ADDR  serve the web UI and JSON API on ADDR (e.g. 127.0.0.1:6876);\n\
         \x20                   off by default. With --password, it requires HTTP Basic\n\
         \x20                   auth with that password (any user name).\n\
         \n\
         ADDR is host:port, or the path of a Unix-domain socket if it contains a /\n\
         (psql -h DIR -p N connects to the socket DIR/.s.PGSQL.N). Both flags can be\n\
         given several times, to listen on several addresses.\n\
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
    let mut scfg = ServerConfig::default();
    let mut listen: Vec<String> = Vec::new();
    let mut http_listen: Vec<String> = Vec::new();
    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        let mut val = || args.next().unwrap_or_else(|| usage());
        match a.as_str() {
            "--data-dir" => cfg.data_dir = PathBuf::from(val()),
            "--listen" => listen.push(val()),
            "--password" => scfg.password = Some(val()),
            "--cache-mb" => cfg.cache_bytes = val().parse::<usize>().unwrap_or_else(|_| usage()) << 20,
            "--checkpoint-secs" => cfg.checkpoint_interval = Duration::from_secs(val().parse().unwrap_or_else(|_| usage())),
            "--retention-gsns" => cfg.retention_gsns = val().parse().unwrap_or_else(|_| usage()),
            "--tick-ms" => {
                let ms: u64 = val().parse().unwrap_or_else(|_| usage());
                cfg.tick = if ms == 0 { None } else { Some(Duration::from_millis(ms)) };
            }
            "--no-sync" => cfg.sync = false,
            "--http-listen" => http_listen.push(val()),
            "-h" | "--help" => usage(),
            _ => usage(),
        }
    }
    if listen.is_empty() {
        listen.push("127.0.0.1:6875".into());
    }
    // shutdown requests, from now on (also during recovery)
    let (stop_tx, stop_rx) = crossbeam_channel::unbounded::<Stop>();
    if let Err(e) = handle_signals(stop_tx.clone()) {
        eprintln!("ivm: cannot install signal handlers: {e}");
        std::process::exit(1);
    }
    watch_stop_file(stop_tx.clone());
    // the listen addresses before recovery, which may take long
    let mut listeners = Vec::new();
    let mut socket_files = Vec::new();
    for addr in &listen {
        match ivm::listen::bind(addr) {
            Ok((l, file)) => {
                listeners.push(l);
                socket_files.extend(file);
            }
            Err(e) => {
                eprintln!("ivm: cannot listen on {addr}: {e}");
                drop(socket_files);
                std::process::exit(1);
            }
        }
    }
    let running = match coord::start(cfg) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("ivm: failed to start: {e}");
            drop(socket_files);
            std::process::exit(1);
        }
    };
    let handle = running.handle.clone();
    let http = if http_listen.is_empty() {
        None
    } else {
        match ivm::http::start(ivm::http::HttpConfig { listen: http_listen, password: scfg.password.clone() }, handle.clone()) {
            Ok(h) => Some(h),
            Err(e) => {
                eprintln!("ivm: cannot start the web UI: {e}");
                running.shutdown();
                drop(socket_files);
                std::process::exit(1);
            }
        }
    };
    // one thread per listener; the sessions of all of them share cancel keys
    let server = Server::new(scfg, handle.clone());
    for (addr, listener) in listen.iter().zip(listeners) {
        log::info!("listening on {}", listener.local_addr().map(|a| a.to_string()).unwrap_or_else(|_| addr.clone()));
        let server = server.clone();
        let stop_tx = stop_tx.clone();
        let spawned = std::thread::Builder::new().name("ivm-listener".into()).spawn(move || {
            let r = server.serve(listener);
            let _ = stop_tx.send(Stop::Failed(match r {
                Ok(()) => "the listener stopped".into(),
                Err(e) => format!("server error: {e}"),
            }));
        });
        if let Err(e) = spawned {
            eprintln!("ivm: cannot start the listener: {e}");
            drop(http);
            running.shutdown();
            drop(socket_files);
            std::process::exit(1);
        }
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
    drop(socket_files);
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
