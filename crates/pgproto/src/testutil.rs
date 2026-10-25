//! Temporary PostgreSQL clusters for integration and differential tests.
//!
//! Each cluster lives in `<base>/<pid>-<n>/` (data directory `data/`, log
//! `postgres.log`). Because Unix socket paths are limited to 107 bytes, the
//! socket directory falls back to `$TMPDIR/pgproto-<pid>-<n>` when the
//! cluster directory is too deep. The owning process holds an flock on
//! `owner.lock` in each directory; the first `start()` in a process removes
//! directories whose lock is free (stopping postmasters left behind by
//! killed test processes), except clusters created with `keep: true`.
//!
//! ```no_run
//! use pgproto::testutil::{ClusterOptions, TempCluster};
//! let pg = TempCluster::start(ClusterOptions::default()).unwrap();
//! let mut client = pgproto::Client::connect(&pgproto::Config::parse(&pg.conninfo("postgres")).unwrap()).unwrap();
//! assert_eq!(pg.psql("postgres", "select 1").unwrap(), "1");
//! ```

use std::fs;
use std::io::{self, Write};
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

/// Default parent directory for cluster directories (override with the
/// `PGPROTO_CLUSTER_DIR` environment variable or `ClusterOptions::base_dir`).
pub const DEFAULT_BASE_DIR: &str =
    "/tmp/claude-10001/-home-michal-r-ivm/9fd5bce0-8aca-41c1-adda-7faa045c1f78/scratchpad/clusters";
/// Default PostgreSQL binary directory (override with `PG_BIN`).
pub const DEFAULT_PG_BIN: &str = "/usr/lib/postgresql/17/bin";

/// Maximum length of a Unix socket path (sizeof(sun_path) - 1).
const MAX_SOCKET_PATH: usize = 107;

#[derive(Clone, Debug, Default)]
pub struct ClusterOptions {
    /// `None` = pick a free port.
    pub port: Option<u16>,
    /// Parent directory; a unique subdirectory is created in it.
    pub base_dir: Option<PathBuf>,
    /// Default: env `PG_BIN` or `/usr/lib/postgresql/17/bin`.
    pub pg_bin: Option<PathBuf>,
    /// Extra lines appended to postgresql.conf.
    pub extra_conf: Vec<String>,
    /// Keep the cluster directory on drop (the server is still stopped).
    pub keep: bool,
    /// If set: superuser password and scram-sha-256 auth for TCP (Unix
    /// socket connections stay `trust`).
    pub password: Option<String>,
}

/// A throwaway PostgreSQL cluster: initdb'd into a fresh directory and
/// started with pg_ctl. Stopped (immediate mode) and deleted on drop.
#[derive(Debug)]
pub struct TempCluster {
    pub data_dir: PathBuf,
    pub socket_dir: PathBuf,
    pub port: u16,
    root: PathBuf,
    /// Socket dir lives outside `root` (path-length fallback) and must be
    /// removed separately.
    separate_socket_dir: bool,
    pg_bin: PathBuf,
    password: Option<String>,
    keep: bool,
    running: bool,
    /// Exclusive flocks on the lock files of `root` (and the separate socket
    /// dir), held while this value lives.
    _locks: Vec<fs::File>,
}

static COUNTER: AtomicU32 = AtomicU32::new(0);

fn other(msg: String) -> io::Error {
    io::Error::other(msg)
}

/// Name of the lock file held (flock) by the owning process for the
/// lifetime of a cluster / socket directory.
const LOCK_FILE: &str = "owner.lock";

/// Marker file left in kept cluster directories so stale-cluster cleanup
/// leaves them alone.
const KEEP_MARKER: &str = "KEEP";

/// Creates a fresh uniquely named directory `<parent>/<prefix><pid>-<n>`
/// containing a lock file that stays exclusively locked while the returned
/// `File` is open. The lock is installed atomically (lock, then rename), so a
/// concurrent [`cleanup_stale`] never sees an unlocked lock file in a live
/// directory.
fn unique_dir(parent: &Path, prefix: &str) -> io::Result<(PathBuf, fs::File)> {
    loop {
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let dir = parent.join(format!("{prefix}{}-{n}", std::process::id()));
        match fs::create_dir(&dir) {
            Ok(()) => {
                #[cfg(unix)]
                {
                    use std::os::unix::fs::PermissionsExt;
                    fs::set_permissions(&dir, fs::Permissions::from_mode(0o700))?;
                }
                let tmp = dir.join(format!("{LOCK_FILE}.tmp"));
                let f = fs::File::create(&tmp)?;
                f.lock()?;
                fs::rename(&tmp, dir.join(LOCK_FILE))?;
                return Ok((dir, f));
            }
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(e) => return Err(e),
        }
    }
}

/// Removes `<prefix><pid>-<n>` directories in `parent` whose owner is gone
/// (their lock file is no longer locked, e.g. after a killed test run),
/// stopping any postmaster still running in them first. Directories without
/// a lock file, or marked as kept, are left alone.
fn cleanup_stale(parent: &Path, prefix: &str, pg_bin: &Path) {
    let Ok(entries) = fs::read_dir(parent) else {
        return;
    };
    for e in entries.flatten() {
        let name = e.file_name();
        let Some(rest) = name.to_str().and_then(|n| n.strip_prefix(prefix)) else {
            continue;
        };
        let Some((pid, n)) = rest.split_once('-') else {
            continue;
        };
        if pid.parse::<u32>().is_err() || n.parse::<u32>().is_err() {
            continue;
        }
        let path = e.path();
        if path.join(KEEP_MARKER).exists() {
            continue;
        }
        let Ok(lock) = fs::File::open(path.join(LOCK_FILE)) else {
            continue;
        };
        if lock.try_lock().is_err() {
            continue; // owner still alive
        }
        let data = path.join("data");
        if data.join("postmaster.pid").exists() {
            let _ = Command::new(pg_bin.join("pg_ctl"))
                .arg("-D")
                .arg(&data)
                .args(["-m", "immediate", "-w", "-t", "30", "stop"])
                .output();
        }
        let _ = fs::remove_dir_all(&path);
    }
}

fn socket_path_len(dir: &Path) -> usize {
    dir.as_os_str().len() + "/.s.PGSQL.65535".len()
}

fn pick_free_port() -> io::Result<u16> {
    let l = TcpListener::bind("127.0.0.1:0")?;
    Ok(l.local_addr()?.port())
}

/// Quotes a conninfo value if needed.
fn conninfo_value(v: &str) -> String {
    if !v.is_empty()
        && !v
            .chars()
            .any(|c| c.is_ascii_whitespace() || c == '\'' || c == '\\')
    {
        return v.to_string();
    }
    let mut s = String::from("'");
    for c in v.chars() {
        if c == '\'' || c == '\\' {
            s.push('\\');
        }
        s.push(c);
    }
    s.push('\'');
    s
}

fn conf_quote(v: &str) -> String {
    format!("'{}'", v.replace('\'', "''"))
}

fn output_text(out: &Output) -> String {
    let mut s = String::from_utf8_lossy(&out.stderr).trim().to_string();
    let stdout = String::from_utf8_lossy(&out.stdout);
    if !stdout.trim().is_empty() {
        if !s.is_empty() {
            s.push('\n');
        }
        s.push_str(stdout.trim());
    }
    s
}

/// Resolves (pg_bin, base_dir) from options and environment.
fn resolve_dirs(opts: &ClusterOptions) -> (PathBuf, PathBuf) {
    let pg_bin = opts
        .pg_bin
        .clone()
        .or_else(|| std::env::var_os("PG_BIN").map(PathBuf::from))
        .unwrap_or_else(|| PathBuf::from(DEFAULT_PG_BIN));
    let base = opts
        .base_dir
        .clone()
        .or_else(|| std::env::var_os("PGPROTO_CLUSTER_DIR").map(PathBuf::from))
        .unwrap_or_else(|| PathBuf::from(DEFAULT_BASE_DIR));
    (pg_bin, base)
}

/// Removes cluster (and socket) directories whose owning process is gone,
/// stopping any postmaster still running in them; clusters created with
/// `keep: true` are left alone. Uses the same base directory / pg_bin
/// resolution as [`TempCluster::start`], which calls this once per process.
pub fn cleanup_stale_clusters(opts: &ClusterOptions) {
    let (pg_bin, base) = resolve_dirs(opts);
    cleanup_stale(&base, "", &pg_bin);
    cleanup_stale(&std::env::temp_dir(), "pgproto-", &pg_bin);
}

impl TempCluster {
    /// initdb + configure + `pg_ctl -w start`.
    pub fn start(opts: ClusterOptions) -> io::Result<TempCluster> {
        let (pg_bin, base) = resolve_dirs(&opts);
        fs::create_dir_all(&base)?;
        static CLEANED: std::sync::Once = std::sync::Once::new();
        CLEANED.call_once(|| cleanup_stale_clusters(&opts));
        let (root, lock) = unique_dir(&base, "")?;

        let mut cluster = TempCluster {
            data_dir: root.join("data"),
            socket_dir: root.clone(),
            port: 0,
            root,
            separate_socket_dir: false,
            pg_bin,
            password: opts.password.clone(),
            keep: opts.keep,
            running: false,
            _locks: vec![lock],
        };
        // From here on, Drop cleans up after failures.

        if socket_path_len(&cluster.root) > MAX_SOCKET_PATH {
            // The cluster directory is too deep for a Unix socket path
            // (108-byte sun_path limit): use a short directory instead.
            let (dir, lock) = unique_dir(&std::env::temp_dir(), "pgproto-")?;
            if socket_path_len(&dir) > MAX_SOCKET_PATH {
                drop(lock);
                let _ = fs::remove_dir_all(&dir);
                return Err(other(format!(
                    "no short enough directory for the Unix socket (tried {})",
                    dir.display()
                )));
            }
            cluster.socket_dir = dir;
            cluster.separate_socket_dir = true;
            cluster._locks.push(lock);
        }

        // initdb
        let mut cmd = cluster.command("initdb");
        cmd.arg("-D").arg(&cluster.data_dir).args([
            "-U",
            "postgres",
            "-E",
            "UTF8",
            "--locale-provider=builtin",
            "--builtin-locale=C.UTF-8",
            "--locale=C",
            "--no-sync",
            "--no-instructions",
        ]);
        let pwfile = cluster.root.join("pwfile");
        if let Some(pw) = &opts.password {
            fs::write(&pwfile, format!("{pw}\n"))?;
            cmd.arg(format!("--pwfile={}", pwfile.display()))
                .args(["--auth-local=trust", "--auth-host=scram-sha-256"]);
        } else {
            cmd.arg("--auth=trust");
        }
        let out = cmd.output().map_err(|e| {
            other(format!(
                "could not run {}/initdb: {e}",
                cluster.pg_bin.display()
            ))
        })?;
        let _ = fs::remove_file(&pwfile);
        if !out.status.success() {
            return Err(other(format!("initdb failed: {}", output_text(&out))));
        }

        // Configuration
        let mut conf = String::from("\n# --- pgproto TempCluster ---\n");
        for line in [
            "wal_level = logical".to_string(),
            "max_replication_slots = 20".to_string(),
            "max_wal_senders = 20".to_string(),
            "fsync = off".to_string(),
            "listen_addresses = '127.0.0.1'".to_string(),
            format!(
                "unix_socket_directories = {}",
                conf_quote(&cluster.socket_dir.to_string_lossy())
            ),
        ]
        .into_iter()
        .chain(opts.extra_conf.iter().cloned())
        {
            conf.push_str(&line);
            conf.push('\n');
        }
        cluster.append_conf(&conf)?;

        // Start, retrying with another port if the chosen one is taken.
        let attempts = if opts.port.is_some() { 1 } else { 8 };
        let mut last_err = None;
        for _ in 0..attempts {
            let port = match opts.port {
                Some(p) => p,
                None => pick_free_port()?,
            };
            cluster.append_conf(&format!("port = {port}\n"))?;
            cluster.port = port;
            match cluster.start_server() {
                Ok(()) => return Ok(cluster),
                Err(e) => {
                    let msg = e.to_string();
                    let port_taken = msg.contains("could not bind")
                        || msg.contains("Address already in use")
                        || msg.contains("could not create any TCP/IP sockets")
                        || msg.contains("lock file");
                    last_err = Some(e);
                    if !port_taken {
                        break;
                    }
                }
            }
        }
        Err(last_err.unwrap_or_else(|| other("could not start cluster".into())))
    }

    fn command(&self, program: &str) -> Command {
        let mut cmd = Command::new(self.pg_bin.join(program));
        // Keep the environment from redirecting or altering the tools.
        for (k, _) in std::env::vars_os() {
            if let Some(k) = k.to_str()
                && k.starts_with("PG")
                && k != "PG_BIN"
            {
                cmd.env_remove(k);
            }
        }
        cmd
    }

    fn append_conf(&self, text: &str) -> io::Result<()> {
        let mut f = fs::OpenOptions::new()
            .append(true)
            .open(self.data_dir.join("postgresql.conf"))?;
        f.write_all(text.as_bytes())
    }

    fn log_file(&self) -> PathBuf {
        self.root.join("postgres.log")
    }

    fn log_tail(&self) -> String {
        let log = fs::read_to_string(self.log_file()).unwrap_or_default();
        let lines: Vec<&str> = log.lines().collect();
        lines[lines.len().saturating_sub(20)..].join("\n")
    }

    fn start_server(&mut self) -> io::Result<()> {
        let out = self
            .command("pg_ctl")
            .arg("-D")
            .arg(&self.data_dir)
            .arg("-l")
            .arg(self.log_file())
            .args(["-w", "-t", "120", "start"])
            .output()?;
        if !out.status.success() {
            // Make sure nothing is left half-started.
            let _ = self
                .command("pg_ctl")
                .arg("-D")
                .arg(&self.data_dir)
                .args(["-m", "immediate", "-w", "stop"])
                .output();
            return Err(other(format!(
                "pg_ctl start failed (port {}): {}\n--- server log ---\n{}",
                self.port,
                output_text(&out),
                self.log_tail()
            )));
        }
        self.running = true;
        Ok(())
    }

    fn stop_server(&mut self, mode: &str) {
        if !self.running {
            return;
        }
        let _ = self
            .command("pg_ctl")
            .arg("-D")
            .arg(&self.data_dir)
            .args(["-m", mode, "-w", "-t", "60", "stop"])
            .output();
        self.running = false;
    }

    /// Unix-socket conninfo: `host=<socket_dir> port=<port> user=postgres dbname=<db>`.
    pub fn conninfo(&self, dbname: &str) -> String {
        format!(
            "host={} port={} user=postgres dbname={}",
            conninfo_value(&self.socket_dir.to_string_lossy()),
            self.port,
            conninfo_value(dbname)
        )
    }

    /// TCP conninfo (`host=127.0.0.1`), including the password if one was
    /// configured.
    pub fn tcp_conninfo(&self, dbname: &str) -> String {
        let mut s = format!(
            "host=127.0.0.1 port={} user=postgres dbname={}",
            self.port,
            conninfo_value(dbname)
        );
        if let Some(pw) = &self.password {
            s.push_str(&format!(" password={}", conninfo_value(pw)));
        }
        s
    }

    /// Runs `psql -X -At -v ON_ERROR_STOP=1 -c <sql>` over the Unix socket as
    /// `postgres`; returns stdout without the trailing newline, or an error
    /// containing stderr.
    pub fn psql(&self, dbname: &str, sql: &str) -> io::Result<String> {
        let out = self
            .command("psql")
            .args(["-X", "-A", "-t", "-v", "ON_ERROR_STOP=1", "-h"])
            .arg(&self.socket_dir)
            .arg("-p")
            .arg(self.port.to_string())
            .args(["-U", "postgres", "-d", dbname, "-c", sql])
            .output()?;
        if !out.status.success() {
            return Err(other(format!(
                "psql failed: {}",
                String::from_utf8_lossy(&out.stderr).trim()
            )));
        }
        let mut s = String::from_utf8_lossy(&out.stdout).into_owned();
        if s.ends_with('\n') {
            s.pop();
        }
        Ok(s)
    }

    /// Replaces pg_hba.conf and reloads the configuration.
    pub fn set_hba(&self, contents: &str) -> io::Result<()> {
        fs::write(self.data_dir.join("pg_hba.conf"), contents)?;
        self.reload()
    }

    /// `SELECT pg_reload_conf()`, then a short pause so the postmaster has
    /// applied it before the next connection attempt.
    pub fn reload(&self) -> io::Result<()> {
        self.psql("postgres", "SELECT pg_reload_conf()")?;
        std::thread::sleep(Duration::from_millis(50));
        Ok(())
    }

    /// Path of the server log file.
    pub fn log_path(&self) -> PathBuf {
        self.log_file()
    }

    /// The cluster's own directory (contains `data/` and the log).
    pub fn root_dir(&self) -> &Path {
        &self.root
    }

    pub fn is_running(&self) -> bool {
        self.running
    }

    /// Stops the server (fast shutdown). Files are kept until drop.
    pub fn stop(&mut self) {
        self.stop_server("fast");
    }

    /// (Re)starts the server: fast shutdown if running, then start.
    pub fn restart(&mut self) -> io::Result<()> {
        self.stop_server("fast");
        self.start_server()
    }
}

impl Drop for TempCluster {
    fn drop(&mut self) {
        self.stop_server("immediate");
        if self.keep {
            let _ = fs::write(self.root.join(KEEP_MARKER), "");
            if self.separate_socket_dir {
                let _ = fs::write(self.socket_dir.join(KEEP_MARKER), "");
            }
            eprintln!(
                "pgproto: keeping cluster at {} (socket dir {}, port {})",
                self.root.display(),
                self.socket_dir.display(),
                self.port
            );
            return;
        }
        let _ = fs::remove_dir_all(&self.root);
        if self.separate_socket_dir {
            let _ = fs::remove_dir_all(&self.socket_dir);
        }
    }
}
