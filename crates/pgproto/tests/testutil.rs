//! TempCluster lifecycle tests.

use std::path::{Path, PathBuf};
use std::process::Command;

use pgproto::testutil::{ClusterOptions, TempCluster, cleanup_stale_clusters};
use pgproto::{Client, Config};

fn test_base(name: &str) -> PathBuf {
    let base = PathBuf::from(pgproto::testutil::DEFAULT_BASE_DIR)
        .join(format!("t-{name}-{}", std::process::id()));
    std::fs::create_dir_all(&base).unwrap();
    base
}

fn postmaster_running(data_dir: &Path) -> bool {
    let pg_ctl = Path::new(pgproto::testutil::DEFAULT_PG_BIN).join("pg_ctl");
    Command::new(pg_ctl)
        .arg("-D")
        .arg(data_dir)
        .arg("status")
        .output()
        .unwrap()
        .status
        .success()
}

/// Child half of `stale_clusters_are_cleaned_up`: starts a cluster, reports
/// its location and aborts without running destructors.
#[test]
#[ignore]
fn child_abandons_cluster() {
    let Some(base) = std::env::var_os("PGPROTO_CHILD_BASE") else {
        return;
    };
    let pg = TempCluster::start(ClusterOptions {
        base_dir: Some(base.into()),
        ..Default::default()
    })
    .unwrap();
    println!("ROOT={}", pg.root_dir().display());
    println!("SOCK={}", pg.socket_dir.display());
    std::process::abort();
}

#[test]
fn stale_clusters_are_cleaned_up() {
    let base = test_base("stale");
    let out = Command::new(std::env::current_exe().unwrap())
        .args([
            "--ignored",
            "--exact",
            "child_abandons_cluster",
            "--nocapture",
            "--test-threads=1",
        ])
        .env("PGPROTO_CHILD_BASE", &base)
        .output()
        .unwrap();
    assert!(!out.status.success(), "child should abort");
    let stdout = String::from_utf8_lossy(&out.stdout);
    let line = |key: &str| {
        stdout
            .lines()
            .find_map(|l| l.find(key).map(|i| &l[i + key.len()..]))
            .map(PathBuf::from)
            .unwrap_or_else(|| panic!("no {key} in {stdout}"))
    };
    let root = line("ROOT=");
    let sock = line("SOCK=");
    assert!(
        postmaster_running(&root.join("data")),
        "orphaned postmaster should still run"
    );

    // A live cluster in the same base dir must survive the cleanup, and so
    // must a kept one.
    let live = TempCluster::start(ClusterOptions {
        base_dir: Some(base.clone()),
        ..Default::default()
    })
    .unwrap();
    let (kept_root, kept_sock) = {
        let kept = TempCluster::start(ClusterOptions {
            base_dir: Some(base.clone()),
            keep: true,
            ..Default::default()
        })
        .unwrap();
        (kept.root_dir().to_path_buf(), kept.socket_dir.clone())
    };
    assert!(kept_root.join("data").exists());

    cleanup_stale_clusters(&ClusterOptions {
        base_dir: Some(base.clone()),
        ..Default::default()
    });
    assert!(!root.exists(), "stale cluster dir removed");
    assert!(!postmaster_running(&root.join("data")));
    if sock != root {
        assert!(!sock.exists(), "stale socket dir removed");
    }
    assert!(kept_root.join("data").exists(), "kept cluster untouched");
    assert_eq!(live.psql("postgres", "select 1").unwrap(), "1");

    drop(live);
    std::fs::remove_dir_all(&base).unwrap();
    let _ = std::fs::remove_dir_all(&kept_sock);
}

#[test]
fn lifecycle_stop_restart_psql() {
    let base = test_base("life");
    let mut pg = TempCluster::start(ClusterOptions {
        base_dir: Some(base.clone()),
        extra_conf: vec!["shared_buffers = '16MB'".into()],
        ..Default::default()
    })
    .unwrap();
    assert!(pg.is_running());
    assert_eq!(pg.psql("postgres", "show shared_buffers").unwrap(), "16MB");
    assert_eq!(pg.psql("postgres", "show wal_level").unwrap(), "logical");
    assert_eq!(pg.psql("postgres", "show fsync").unwrap(), "off");
    assert_eq!(
        pg.psql("postgres", "show max_replication_slots").unwrap(),
        "20"
    );
    assert_eq!(
        pg.psql(
            "postgres",
            "select datcollate, datlocprovider from pg_database where datname = 'postgres'"
        )
        .unwrap(),
        "C|b"
    );
    assert_eq!(pg.psql("postgres", "show server_encoding").unwrap(), "UTF8");
    let e = pg.psql("postgres", "select * from nope").unwrap_err();
    assert!(e.to_string().contains("does not exist"), "{e}");
    pg.psql(
        "postgres",
        "create table t (x int); insert into t values (1)",
    )
    .unwrap();

    pg.stop();
    assert!(!pg.is_running());
    assert!(Client::connect(&Config::parse(&pg.conninfo("postgres")).unwrap()).is_err());
    pg.restart().unwrap();
    let mut c = Client::connect(&Config::parse(&pg.conninfo("postgres")).unwrap()).unwrap();
    assert_eq!(c.query("select x from t").unwrap().get(0, 0), Some("1"));
    drop(c);
    let root = pg.root_dir().to_path_buf();
    let sock = pg.socket_dir.clone();
    drop(pg);
    assert!(!root.exists());
    assert!(!sock.exists());

    // Fixed port.
    let port = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap().port()
    };
    let pg = TempCluster::start(ClusterOptions {
        base_dir: Some(base.clone()),
        port: Some(port),
        ..Default::default()
    })
    .unwrap();
    assert_eq!(pg.port, port);
    assert_eq!(pg.psql("postgres", "show port").unwrap(), port.to_string());
    // The same port again fails.
    assert!(
        TempCluster::start(ClusterOptions {
            base_dir: Some(base.clone()),
            port: Some(port),
            ..Default::default()
        })
        .is_err()
    );
    drop(pg);
    std::fs::remove_dir_all(&base).unwrap();
}
