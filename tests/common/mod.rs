//! [pg] A throwaway cluster for the tests that need a real server: made with the newest
//! PostgreSQL installed (/usr/lib/postgresql/<ver>/bin, or PG_BIN), and reached on its
//! unix socket only. Without one a test says so and passes.
#![allow(dead_code)]

use std::path::{Path, PathBuf};
use std::process::Command;

/// A cluster of its own, stopped and removed when dropped
pub struct Pg {
    pub bin: PathBuf,
    pub dir: PathBuf,
    pub port: u16,
}

impl Pg {
    /// none when no PostgreSQL is installed
    pub fn start(name: &str) -> Option<Pg> {
        let bin = pg_bin()?;
        let dir = std::env::temp_dir().join(format!("sg-{}-{}", name, std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        // the socket alone: no TCP port to collide with another run's
        let port = 50000 + (std::process::id() % 10000) as u16;
        let ok = Command::new(bin.join("initdb")).args(["-A", "trust", "-U", "postgres", "-E", "UTF8", "--no-locale", "-D"])
            .arg(dir.join("data")).output().unwrap().status.success();
        assert!(ok, "initdb");
        let pg = Pg { bin, dir, port };
        let ok = Command::new(pg.bin.join("pg_ctl")).args(["-w", "-l"]).arg(pg.dir.join("log")).arg("-D").arg(pg.dir.join("data"))
            .arg("-o").arg(format!("-c listen_addresses='' -k {} -p {}", pg.dir.display(), pg.port))
            .arg("start").output().unwrap().status.success();
        assert!(ok, "pg_ctl start: {}", std::fs::read_to_string(pg.dir.join("log")).unwrap_or_default());
        Some(pg)
    }

    pub fn url(&self) -> String {
        format!("host={} port={} user=postgres dbname=postgres", self.dir.display(), self.port)
    }

    pub async fn client(&self) -> tokio_postgres::Client {
        let (c, conn) = tokio_postgres::connect(&self.url(), tokio_postgres::NoTls).await.unwrap();
        tokio::spawn(conn);
        c
    }
}

impl Drop for Pg {
    fn drop(&mut self) {
        let _ = Command::new(self.bin.join("pg_ctl")).args(["-w", "-m", "immediate", "-D"]).arg(self.dir.join("data"))
            .arg("stop").output();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// PG_BIN, else the newest /usr/lib/postgresql/<ver>/bin with an initdb
pub fn pg_bin() -> Option<PathBuf> {
    if let Ok(b) = std::env::var("PG_BIN") {
        return Some(PathBuf::from(b));
    }
    let mut versions: Vec<(u32, PathBuf)> = std::fs::read_dir("/usr/lib/postgresql").ok()?
        .filter_map(|e| e.ok())
        .filter_map(|e| Some((e.file_name().to_str()?.parse().ok()?, e.path().join("bin"))))
        .filter(|(_, b): &(u32, PathBuf)| Path::new(b).join("initdb").exists())
        .collect();
    versions.sort();
    let found = versions.pop().map(|(_, b)| b);
    if found.is_none() {
        eprintln!("no PostgreSQL installed (PG_BIN, /usr/lib/postgresql): skipped");
    }
    found
}
