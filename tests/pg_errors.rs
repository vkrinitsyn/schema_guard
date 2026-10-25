//! [pg] A statement Postgres refuses, against a real server: a throwaway cluster, made
//! with the newest PostgreSQL installed (/usr/lib/postgresql/<ver>/bin, or PG_BIN), and
//! reached on its unix socket only. Without one the tests say so and pass.

use std::path::{Path, PathBuf};
use std::process::Command;

use schema_guard_tokio::table::Table;
use schema_guard_tokio::{DeployError, DeployStep, MigrationOptions};
use tokio_postgres::error::SqlState;
use yaml_rust2::YamlLoader;

/// A cluster of its own, stopped and removed when dropped
struct Pg {
    bin: PathBuf,
    dir: PathBuf,
    port: u16,
}

impl Pg {
    /// none when no PostgreSQL is installed
    fn start(name: &str) -> Option<Pg> {
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

    fn url(&self) -> String {
        format!("host={} port={} user=postgres dbname=postgres", self.dir.display(), self.port)
    }

    async fn client(&self) -> tokio_postgres::Client {
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
fn pg_bin() -> Option<PathBuf> {
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

/// a table of one column of a type the server has not
fn refused_table() -> Table {
    let y = YamlLoader::load_from_str("columns:\n  - column: { name: id, type: int }\n  - column: { name: a, type: nosuchtype }\n")
        .unwrap().remove(0);
    Table::new(&y, "refused", None).unwrap()
}

/// [pg] the refused statement comes back as Postgres said it: the native error under the
/// deploy's, its SQLSTATE, the statement - and the line of it the server pointed at
#[tokio::test]
async fn a_refused_statement_comes_back_as_postgres_said_it() {
    let Some(pg) = Pg::start("refused") else { return };
    let mut c = pg.client().await;
    let mut tx = c.transaction().await.unwrap();
    let mut dbc = schema_guard_tokio::loader::load_info_schema("postgres", &mut tx).await.unwrap();

    let e = refused_table().deploy_pg(&mut dbc, &mut tx, &"public".to_string(), "S1__refused.yaml", None,
        &MigrationOptions::default()).await.unwrap_err();
    let DeployError::Pg { step, sql, file, err } = &e else { panic!("{}", e) };
    assert_eq!((step, file.as_str()), (&DeployStep::Table, "S1__refused.yaml"));
    assert_eq!(err.code(), Some(&SqlState::UNDEFINED_OBJECT));
    assert!(sql.contains("nosuchtype"), "{}", sql);
    let source = std::error::Error::source(&e).unwrap();
    assert_eq!(source.to_string(), err.to_string());

    // said: the code, the statement, the file, and under the line - the ^ at the type
    let said = e.to_string();
    assert!(said.starts_with("SG-E") && said.contains("42704") && said.contains("S1__refused.yaml")
        && !said.contains("db error"), "{}", said);
    let lines: Vec<&str> = said.lines().collect();
    let caret = lines.iter().position(|l| l.trim() == "^").unwrap_or_else(|| panic!("no ^ in {}", said));
    let col = lines[caret].chars().count() - 1;
    assert_eq!(lines[caret - 1].chars().skip(col).take(10).collect::<String>(), "nosuchtype", "{}", said);

    // without the statement: the same error, its position as a number
    let bare = schema_guard_tokio::pg_error(err);
    assert!(bare.contains("42704") && !bare.contains('^'), "{}", bare);
}

/// [pg] migrate1, schema_guard's own way to a database: the deploy's message, in full
#[tokio::test]
async fn migrate_says_the_refusal_in_full() {
    let Some(pg) = Pg::start("migrate") else { return };
    let y = YamlLoader::load_from_str("database:\n  - schemaName: public\n    tables:\n      - table:\n          tableName: refused\n\
        \x20         columns:\n            - column: { name: a, type: nosuchtype }\n").unwrap().remove(0);
    let e = schema_guard_tokio::migrate1(y, &pg.url()).await.unwrap_err();
    assert!(e.starts_with("SG-E") && e.contains("42704") && e.contains('^') && !e.contains("db error"), "{}", e);

    // and a server that is not there: what the client could not do, and why
    let gone = format!("host={} port={} user=postgres dbname=postgres", pg.dir.display(), pg.port + 1);
    let y = YamlLoader::load_from_str("database: []\n").unwrap().remove(0);
    let e = schema_guard_tokio::migrate1(y, &gone).await.unwrap_err();
    assert!(e.starts_with("SG-E") && e.contains("No such file or directory"), "{}", e);
}

async fn show(c: &tokio_postgres::Client) -> String {
    c.query_one("show lc_messages", &[]).await.unwrap().get(0)
}

/// [i18n] the server is asked for its messages in our language, when it has them; and
/// one that has not is left as it was, its connection still good
#[tokio::test]
async fn the_server_speaks_our_language_when_it_can() {
    let Some(pg) = Pg::start("lc") else { return };
    let c = pg.client().await;
    let before = show(&c).await;
    let has_ru = c.batch_execute("SET lc_messages TO 'ru_RU.UTF-8'").await.is_ok();
    c.batch_execute(&format!("SET lc_messages TO '{}'", before)).await.unwrap();

    schema_guard_tokio::set_lc_messages(&c, "ru").await;
    if has_ru {
        assert_eq!(show(&c).await, "ru_RU.UTF-8");
    } else {
        assert_eq!(show(&c).await, before, "a locale the server has not: as it was");
    }
    let one: i32 = c.query_one("select 1", &[]).await.unwrap().get(0);
    assert_eq!(one, 1);
}
