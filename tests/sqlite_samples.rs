//! [sqlite] The samples of this crate (tests/*.yaml, PostgreSQL's schemas) applied to SQLite, the
//! embedded one, turso as yt-lite uses it - PostgreSQL's existing samples to the embedded SQLite, as
//! part of schema_guard's end-to-end tests: each mapped by the built-in postgres-sqlite
//! mapping, deployed, read back, applied a second time with nothing to do, and written to. No
//! server: each sample on in-memory databases of turso, the embedded SQLite yt-lite uses. And the rule of the conversion: it is made for the
//! target alone - the same samples applied to PostgreSQL are left as they are.

use turso::Connection;
use schema_guard_tokio::mapping::{Mapping, Source};
use schema_guard_tokio::{load_schema_from_file, parse_yaml_schema, sqlite, MigrationOptions};
use yaml_rust2::Yaml;

/// every sample schema of tests/: a new one is taken up by itself
fn samples() -> Vec<String> {
    let mut v: Vec<String> = std::fs::read_dir("tests").unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().map(|x| x == "yaml").unwrap_or(false))
        .map(|p| p.to_string_lossy().to_string())
        .collect();
    v.sort();
    assert!(v.len() >= 2, "the samples: {:?}", v);
    v
}

fn to_sqlite(y: &Yaml) -> Yaml {
    let m = Mapping::builtin("postgres", "sqlite").unwrap().unwrap();
    m.apply(y, &Source::parse("sqlite.3").unwrap()).unwrap_or_else(|r| panic!("refused: {:#?}", r)).schema
}

/// a connection with a database attached for each schema the sample names but main / public
async fn connection_for(y: &Yaml, file: &str) -> Connection {
    let c = turso::Builder::new_local(":memory:").experimental_attach(true).build().await.unwrap().connect().unwrap();
    for s in &parse_yaml_schema(y.clone(), file).unwrap().list {
        let n = s.schema_name.as_str();
        if !n.eq_ignore_ascii_case("main") && !n.eq_ignore_ascii_case("public") {
            c.execute_batch(&format!("ATTACH DATABASE ':memory:' AS \"{}\"", n)).await.unwrap();
        }
    }
    c
}

#[test]
fn every_sample_is_a_postgres_schema_and_left_as_it_is_for_postgres() {
    let m = Mapping::builtin("postgres", "sqlite").unwrap().unwrap();
    for f in samples() {
        let y = load_schema_from_file(&f).unwrap();
        // no source: a schema written by hand is PostgreSQL's
        assert_eq!(Source::of(&y).unwrap().vendor, "postgres", "{}", f);
        // applied to PostgreSQL: the conversion is the target's alone, so nothing changes
        let same = m.apply(&y, &Source::parse("postgres.16").unwrap()).unwrap();
        assert_eq!(same.schema, y, "{}: changed for its own vendor", f);
        assert!(same.notes.is_empty(), "{}: {:?}", f, same.notes);
    }
}

#[tokio::test]
async fn every_sample_is_deployed_to_sqlite_read_back_and_applied_again_with_nothing_to_do() {
    let opt = MigrationOptions::default();
    for f in samples() {
        let mapped = to_sqlite(&load_schema_from_file(&f).unwrap());
        let c = connection_for(&mapped, &f).await;
        let first = sqlite::migrate(mapped.clone(), &c, false, &f, &opt).await.unwrap_or_else(|e| panic!("{}: {}", f, e));
        assert!(!first.statements.is_empty(), "{}: nothing made", f);

        // read back: each table of the schema (not a template), each of its columns
        let info = sqlite::load_info_schema(&c).await.unwrap();
        for s in &parse_yaml_schema(mapped.clone(), &f).unwrap().list {
            let db = if s.schema_name.eq_ignore_ascii_case("public") { "main" } else { s.schema_name.as_str() };
            for t in s.tables.list.iter().filter(|t| t.is_template != Some(true)) {
                let have = info[db].get(&t.table_name).unwrap_or_else(|| panic!("{}: no table {}.{}", f, db, t.table_name));
                for col in &t.columns.list {
                    assert!(have.columns.contains_key(&col.name), "{}: {}.{} has no column {}", f, db, t.table_name, col.name);
                }
            }
        }

        // the second time: nothing to do, no table rebuilt
        let again = sqlite::migrate(mapped.clone(), &c, false, &f, &opt).await.unwrap();
        assert!(again.statements.is_empty(), "{}: a second run made {:?}", f, again.statements);

        // and each table takes a row of its defaults
        for s in &parse_yaml_schema(mapped.clone(), &f).unwrap().list {
            let db = if s.schema_name.eq_ignore_ascii_case("public") { "main" } else { s.schema_name.as_str() };
            for t in s.tables.list.iter().filter(|t| t.is_template != Some(true)) {
                let needs: Vec<_> = t.columns.list.iter()
                    .filter(|col| col.constraint.as_ref().map(|k| !k.nullable).unwrap_or(false)
                        && col.default_value.is_none() && !col.column_type.eq_ignore_ascii_case("integer"))
                    .collect();
                if needs.is_empty() {
                    c.execute_batch(&format!("INSERT INTO \"{}\".\"{}\" DEFAULT VALUES", db, t.table_name)).await
                        .unwrap_or_else(|e| panic!("{}: a row of {}.{}'s defaults: {}", f, db, t.table_name, e));
                }
            }
        }
    }
}
