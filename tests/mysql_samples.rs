//! [mysql] The samples of this crate (tests/*.yaml, PostgreSQL's schemas) applied to a live MySQL:
//! each mapped by the built-in postgres-mysql mapping, deployed, read back, applied a second time
//! with nothing to do, and written to. It needs a server, so it is ignored unless asked for:
//!
//!   SG_MYSQL_URL=mysql://root:pw@host:3306/sg_test cargo test --features mysql --test mysql_samples -- --ignored
//!
//! The URL's database is the one `public` is; every other schema of a sample is a database of
//! its name. Each run drops and makes again the databases it uses: give it a server of its own.

use schema_guard_tokio::mapping::{Mapping, Source};
use schema_guard_tokio::{load_schema_from_file, mysql, parse_yaml_schema, MigrationOptions};
use sqlx::Connection;

fn samples() -> Vec<String> {
    let mut v: Vec<String> = std::fs::read_dir("tests").unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().map(|x| x == "yaml").unwrap_or(false))
        .map(|p| p.to_string_lossy().to_string())
        .collect();
    v.sort();
    v
}

/// with no server: every sample maps to MySQL and plans on an empty one
#[test]
fn every_sample_maps_to_mysql_and_plans_on_an_empty_server() {
    let m = Mapping::builtin("postgres", "mysql").unwrap().unwrap();
    for f in samples() {
        let mapped = m.apply(&load_schema_from_file(&f).unwrap(), &Source::parse("mysql.8.0").unwrap())
            .unwrap_or_else(|r| panic!("{}: refused: {:#?}", f, r)).schema;
        let schemas = parse_yaml_schema(mapped, &f).unwrap();
        let named: Vec<(String, &schema_guard_tokio::schema::Schema)> = schemas.list.iter()
            .map(|s| (if s.schema_name.eq_ignore_ascii_case("public") { "sg_test".to_string() } else { s.schema_name.clone() }, s)).collect();
        let p = mysql::plan_on(&named, &Default::default(), &MigrationOptions::default()).unwrap_or_else(|e| panic!("{}: {}", f, e));
        assert!(p.statements.iter().any(|x| x.starts_with("CREATE TABLE")), "{}: {:?}", f, p.statements);
    }
}

#[tokio::test]
#[ignore = "needs a MySQL server: SG_MYSQL_URL=mysql://user:pw@host/db"]
async fn every_sample_is_deployed_to_mysql_read_back_and_applied_again_with_nothing_to_do() {
    let url = std::env::var("SG_MYSQL_URL").expect("SG_MYSQL_URL: a MySQL server of its own");
    let current = url.rsplit('/').next().unwrap_or("").split('?').next().unwrap_or("").to_string();
    assert!(!current.is_empty(), "SG_MYSQL_URL names its database: the one `public` is");
    let m = Mapping::builtin("postgres", "mysql").unwrap().unwrap();
    let opt = MigrationOptions::default();
    for f in samples() {
        let mapped = m.apply(&load_schema_from_file(&f).unwrap(), &Source::parse("mysql.8.0").unwrap())
            .unwrap_or_else(|r| panic!("{}: refused: {:#?}", f, r)).schema;
        let schemas = parse_yaml_schema(mapped.clone(), &f).unwrap();
        let dbs: Vec<String> = schemas.list.iter()
            .map(|s| if s.schema_name.eq_ignore_ascii_case("public") { current.clone() } else { s.schema_name.clone() }).collect();
        let mut c = sqlx::MySqlConnection::connect(&url).await.unwrap();
        for db in &dbs {
            sqlx::raw_sql(sqlx::AssertSqlSafe(format!("DROP DATABASE IF EXISTS `{}`; CREATE DATABASE `{}`", db, db))).execute(&mut c).await.unwrap();
        }
        sqlx::raw_sql(sqlx::AssertSqlSafe(format!("USE `{}`", current))).execute(&mut c).await.unwrap();

        let first = mysql::migrate(mapped.clone(), &mut c, false, &f, &opt).await.unwrap_or_else(|e| panic!("{}: {}", f, e));
        assert!(!first.statements.is_empty(), "{}: nothing made", f);

        let refs: Vec<&str> = dbs.iter().map(|d| d.as_str()).collect();
        let info = mysql::load_info_schema(&mut c, &refs).await.unwrap();
        for (s, db) in schemas.list.iter().zip(&dbs) {
            for t in s.tables.list.iter().filter(|t| t.is_template != Some(true)) {
                let have = info[db].get(&t.table_name).unwrap_or_else(|| panic!("{}: no table {}.{}", f, db, t.table_name));
                for col in &t.columns.list {
                    assert!(have.columns.contains_key(&col.name), "{}: {}.{} has no column {}", f, db, t.table_name, col.name);
                }
            }
        }

        let again = mysql::migrate(mapped.clone(), &mut c, false, &f, &opt).await.unwrap();
        assert!(again.statements.is_empty(), "{}: a second run made {:?}", f, again.statements);
    }
}

/// live: a column added, a wider type, a NOT NULL and another default, in place; a narrower type
/// refused before any statement runs
#[tokio::test]
#[ignore = "needs a MySQL server: SG_MYSQL_URL=mysql://user:pw@host/db"]
async fn a_change_is_made_in_place_and_a_refusal_runs_nothing() {
    let url = std::env::var("SG_MYSQL_URL").expect("SG_MYSQL_URL");
    let mut c = sqlx::MySqlConnection::connect(&url).await.unwrap();
    sqlx::raw_sql("DROP DATABASE IF EXISTS `sg_change`").execute(&mut c).await.unwrap();
    let v1 = "
database:
  - schemaName: sg_change
    tables:
      - table:
          tableName: t
          columns:
            - column:
                name: id
                type: int auto_increment
                constraint:
                  primaryKey: true
            - column:
                name: name
                type: varchar(20)
            - column:
                name: active
                type: tinyint(1)
                defaultValue: \"1\"
";
    let y = |s: &str| schema_guard_tokio::load_schema_from_src(s.to_string()).unwrap();
    let opt = MigrationOptions::default();
    mysql::migrate(y(v1), &mut c, false, "S1__a.yaml", &opt).await.unwrap();
    sqlx::raw_sql("INSERT INTO sg_change.t (name) VALUES ('a')").execute(&mut c).await.unwrap();
    let v2 = v1.replace("type: varchar(20)", "type: varchar(80)\n                constraint:\n                  nullable: false")
        .replace("defaultValue: \"1\"", "defaultValue: \"0\"\n            - column:\n                name: note\n                type: text");
    let p = mysql::migrate(y(&v2), &mut c, false, "S2__a.yaml", &opt).await.unwrap();
    assert_eq!(p.statements.len(), 1, "{:?}", p.statements);
    let info = mysql::load_info_schema(&mut c, &["sg_change"]).await.unwrap();
    let t = &info["sg_change"]["t"];
    assert_eq!(t.columns["name"].column_type, "varchar(80)");
    assert!(!t.columns["name"].nullable);
    assert_eq!(t.columns["active"].column_default.as_deref(), Some("0"));
    assert!(t.columns.contains_key("note"));
    assert!(mysql::migrate(y(&v2), &mut c, false, "S2__a.yaml", &opt).await.unwrap().statements.is_empty());
    let narrow = v2.replace("type: varchar(80)", "type: varchar(10)");
    assert!(mysql::migrate(y(&narrow), &mut c, false, "S3__a.yaml", &opt).await.unwrap_err().contains("set with_size_cut"));
    let info = mysql::load_info_schema(&mut c, &["sg_change"]).await.unwrap();
    assert_eq!(info["sg_change"]["t"].columns["name"].column_type, "varchar(80)", "nothing ran");
    sqlx::raw_sql("DROP DATABASE `sg_change`").execute(&mut c).await.unwrap();
}
