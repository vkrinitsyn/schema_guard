//! [dry-run] The _plan deploys, against a real server: each table's dry run hands its
//! callback the table's schema.table and the changes to make there, beside the SQL;
//! the functions without _plan hand the SQL alone. A throwaway cluster (common.rs)

mod common;

use std::sync::Mutex;

use common::Pg;
use schema_guard_tokio::changes::Changes;
use schema_guard_tokio::loader::load_info_schema;
use schema_guard_tokio::table::Table;
use schema_guard_tokio::{migrate, migrate_plan, parse_yaml_schema, MigrationOptions};
use yaml_rust2::YamlLoader;

fn table(name: &str, yaml: &str) -> Table {
    Table::new(&YamlLoader::load_from_str(yaml).unwrap().remove(0), name, None).unwrap()
}

/// what a callback was handed: each table's schema.table, its changes, its SQL's parts
type Seen = Mutex<Vec<(Option<String>, Option<Changes>, usize)>>;

/// [dry-run] a table created: its columns, index, grant, trigger, comment and rows; a
/// table there already: a column added, one widened and its NOT NULL dropped, its owner
/// and primary key; a table as it is: no changes. Nothing reaches the database
#[tokio::test]
async fn each_table_comes_with_its_changes() {
    let Some(pg) = Pg::start("plan") else { return };
    let mut c = pg.client().await;
    c.batch_execute("CREATE TABLE public.t (id int primary key, name varchar(10) not null);\n\
        CREATE TABLE public.same (id int primary key);").await.unwrap();
    let mut tx = c.transaction().await.unwrap();
    let mut dbc = load_info_schema("postgres", &mut tx).await.unwrap();
    let seen: Seen = Mutex::new(vec![]);
    let callback = |sql: Vec<String>, table: Option<&str>, changes: Option<&Changes>| {
        seen.lock().unwrap().push((table.map(String::from), changes.cloned(), sql.len()));
        Ok(())
    };
    let public = "public".to_string();
    let opt = MigrationOptions { with_index_drop: true, ..Default::default() };

    let created = table("n", "description: a new one\ncolumns:\n\
        \x20 - column: {name: id, type: int, constraint: {primaryKey: true}}\n\
        \x20 - column: {name: ref, type: int, index: {name: n_ref}}\n\
        triggers:\n  - trigger: {name: n_trg, event: before update, when: for each row, proc: f()}\n\
        grant:\n  - select: report\n\
        data:\n  - ['1', '2']\n  - ['2', '3']\n");
    assert!(!created.deploy_plan_pg(&mut dbc, &mut tx, &public, "S1.yaml", Some(&callback), &opt).await.unwrap());
    let changed = table("t", "owner: someone\ncolumns:\n\
        \x20 - column: {name: id, type: int, constraint: {primaryKey: true}}\n\
        \x20 - column: {name: name, type: varchar(20)}\n\
        \x20 - column: {name: email, type: text, constraint: {primaryKey: true}}\n");
    assert!(!changed.deploy_plan_pg(&mut dbc, &mut tx, &public, "S1.yaml", Some(&callback), &opt).await.unwrap());
    let same = table("same", "columns:\n  - column: {name: id, type: int, constraint: {primaryKey: true}}\n");
    assert!(!same.deploy_plan_pg(&mut dbc, &mut tx, &public, "S1.yaml", Some(&callback), &opt).await.unwrap());

    let seen = seen.into_inner().unwrap();
    assert_eq!(seen.len(), 3, "{:?}", seen);
    assert_eq!(seen[0], (Some("public.n".into()), Some(Changes {
        create: true, columns: vec!["id".into(), "ref".into()], indexes: vec!["n_ref".into()],
        grants: vec!["report".into()], triggers: vec!["n_trg".into()], comment: true, rows: 2, ..Default::default()
    }), 5));
    assert_eq!(seen[1], (Some("public.t".into()), Some(Changes {
        columns: vec!["email".into()], altered: vec!["name".into()], primary_key: true, owner: true, ..Default::default()
    }), 5));
    assert_eq!(seen[2], (Some("public.same".into()), None, 5));

    // without _plan: the SQL alone
    let parts: Mutex<Vec<usize>> = Mutex::new(vec![]);
    let sql_only = |sql: Vec<String>| {
        parts.lock().unwrap().push(sql.len());
        Ok(())
    };
    let other = table("o", "columns:\n  - column: {name: id, type: int}\n");
    assert!(!other.deploy_pg(&mut dbc, &mut tx, &public, "S1.yaml", Some(&sql_only), &opt).await.unwrap());
    assert!(other.deploy_fk_pg(&parse_yaml_schema(YamlLoader::load_from_str("database: []").unwrap().remove(0), "S1.yaml").unwrap(),
        &mut dbc, &mut tx, &public, false, "S1.yaml", Some(&sql_only)).await.is_ok());
    assert_eq!(parts.into_inner().unwrap(), vec![5, 1]);

    // the dry run ran nothing
    let n: i64 = tx.query_one("select count(*) from pg_tables where tablename in ('n', 'o')", &[]).await.unwrap().get(0);
    assert_eq!(n, 0);
}

/// [dry-run] migrate_plan: every table's changes, then its foreign keys - by column; a
/// table with none, none. migrate and the schema's deploys without _plan: the SQL alone
#[tokio::test]
async fn a_schema_comes_with_each_tables_changes_and_foreign_keys() {
    let Some(pg) = Pg::start("plan-fk") else { return };
    let mut c = pg.client().await;
    let mut tx = c.transaction().await.unwrap();
    let yaml = "database:\n  - schemaName: public\n    tables:\n\
        \x20     - table:\n          tableName: a\n          columns:\n\
        \x20           - column: {name: id, type: int, constraint: {primaryKey: true}}\n\
        \x20     - table:\n          tableName: b\n          columns:\n\
        \x20           - column: {name: id, type: int, constraint: {primaryKey: true}}\n\
        \x20           - column: {name: a_id, type: int, constraint: {foreignKey: {references: a}}}\n";
    let y = YamlLoader::load_from_str(yaml).unwrap().remove(0);
    let seen: Seen = Mutex::new(vec![]);
    let callback = |sql: Vec<String>, table: Option<&str>, changes: Option<&Changes>| {
        seen.lock().unwrap().push((table.map(String::from), changes.cloned(), sql.len()));
        Ok(())
    };
    migrate_plan(y.clone(), &mut tx, Some(&callback), "S1.yaml", &MigrationOptions::default()).await.unwrap();
    let seen = seen.into_inner().unwrap();
    let create = |columns: &[&str]| Some(Changes { create: true, columns: columns.iter().map(|c| c.to_string()).collect(), ..Default::default() });
    assert_eq!(seen, vec![
        (Some("public.a".into()), create(&["id"]), 5),
        (Some("public.b".into()), create(&["id", "a_id"]), 5),
        (Some("public.a".into()), None, 1),
        (Some("public.b".into()), Some(Changes { foreign_keys: vec!["a_id".into()], ..Default::default() }), 1),
    ]);

    // without _plan: migrate, and the schema's own deploys, hand the SQL alone
    let parts: Mutex<Vec<usize>> = Mutex::new(vec![]);
    let sql_only = |sql: Vec<String>| {
        parts.lock().unwrap().push(sql.len());
        Ok(())
    };
    migrate(y.clone(), &mut tx, Some(&sql_only), "S1.yaml", &MigrationOptions::default()).await.unwrap();
    let schemas = parse_yaml_schema(y, "S1.yaml").unwrap();
    let mut dbc = load_info_schema("postgres", &mut tx).await.unwrap();
    for s in &schemas.list {
        s.deploy_all_tables(&mut dbc, &mut tx, Some(&sql_only), &MigrationOptions::default()).await.unwrap();
        s.deploy_all_fk(&schemas, &mut dbc, &mut tx, false, Some(&sql_only)).await.unwrap();
    }
    assert_eq!(parts.into_inner().unwrap(), vec![5, 5, 1, 1, 5, 5, 1, 1]);
}
