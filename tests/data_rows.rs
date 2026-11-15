//! [data] The rows a table's `data` holds, against a real server: a value is text, whatever
//! it holds. A throwaway cluster (common.rs)

mod common;

use common::Pg;
use schema_guard_tokio::{migrate, MigrationOptions};
use yaml_rust2::YamlLoader;

/// [2.3.1] a value with a `'` is inserted as it is written - `O'Brien` failed the whole
/// table's data before - and a value that reads as SQL is a value: no statement of its own.
/// The table's and the column's description beside it, as since 2.2.0
#[tokio::test]
async fn a_value_with_a_quote_is_inserted_as_written() {
    let Some(pg) = Pg::start("data-rows") else { return };
    let mut c = pg.client().await;
    let yaml = "database:\n  - schemaName: public\n    tables:\n\
        \x20     - table:\n          tableName: people\n          description: \"the site's people\"\n          columns:\n\
        \x20           - column: {name: id, type: int, constraint: {primaryKey: true}}\n\
        \x20           - column: {name: name, type: text, description: \"a person's name\"}\n\
        \x20         data:\n\
        \x20           - ['1', plain]\n\
        \x20           - ['2', \"O'Brien\"]\n\
        \x20           - ['3', \"it''s\"]\n\
        \x20           - ['4', \"x'); drop table people; --\"]\n\
        \x20           - ['5', \"'\"]\n";
    let y = YamlLoader::load_from_str(yaml).unwrap().remove(0);
    let mut tx = c.transaction().await.unwrap();
    migrate(y, &mut tx, None, "S1.yaml", &MigrationOptions::default()).await.unwrap();
    tx.commit().await.unwrap();
    let rows: Vec<(i32, String)> = c.query("select id, name from public.people order by id", &[]).await.unwrap()
        .iter().map(|r| (r.get(0), r.get(1))).collect();
    assert_eq!(rows, vec![(1, "plain".to_string()), (2, "O'Brien".to_string()), (3, "it''s".to_string()),
        (4, "x'); drop table people; --".to_string()), (5, "'".to_string())]);
    let comment: Option<String> = c.query_one("select obj_description('public.people'::regclass)", &[]).await.unwrap().get(0);
    assert_eq!(comment.as_deref(), Some("the site's people"));
    let comment: Option<String> = c.query_one("select col_description('public.people'::regclass, 2)", &[]).await.unwrap().get(0);
    assert_eq!(comment.as_deref(), Some("a person's name"));
}
