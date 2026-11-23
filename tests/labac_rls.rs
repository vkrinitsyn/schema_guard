//! [security] A table's security declaration against a real server: the SQL
//! part, the registry and the RLS policies made once; rows as a role's token says - by the
//! table's owner, not a superuser. A throwaway cluster (common.rs)

mod common;

use common::Pg;
use schema_guard_tokio::security::SecurityMode;
use schema_guard_tokio::{migrate, MigrationOptions};

/// the options of a deploy under `mode`
fn mode(mode: SecurityMode) -> MigrationOptions {
    MigrationOptions::new().security(mode)
}
use yaml_rust2::YamlLoader;

const YAML: &str = "database:\n  - schemaName: public\n    tables:\n\
    \x20     - table:\n          tableName: orders\n\
    \x20         security: {schemaPath: [dept], softDelete: deleted_at}\n\
    \x20         columns:\n\
    \x20           - column: {name: id, type: int, constraint: {primaryKey: true}}\n\
    \x20           - column: {name: dept, type: text}\n\
    \x20           - column: {name: total, type: int}\n\
    \x20           - column: {name: deleted_at, type: timestamptz}\n\
    \x20         grant:\n\
    \x20           - {all: sales}\n\
    \x20         data:\n\
    \x20           - ['1', sales, '10']\n\
    \x20           - ['2', hr, '20']\n";

/// a token labac.enter takes: the ids per kind:action, signed with the key as ytproxy does
async fn token(c: &tokio_postgres::Client, ids: &str) -> String {
    c.query_one("SELECT p || '.' || encode(hmac(p::bytea, (SELECT secret FROM labac.key), 'sha256'), 'hex') \
        FROM (SELECT replace(encode(convert_to('{\"exp\": 9999999999, \"ids\": ' || $1 || '}', 'UTF8'), 'base64'), E'\\n', '') p) x",
        &[&ids]).await.unwrap().get(0)
}

async fn deploy(c: &mut tokio_postgres::Client) {
    let y = YamlLoader::load_from_str(YAML).unwrap().remove(0);
    let mut tx = c.transaction().await.unwrap();
    tx.batch_execute("SET ROLE app").await.unwrap();
    migrate(y, &mut tx, None, "orders.yaml", &mode(SecurityMode::Rls)).await.unwrap();
    tx.commit().await.unwrap();
}

/// the labac policies' oids: the same ones when nothing made them again
async fn oids(c: &tokio_postgres::Client) -> Vec<i64> {
    c.query("SELECT oid::int8 FROM pg_policy WHERE polrelid = 'public.orders'::regclass ORDER BY 1", &[]).await.unwrap()
        .iter().map(|r| r.get::<_, i64>(0)).collect()
}

#[tokio::test]
async fn rls_policies_filter_rows_by_the_token() {
    let Some(pg) = Pg::start("labac-rls") else { return };
    let mut c = pg.client().await;
    c.batch_execute("CREATE ROLE app; CREATE ROLE sales; GRANT CREATE ON DATABASE postgres TO app; \
        GRANT CREATE ON SCHEMA public TO app").await.unwrap();
    deploy(&mut c).await;
    let reg: (String, String, Option<String>, bool) = c.query_one("SELECT column_name, kind, soft_delete, rls FROM labac.tables \
        WHERE schema_name = 'public' AND table_name = 'orders'", &[]).await.map(|r| (r.get(0), r.get(1), r.get(2), r.get(3))).unwrap();
    assert_eq!(reg, ("dept".into(), "dept".into(), Some("deleted_at".into()), true));
    let (on, forced): (bool, bool) = c.query_one("SELECT relrowsecurity, relforcerowsecurity FROM pg_class WHERE oid = 'public.orders'::regclass",
        &[]).await.map(|r| (r.get(0), r.get(1))).unwrap();
    assert!(on && forced);
    let before = oids(&c).await;
    assert_eq!(before.len(), 4);
    // the key ytserv writes; the second deploy makes nothing again, and puts its rows with FORCE on
    c.batch_execute("INSERT INTO labac.key (id, secret) VALUES (1, 'k')").await.unwrap();
    deploy(&mut c).await;
    assert_eq!(oids(&c).await, before, "a declaration not changed: its policies stay");
    let may: bool = c.query_one("SELECT has_function_privilege('sales', 'labac.enter(text)', 'EXECUTE') \
        AND has_function_privilege('sales', 'labac.enter_session(text)', 'EXECUTE')", &[]).await.unwrap().get(0);
    assert!(may, "the table's grantee may enter");

    let read = token(&c, "{\"dept:select\": [\"sales\"], \"dept:update\": [\"sales\"]}").await;
    let tx = c.transaction().await.unwrap();
    tx.batch_execute("SET LOCAL ROLE sales").await.unwrap();
    let n: i64 = tx.query_one("SELECT count(*) FROM orders", &[]).await.unwrap().get(0);
    assert_eq!(n, 0, "no token: no rows");
    tx.execute("SELECT labac.enter($1)", &[&read]).await.unwrap();
    let ids: Vec<i32> = tx.query("SELECT id FROM orders ORDER BY id", &[]).await.unwrap().iter().map(|r| r.get(0)).collect();
    assert_eq!(ids, vec![1], "the token's dept only");
    assert_eq!(tx.execute("UPDATE orders SET total = total + 1", &[]).await.unwrap(), 1, "hr's row not reached");
    assert!(tx.execute("UPDATE orders SET deleted_at = now() WHERE id = 1", &[]).await.is_err(), "a soft delete needs its level");
    tx.rollback().await.unwrap();

    let soft = token(&c, "{\"dept:select\": [\"sales\"], \"dept:update\": [\"sales\"], \"dept:soft_delete\": [\"sales\"]}").await;
    let tx = c.transaction().await.unwrap();
    tx.batch_execute("SET LOCAL ROLE sales").await.unwrap();
    tx.execute("SELECT labac.enter($1)", &[&soft]).await.unwrap();
    assert_eq!(tx.execute("UPDATE orders SET deleted_at = now() WHERE id = 1", &[]).await.unwrap(), 1);
    assert_eq!(tx.execute("DELETE FROM orders", &[]).await.unwrap(), 0, "no delete ids");
    tx.rollback().await.unwrap();
    // for the session, as ytproxy enters it on a pooled connection: past the transaction, gone with a reset
    c.batch_execute("SET ROLE sales").await.unwrap();
    c.execute("SELECT labac.enter_session($1)", &[&read]).await.unwrap();
    let n: i64 = c.query_one("SELECT count(*) FROM orders", &[]).await.unwrap().get(0);
    assert_eq!(n, 1, "the session's subject, in a later transaction");
    c.batch_execute("RESET ROLE").await.unwrap();
    c.batch_execute("DISCARD ALL").await.unwrap();
    c.batch_execute("SET ROLE sales").await.unwrap();
    let n: i64 = c.query_one("SELECT count(*) FROM orders", &[]).await.unwrap().get(0);
    assert_eq!(n, 0, "reset: no subject");
    c.batch_execute("RESET ROLE").await.unwrap();
    // [review 2026-11-21] a token made for another role is no subject here
    let theirs = token(&c, "{\"dept:select\": [\"sales\"]}, \"role\": \"ops\"").await;
    let mine = token(&c, "{\"dept:select\": [\"sales\"]}, \"role\": \"postgres\"").await;
    let n: i64 = c.query_one("SELECT count(*) FROM orders WHERE labac.checked($1) IS NULL", &[&theirs]).await.unwrap().get(0);
    assert_eq!(n, 2, "another role's token does not check");
    let ok: bool = c.query_one("SELECT labac.checked($1) IS NOT NULL", &[&mine]).await.unwrap().get(0);
    assert!(ok, "the login's own role");
    let forged = format!("{}0", &read[..read.len() - 1]);
    let tx = c.transaction().await.unwrap();
    tx.batch_execute("SET LOCAL ROLE sales").await.unwrap();
    assert!(tx.execute("SELECT labac.enter($1)", &[&forged]).await.is_err(), "a token not signed with the key");
}


/// [security] rows that follow their parent's value (a path of join hops) under
/// the alias its table defines, and rows that follow a table with no declaration of its own under the
/// alias they say; order_items stands before its orders in the file
const PATHS: &str = "database:\n  - schemaName: public\n    tables:\n\
    \x20     - table:\n          tableName: order_items\n\
    \x20         security: {schemaPath: [public.order_items.order_id, public.orders.dept]}\n\
    \x20         columns:\n\
    \x20           - column: {name: id, type: int, constraint: {primaryKey: true}}\n\
    \x20           - column: {name: order_id, type: int}\n\
    \x20         grant:\n\
    \x20           - {all: sales}\n\
    \x20         data:\n\
    \x20           - ['10', '1']\n\
    \x20           - ['20', '2']\n\
    \x20     - table:\n          tableName: orders\n\
    \x20         security: {schemaPath: [dept], alias: department}\n\
    \x20         columns:\n\
    \x20           - column: {name: id, type: int, constraint: {primaryKey: true}}\n\
    \x20           - column: {name: dept, type: text}\n\
    \x20         grant:\n\
    \x20           - {all: sales}\n\
    \x20         data:\n\
    \x20           - ['1', sales]\n\
    \x20           - ['2', hr]\n\
    \x20     - table:\n          tableName: folders\n\
    \x20         columns:\n\
    \x20           - column: {name: id, type: int, constraint: {primaryKey: true}}\n\
    \x20           - column: {name: code, type: text}\n\
    \x20         grant:\n\
    \x20           - {all: sales}\n\
    \x20         data:\n\
    \x20           - ['1', p1]\n\
    \x20           - ['2', p2]\n\
    \x20     - table:\n          tableName: documents\n\
    \x20         security: {schemaPath: [public.documents.folder_id, public.folders.code], alias: project}\n\
    \x20         columns:\n\
    \x20           - column: {name: id, type: int, constraint: {primaryKey: true}}\n\
    \x20           - column: {name: folder_id, type: int}\n\
    \x20         grant:\n\
    \x20           - {all: sales}\n\
    \x20         data:\n\
    \x20           - ['100', '1']\n\
    \x20           - ['200', '2']\n\
    \x20           - ['300', '2']\n";

/// as role sales with a token of `ids`: the order items and documents it sees, and whether moving
/// item 10 to the hr order is refused
async fn rows(c: &mut tokio_postgres::Client, ids: &str) -> (Vec<i32>, Vec<i32>, bool) {
    let t = token(c, ids).await;
    let tx = c.transaction().await.unwrap();
    tx.batch_execute("SET LOCAL ROLE sales").await.unwrap();
    tx.execute("SELECT labac.enter($1)", &[&t]).await.unwrap();
    let items: Vec<i32> = tx.query("SELECT id FROM order_items ORDER BY id", &[]).await.unwrap().iter().map(|r| r.get(0)).collect();
    let docs: Vec<i32> = tx.query("SELECT id FROM documents ORDER BY id", &[]).await.unwrap().iter().map(|r| r.get(0)).collect();
    let moved = tx.execute("UPDATE order_items SET order_id = 2 WHERE id = 10", &[]).await;
    tx.rollback().await.unwrap();
    (items, docs, moved.is_err())
}

#[tokio::test]
async fn rows_follow_a_path_and_the_alias_is_a_name() {
    let Some(pg) = Pg::start("security-paths") else { return };
    let mut c = pg.client().await;
    c.batch_execute("CREATE ROLE app; CREATE ROLE sales; GRANT CREATE ON DATABASE postgres TO app; \
        GRANT CREATE ON SCHEMA public TO app").await.unwrap();
    let y = YamlLoader::load_from_str(PATHS).unwrap().remove(0);
    let mut tx = c.transaction().await.unwrap();
    tx.batch_execute("SET ROLE app").await.unwrap();
    migrate(y, &mut tx, None, "paths.yaml", &mode(SecurityMode::Rls)).await.unwrap();
    tx.commit().await.unwrap();
    c.batch_execute("INSERT INTO labac.key (id, secret) VALUES (1, 'k')").await.unwrap();
    let reg: Vec<(String, String, String, String)> = c.query("SELECT table_name, column_name, kind, endpoint FROM labac.tables ORDER BY 1", &[]).await
        .unwrap().iter().map(|r| (r.get(0), r.get(1), r.get(2), r.get(3))).collect();
    assert_eq!(reg, vec![
        ("documents".into(), "public.documents.folder_id|public.folders.code".into(), "project".into(), "public.folders.code".into()),
        ("order_items".into(), "public.order_items.order_id|public.orders.dept".into(), "department".into(), "public.orders.dept".into()),
        ("orders".into(), "dept".into(), "department".into(), "public.orders.dept".into())],
        "the path as written, the alias of its entity, where it ends - and no path stored resolved");
    let (items, docs, moved_refused) = rows(&mut c, "{\"department:select\": [\"sales\"], \"department:update\": [\"sales\"], \"project:select\": [\"p1\"]}").await;
    assert_eq!(items, vec![10], "the item of the sales order only");
    assert_eq!(docs, vec![100], "the documents of folder p1");
    assert!(moved_refused, "an item moved to the hr order leaves the subject's reach: WITH CHECK refuses it");
    let (items, docs, _) = rows(&mut c, "{\"project:select\": [\"p1\", \"p2\"], \"dept:select\": [\"sales\"]}").await;
    assert_eq!((items, docs), (vec![], vec![100, 200, 300]), "ids of dept are not ids of department: no items");
    // the kind read per row is no more: a `kind`, and an alias that is a path, do not deploy
    // [review 2026-11-22] each by its own refusal, not by any error that names an alias
    for (bad, refusal) in [("kind: dept", "security `kind` is `alias` now"),
                           ("alias: public.documents.folder_id|public.folders.code", "security alias is a name, not a path")] {
        let y = YamlLoader::load_from_str(&PATHS.replace("alias: project", bad)).unwrap().remove(0);
        let mut tx = c.transaction().await.unwrap();
        let e = migrate(y, &mut tx, None, "paths.yaml", &mode(SecurityMode::Rls)).await.unwrap_err();
        assert!(e.contains(refusal), "{bad}: {e}");
    }
    // `rls` was a table's key: refused by that name, under every mode - off too, which reads the file
    for m in [SecurityMode::Rls, SecurityMode::Lsr(true), SecurityMode::Off] {
        let y = YamlLoader::load_from_str(&PATHS.replace("alias: project", "alias: project, rls: true")).unwrap().remove(0);
        let mut tx = c.transaction().await.unwrap();
        let e = migrate(y, &mut tx, None, "paths.yaml", &mode(m)).await.unwrap_err();
        assert!(e.contains("`rls` is the deploy's mode now"), "{m:?}: {e}");
    }
}


/// [security] MigrationOptions decide for the deploy, a table says nothing of it: off - the default -
/// installs and registers nothing and leaves a table as it is; rls makes the policies; lsr takes
/// them off and keeps the table registered
#[tokio::test]
async fn the_deploys_options_decide() {
    let Some(pg) = Pg::start("security-mode") else { return };
    let mut c = pg.client().await;
    c.batch_execute("CREATE ROLE app; CREATE ROLE sales; GRANT CREATE ON DATABASE postgres TO app; \
        GRANT CREATE ON SCHEMA public TO app").await.unwrap();
    for opt in [MigrationOptions::default(), mode(SecurityMode::Off), mode("anything else".parse().unwrap())] {
        let y = YamlLoader::load_from_str(YAML).unwrap().remove(0);
        let mut tx = c.transaction().await.unwrap();
        tx.batch_execute("SET ROLE app").await.unwrap();
        migrate(y, &mut tx, None, "orders.yaml", &opt).await.unwrap();
        tx.commit().await.unwrap();
        let installed: bool = c.query_one("SELECT to_regclass('labac.tables') IS NOT NULL", &[]).await.unwrap().get(0);
        assert!(!installed, "off, the default: nothing installed");
    }
    deploy(&mut c).await;
    assert_eq!(oids(&c).await.len(), 4, "rls: the policies");
    // off again: the table stays as rls made it
    run(&mut c, YAML, &MigrationOptions::default()).await.unwrap();
    assert_eq!(oids(&c).await.len(), 4, "off leaves a table as it is");
    for traversal in [true, false] {
        run(&mut c, YAML, &mode(SecurityMode::Lsr(traversal))).await.unwrap();
        assert_eq!(oids(&c).await.len(), 0, "lsr: labac's policies off");
        let (on, rls): (bool, bool) = c.query_one("SELECT c.relrowsecurity, t.rls FROM pg_class c, labac.tables t \
            WHERE c.oid = 'public.orders'::regclass AND t.table_name = 'orders'", &[]).await.map(|r| (r.get(0), r.get(1))).unwrap();
        assert!(!on && !rls, "row-level security off, the registry says lsr");
    }
}


async fn run(c: &mut tokio_postgres::Client, yaml: &str, opt: &MigrationOptions) -> Result<usize, String> {
    let y = YamlLoader::load_from_str(yaml).unwrap().remove(0);
    let mut tx = c.transaction().await.unwrap();
    tx.batch_execute("SET ROLE app").await.unwrap();
    let n = migrate(y, &mut tx, None, "file.yaml", opt).await?;
    tx.commit().await.unwrap();
    Ok(n)
}

/// every labac policy of the database, by its oid: the same ones when nothing made them again
async fn all_policies(c: &tokio_postgres::Client) -> Vec<i64> {
    c.query("SELECT oid::int8 FROM pg_policy WHERE polname LIKE 'labac%' ORDER BY 1", &[]).await.unwrap().iter().map(|r| r.get(0)).collect()
}

/// the alias a table's select policy compares with, as PostgreSQL holds it
async fn policy_alias(c: &tokio_postgres::Client, table: &str) -> String {
    let q: String = c.query_one("SELECT pg_get_expr(polqual, polrelid) FROM pg_policy WHERE polrelid = $1::text::regclass AND polname = 'labac_select'",
        &[&format!("public.{table}")]).await.unwrap().get(0);
    q.split("labac.ids('").nth(1).and_then(|r| r.split('\'').next()).unwrap_or("").to_string()
}

/// [security] across files (as sge deploys: the file's checks are `Lsr(true)`'s alone): a table
/// whose path leaves its file takes the alias the database registers for the entity; one that says
/// another is refused; the defining table's new alias is carried to the tables that reach it; and
/// with the checks on, a path that leaves the file is refused before anything changes
#[tokio::test]
async fn an_alias_is_the_entitys_across_files() {
    let Some(pg) = Pg::start("security-files") else { return };
    let mut c = pg.client().await;
    c.batch_execute("CREATE ROLE app; CREATE ROLE sales; GRANT CREATE ON DATABASE postgres TO app; \
        GRANT CREATE ON SCHEMA public TO app").await.unwrap();
    let orders = |alias: &str| format!("database:\n  - schemaName: public\n    tables:\n\
        \x20     - table:\n          tableName: orders\n\
        \x20         security: {{schemaPath: [dept], alias: {alias}}}\n\
        \x20         columns:\n\
        \x20           - column: {{name: id, type: int, constraint: {{primaryKey: true}}}}\n\
        \x20           - column: {{name: dept, type: text}}\n");
    let items = |extra: &str| format!("database:\n  - schemaName: public\n    tables:\n\
        \x20     - table:\n          tableName: order_items\n\
        \x20         security: {{schemaPath: [public.order_items.order_id, public.orders.dept]{extra}}}\n\
        \x20         columns:\n\
        \x20           - column: {{name: id, type: int, constraint: {{primaryKey: true}}}}\n\
        \x20           - column: {{name: order_id, type: int}}\n");
    let files = mode(SecurityMode::Rls);
    // the file's checks, lsr's with its traversal: the items' file alone does not stand
    let e = run(&mut c, &items(""), &mode(SecurityMode::Lsr(true))).await.unwrap_err();
    assert!(e.contains("public.orders is not a table of this file") && e.contains("SecurityMode::Lsr(false)"), "{e}");
    let n: i64 = c.query_one("SELECT count(*) FROM pg_tables WHERE tablename = 'order_items'", &[]).await.unwrap().get(0);
    assert_eq!(n, 0, "refused before anything changed");
    // without them - lsr with no traversal, and rls - the database is asked: the table is not there yet
    for m in [SecurityMode::Lsr(false), SecurityMode::Rls] {
        let e = run(&mut c, &items(""), &mode(m)).await.unwrap_err();
        assert!(e.contains("public.orders is not known") && !e.contains("of this file"), "{m:?}: {e}");
    }
    // as sge deploys: the defining file, then the one that reaches it
    run(&mut c, &orders("department"), &files).await.unwrap();
    run(&mut c, &items(""), &files).await.unwrap();
    assert_eq!(policy_alias(&c, "order_items").await, "department", "the registry's alias of public.orders.dept");
    let e = run(&mut c, &items(", alias: dept"), &files).await.unwrap_err();
    assert!(e.contains("public.orders.dept is department in this database (public.orders), not dept"), "{e}");
    // the defining table says another alias: the registered table that reaches it follows, row and policies
    run(&mut c, &orders("division"), &files).await.unwrap();
    let kind: String = c.query_one("SELECT kind FROM labac.tables WHERE table_name = 'order_items'", &[]).await.unwrap().get(0);
    assert_eq!((kind.as_str(), policy_alias(&c, "order_items").await.as_str(), policy_alias(&c, "orders").await.as_str()),
        ("division", "division", "division"));
    // both files again: nothing is made again
    let made = all_policies(&c).await;
    assert_eq!(made.len(), 8);
    run(&mut c, &items(""), &files).await.unwrap();
    run(&mut c, &orders("division"), &files).await.unwrap();
    assert_eq!(all_policies(&c).await, made, "the same policies, not new ones");
}

/// [security] the file's checks are lsr's (`Lsr(true)`); an alias that contradicts its entity's is
/// refused at deploy all the same - under rls and under lsr with no traversal - by the defining
/// table of the same deploy
#[tokio::test]
async fn an_alias_against_its_entitys_in_one_file() {
    let Some(pg) = Pg::start("security-onefile") else { return };
    let mut c = pg.client().await;
    c.batch_execute("CREATE ROLE app; GRANT CREATE ON DATABASE postgres TO app; GRANT CREATE ON SCHEMA public TO app").await.unwrap();
    let yaml = |alias: &str| format!("database:\n  - schemaName: public\n    tables:\n\
        \x20     - table:\n          tableName: order_items\n\
        \x20         security: {{schemaPath: [public.order_items.order_id, public.orders.dept]{alias}}}\n\
        \x20         columns:\n\
        \x20           - column: {{name: id, type: int, constraint: {{primaryKey: true}}}}\n\
        \x20           - column: {{name: order_id, type: int}}\n\
        \x20     - table:\n          tableName: orders\n\
        \x20         security: {{schemaPath: [dept], alias: department}}\n\
        \x20         columns:\n\
        \x20           - column: {{name: id, type: int, constraint: {{primaryKey: true}}}}\n\
        \x20           - column: {{name: dept, type: text}}\n");
    for m in [SecurityMode::Rls, SecurityMode::Lsr(false)] {
        let e = run(&mut c, &yaml(", alias: dept"), &mode(m)).await.unwrap_err();
        assert!(e.contains("public.orders.dept is department in this file (public.orders), not dept"), "{m:?}: {e}");
    }
    let e = run(&mut c, &yaml(", alias: dept"), &mode(SecurityMode::Lsr(true))).await.unwrap_err();
    assert!(e.contains("is department in this file, not dept"), "the file's check says it first: {e}");
    run(&mut c, &yaml(""), &mode(SecurityMode::Rls)).await.expect("said once, by the table that defines it");
    assert_eq!((policy_alias(&c, "order_items").await.as_str(), policy_alias(&c, "orders").await.as_str()), ("department", "department"));
}

/// [review 2026-11-22] two tables whose policies would read each other are refused with rls, taken
/// without it
#[tokio::test]
async fn two_tables_reading_each_other() {
    let Some(pg) = Pg::start("security-cycle") else { return };
    let mut c = pg.client().await;
    c.batch_execute("CREATE ROLE app; GRANT CREATE ON DATABASE postgres TO app; GRANT CREATE ON SCHEMA public TO app").await.unwrap();
    let yaml = "database:\n  - schemaName: public\n    tables:\n\
        \x20     - table:\n          tableName: a\n\
        \x20         security: {schemaPath: [public.a.b_id, public.b.code], alias: x}\n\
        \x20         columns:\n\
        \x20           - column: {name: id, type: int, constraint: {primaryKey: true}}\n\
        \x20           - column: {name: b_id, type: int}\n\
        \x20           - column: {name: code, type: text}\n\
        \x20     - table:\n          tableName: b\n\
        \x20         security: {schemaPath: [public.b.a_id, public.a.code], alias: y}\n\
        \x20         columns:\n\
        \x20           - column: {name: id, type: int, constraint: {primaryKey: true}}\n\
        \x20           - column: {name: a_id, type: int}\n\
        \x20           - column: {name: code, type: text}\n";
    let e = run(&mut c, yaml, &mode(SecurityMode::Rls)).await.unwrap_err();
    assert!(e.contains("comes back to it through public.b") && e.contains("deploys under lsr"), "{e}");
    run(&mut c, yaml, &mode(SecurityMode::Lsr(true))).await.expect("registered for the rewrite");
    let n: i64 = c.query_one("SELECT count(*) FROM labac.tables WHERE NOT rls", &[]).await.unwrap().get(0);
    assert_eq!(n, 2);
}

/// the rows of public.orders as the superuser sees them (`run` leaves the session as the owner, whom
/// the forced policies hold too)
async fn orders_count(c: &tokio_postgres::Client) -> i64 {
    c.batch_execute("RESET ROLE").await.unwrap();
    c.query_one("SELECT count(*) FROM orders", &[]).await.unwrap().get(0)
}

/// row-level security of public.orders: enabled, forced
async fn orders_forced(c: &tokio_postgres::Client) -> (bool, bool) {
    let r = c.query_one("SELECT relrowsecurity, relforcerowsecurity FROM pg_class WHERE oid = 'public.orders'::regclass", &[]).await.unwrap();
    (r.get(0), r.get(1))
}

/// [review 2026-11-22] a table deployed rls, then deployed under lsr (or off)
/// with a new column and its data rows: the rows go in although the policies are still there when
/// they do - FORCE is lifted for them - and the declaration is deployed after
#[tokio::test]
async fn data_rows_when_rls_is_taken_off() {
    let Some(pg) = Pg::start("security-data") else { return };
    let mut c = pg.client().await;
    c.batch_execute("CREATE ROLE app; GRANT CREATE ON DATABASE postgres TO app; GRANT CREATE ON SCHEMA public TO app").await.unwrap();
    let yaml = |extra: &str, rows: &str| format!("database:\n  - schemaName: public\n    tables:\n\
        \x20     - table:\n          tableName: orders\n\
        \x20         security: {{schemaPath: [dept]}}\n\
        \x20         columns:\n\
        \x20           - column: {{name: id, type: int, constraint: {{primaryKey: true}}}}\n\
        \x20           - column: {{name: dept, type: text}}\n{extra}\
        \x20         data:\n\
        \x20           - ['1', sales]\n{rows}");
    run(&mut c, &yaml("", ""), &mode(SecurityMode::Rls)).await.unwrap();
    assert_eq!((orders_count(&c).await, orders_forced(&c).await), (1, (true, true)));
    // a deploy under lsr, a column and a row more
    run(&mut c, &yaml("            - column: {name: total, type: int}\n", "            - ['2', hr]\n"), &mode(SecurityMode::Lsr(true))).await
        .expect("the rows of a table still forced");
    assert_eq!((orders_count(&c).await, orders_forced(&c).await), (2, (false, false)), "the row is in, the policies gone");
    // rls again, then a deploy with security off: the table stays as it is, the rows go in
    run(&mut c, &yaml("            - column: {name: total, type: int}\n", "            - ['2', hr]\n"), &mode(SecurityMode::Rls)).await.unwrap();
    assert_eq!(orders_forced(&c).await, (true, true));
    run(&mut c, &yaml("            - column: {name: total, type: int}\n            - column: {name: note, type: text}\n",
        "            - ['2', hr]\n            - ['3', it]\n"), &MigrationOptions::default()).await.expect("security off: the rows of a forced table");
    assert_eq!((orders_count(&c).await, orders_forced(&c).await), (3, (true, true)), "left as it was");
    // and under lsr: registered only
    let lsr = mode(SecurityMode::Lsr(false));
    run(&mut c, &yaml("            - column: {name: total, type: int}\n            - column: {name: note, type: text}\n            - column: {name: more, type: text}\n",
        "            - ['2', hr]\n            - ['3', it]\n            - ['4', ops]\n"), &lsr).await.expect("lsr: the rows of a forced table");
    assert_eq!((orders_count(&c).await, orders_forced(&c).await), (4, (false, false)));
}

/// [review 2026-11-22] a database of a schema_guard before this one - an older SQL part, registry
/// rows with no endpoint: a table that reaches an entity still takes the entity's registered alias,
/// and a defining table's new alias still reaches it; a registered table whose path no longer
/// resolves stops the carry, row and policies together
#[tokio::test]
async fn a_registry_of_an_older_version() {
    let Some(pg) = Pg::start("security-old") else { return };
    let mut c = pg.client().await;
    c.batch_execute("CREATE ROLE app; GRANT CREATE ON DATABASE postgres TO app; GRANT CREATE ON SCHEMA public TO app").await.unwrap();
    let orders = |alias: &str| format!("database:\n  - schemaName: public\n    tables:\n\
        \x20     - table:\n          tableName: orders\n\
        \x20         security: {{schemaPath: [dept], alias: {alias}}}\n\
        \x20         columns:\n\
        \x20           - column: {{name: id, type: int, constraint: {{primaryKey: true}}}}\n\
        \x20           - column: {{name: dept, type: text}}\n");
    let items = "database:\n  - schemaName: public\n    tables:\n\
        \x20     - table:\n          tableName: order_items\n\
        \x20         security: {schemaPath: [public.order_items.order_id, public.orders.dept]}\n\
        \x20         columns:\n\
        \x20           - column: {name: id, type: int, constraint: {primaryKey: true}}\n\
        \x20           - column: {name: order_id, type: int}\n";
    let files = mode(SecurityMode::Rls);
    run(&mut c, &orders("department"), &files).await.unwrap();
    run(&mut c, items, &files).await.unwrap();
    assert_eq!(policy_alias(&c, "order_items").await, "department");
    let age = "COMMENT ON FUNCTION labac.checked(text) IS 'labac:sql:3'; UPDATE labac.tables SET endpoint = NULL";
    // as an older version left it
    c.batch_execute(age).await.unwrap();
    run(&mut c, items, &files).await.expect("the items' file alone, on the older registry");
    let (kind, endpoint): (String, Option<String>) = { let r = c.query_one("SELECT kind, endpoint FROM labac.tables WHERE table_name = 'order_items'", &[]).await.unwrap(); (r.get(0), r.get(1)) };
    assert_eq!((kind.as_str(), endpoint.as_deref(), policy_alias(&c, "order_items").await.as_str()),
        ("department", Some("public.orders.dept"), "department"), "the registered alias, not the column's name");
    // the defining table's new alias reaches a row with no endpoint
    c.batch_execute(age).await.unwrap();
    run(&mut c, &orders("division"), &files).await.unwrap();
    let (kind, endpoint): (String, Option<String>) = { let r = c.query_one("SELECT kind, endpoint FROM labac.tables WHERE table_name = 'order_items'", &[]).await.unwrap(); (r.get(0), r.get(1)) };
    assert_eq!((kind.as_str(), endpoint.as_deref(), policy_alias(&c, "order_items").await.as_str()), ("division", Some("public.orders.dept"), "division"));
    // a registered table whose path no longer resolves: the carry stops, nothing half done
    c.batch_execute("UPDATE labac.tables SET column_name = 'public.order_items.order_id|public.gone.dept', endpoint = 'public.orders.dept' WHERE table_name = 'order_items'").await.unwrap();
    let e = run(&mut c, &orders("unit"), &files).await.unwrap_err();
    assert!(e.contains("cannot be carried to public.order_items") && e.contains("public.gone"), "{e}");
    let kind: String = c.query_one("SELECT kind FROM labac.tables WHERE table_name = 'order_items'", &[]).await.unwrap().get(0);
    assert_eq!((kind.as_str(), policy_alias(&c, "order_items").await.as_str(), policy_alias(&c, "orders").await.as_str()),
        ("division", "division", "division"), "row and policies as they were");
    // a registered table the database no longer has is passed by
    c.batch_execute("UPDATE labac.tables SET column_name = 'public.order_items.order_id|public.orders.dept' WHERE table_name = 'order_items'; \
        INSERT INTO labac.tables (schema_name, table_name, column_name, kind, rls, endpoint) VALUES ('public', 'dropped', 'public.dropped.o|public.orders.dept', 'division', true, 'public.orders.dept')").await.unwrap();
    run(&mut c, &orders("unit"), &files).await.expect("a stale row does not stop it");
    assert_eq!(policy_alias(&c, "order_items").await, "unit");
    // a registered table whose soft-delete column is gone: said as that, before any of its policies is made
    c.batch_execute("UPDATE labac.tables SET soft_delete = 'gone_at' WHERE table_name = 'order_items'").await.unwrap();
    let e = run(&mut c, &orders("team"), &files).await.unwrap_err();
    assert!(e.contains("cannot be carried to public.order_items") && e.contains("softDelete column gone_at"), "{e}");
    assert_eq!((policy_alias(&c, "order_items").await.as_str(), policy_alias(&c, "orders").await.as_str()), ("unit", "unit"));
}

/// a token labac.enter takes, of the body given (its `exp` added): ids, the pairs open to any id, levels
async fn token_of(c: &tokio_postgres::Client, body: &str) -> String {
    c.batch_execute("RESET ROLE").await.unwrap();
    let text = format!("{{\"exp\": 9999999999, {body}}}");
    c.query_one("SELECT p || '.' || encode(hmac(p::bytea, (SELECT secret FROM labac.key), 'sha256'), 'hex') \
        FROM (SELECT replace(encode(convert_to($1, 'UTF8'), 'base64'), E'\\n', '') p) x", &[&text]).await.unwrap().get(0)
}

/// the ids `sql` gives the role sales under `token`, in order
async fn seen(c: &mut tokio_postgres::Client, token: &str, sql: &str) -> Vec<i32> {
    let tx = c.transaction().await.unwrap();
    tx.batch_execute("SET LOCAL ROLE sales").await.unwrap();
    tx.query("SELECT labac.enter($1)", &[&token]).await.unwrap();
    tx.query(sql, &[]).await.unwrap().iter().map(|r| r.get::<_, i32>(0)).collect()
}

/// whether the role sales may run `sql` under `token` (rolled back either way)
async fn may(c: &mut tokio_postgres::Client, token: &str, sql: &str) -> bool {
    let tx = c.transaction().await.unwrap();
    tx.batch_execute("SET LOCAL ROLE sales").await.unwrap();
    tx.query("SELECT labac.enter($1)", &[&token]).await.unwrap();
    tx.execute(sql, &[]).await.map(|n| n > 0).unwrap_or(false)
}

const LEVELS: &str = "database:\n  - schemaName: public\n    tables:\n\
    \x20     - table:\n          tableName: projects\n\
    \x20         security: {schemaPath: {dml: [dept], sal: [sal]}, alias: dept}\n\
    \x20         columns:\n\
    \x20           - column: {name: id, type: int, constraint: {primaryKey: true}}\n\
    \x20           - column: {name: dept, type: text}\n\
    \x20           - column: {name: sal, type: smallint}\n\
    \x20         grant:\n\
    \x20           - {all: sales}\n\
    \x20     - table:\n          tableName: docs\n\
    \x20         security:\n\
    \x20           schemaPath:\n\
    \x20             dml: [public.docs.project_id, public.projects.dept]\n\
    \x20             sal: [public.docs.project_id, public.projects.sal]\n\
    \x20             review: [review_level]\n\
    \x20         columns:\n\
    \x20           - column: {name: id, type: int, constraint: {primaryKey: true}}\n\
    \x20           - column: {name: project_id, type: int}\n\
    \x20           - column: {name: review_level, type: int}\n\
    \x20         grant:\n\
    \x20           - {all: sales}\n\
    \x20     - table:\n          tableName: notices\n\
    \x20         security: {schemaPath: {sal: [sal]}}\n\
    \x20         columns:\n\
    \x20           - column: {name: id, type: int, constraint: {primaryKey: true}}\n\
    \x20           - column: {name: sal, type: int}\n\
    \x20         grant:\n\
    \x20           - {all: sales}\n";

/// [levels] rows by the user's level, against a real server: the user's level must be the same as or
/// higher than the row's - in the row's own column or through a join - beside the dml condition;
/// 0 reaches nothing, a NULL level counts as 0, a row whose path reaches no row is reached by nobody;
/// an action given on every resource of a kind leaves only the levels (a reviewer of all departments
/// below the top secret ones); a row is not made or moved above the user's level; a table of levels
/// alone; nothing is made again on a second deploy
const TASKS: &str = "database:\n  - schemaName: public\n    tables:\n\
    \x20     - table:\n          tableName: tasks\n\
    \x20         security: {schemaPath: [public.tasks.project_id, public.projects.dept]}\n\
    \x20         columns:\n\
    \x20           - column: {name: id, type: int, constraint: {primaryKey: true}}\n\
    \x20           - column: {name: project_id, type: int}\n\
    \x20         grant:\n\
    \x20           - {all: sales}\n";

#[tokio::test]
async fn rows_by_the_users_level() {
    let Some(pg) = Pg::start("security-levels") else { return };
    let mut c = pg.client().await;
    c.batch_execute("CREATE ROLE app; CREATE ROLE sales; GRANT CREATE ON DATABASE postgres TO app; \
        GRANT CREATE ON SCHEMA public TO app").await.unwrap();
    run(&mut c, LEVELS, &mode(SecurityMode::Rls)).await.unwrap();
    let levels: Option<String> = c.query_one("SELECT levels FROM labac.tables WHERE table_name = 'docs'", &[]).await.unwrap().get(0);
    assert_eq!(levels.as_deref(), Some("{\"review\":\"review_level\",\"sal\":\"public.docs.project_id|public.projects.sal\"}"));
    let (path, kind): (String, String) = { let r = c.query_one("SELECT column_name, kind FROM labac.tables WHERE table_name = 'notices'", &[]).await.unwrap(); (r.get(0), r.get(1)) };
    assert_eq!((path.as_str(), kind.as_str()), ("", ""), "levels alone: no path, no alias");
    let made = all_policies(&c).await;
    assert_eq!(made.len(), 12);
    run(&mut c, LEVELS, &mode(SecurityMode::Rls)).await.unwrap();
    assert_eq!(all_policies(&c).await, made, "the same policies, not new ones");
    c.batch_execute("RESET ROLE; INSERT INTO labac.key (id, secret) VALUES (1, 'k'); \
        INSERT INTO projects VALUES (1, 'sales', 1), (2, 'sales', 3), (3, 'hr', 2), (4, 'hr', 6), (5, 'sales', NULL); \
        INSERT INTO docs VALUES (10, 1, 1), (20, 2, 1), (30, 1, 4), (40, NULL, 1), (50, 5, NULL), (60, 3, 1); \
        INSERT INTO notices VALUES (1, 1), (2, 4), (3, NULL)").await.unwrap();
    let sales = "\"ids\": {\"dept:select\": [\"sales\"], \"dept:insert\": [\"sales\"], \"dept:update\": [\"sales\"]}";
    // the member of sales at sal 2: its department's projects at 2 or lower, and the one with no level
    let t = token_of(&c, &format!("{sales}, \"levels\": {{\"sal\": 2, \"review\": 1}}")).await;
    assert_eq!(seen(&mut c, &t, "SELECT id FROM projects ORDER BY 1").await, vec![1, 5]);
    // its documents: the project's level through the join, and the document's own review level
    assert_eq!(seen(&mut c, &t, "SELECT id FROM docs ORDER BY 1").await, vec![10, 50], "20: its project is at 3; 30: review 4; 40: no project; 60: hr");
    assert_eq!(seen(&mut c, &t, "SELECT id FROM notices ORDER BY 1").await, vec![1, 3]);
    // no level at all: nothing of a table that declares one, whatever the ids
    let none = token_of(&c, sales).await;
    for table in ["projects", "docs", "notices"] {
        assert!(seen(&mut c, &none, &format!("SELECT id FROM {table}")).await.is_empty(), "{table}: level 0 reaches nothing");
    }
    let half = token_of(&c, &format!("{sales}, \"levels\": {{\"sal\": 6}}")).await;
    assert!(seen(&mut c, &half, "SELECT id FROM docs").await.is_empty(), "every level of the table: no review level, no document");
    assert_eq!(seen(&mut c, &half, "SELECT id FROM projects ORDER BY 1").await, vec![1, 2, 5]);
    // the reviewer: select on every department, said apart from the ids; cleared for 5 - not the top secret
    let reviewer = token_of(&c, "\"ids\": {}, \"any\": [\"dept:select\"], \"levels\": {\"sal\": 5, \"review\": 6}").await;
    assert_eq!(seen(&mut c, &reviewer, "SELECT id FROM projects ORDER BY 1").await, vec![1, 2, 3, 5], "all departments, 4 is at 6");
    assert_eq!(seen(&mut c, &reviewer, "SELECT id FROM docs ORDER BY 1").await, vec![10, 20, 30, 50, 60], "40 reaches no project: nobody");
    assert!(!may(&mut c, &reviewer, "UPDATE projects SET dept = dept WHERE id = 1").await, "select on all is no update");
    let star = token_of(&c, "\"ids\": {\"dept:select\": [\"*\"]}, \"levels\": {\"sal\": 6}").await;
    assert!(seen(&mut c, &star, "SELECT id FROM projects").await.is_empty(), "a * among the ids is an id, not every id");
    // [review 2026-11-22] a hidden parent denies: a task follows its project for the department only -
    // it declares no level - and is reached only when its project is read. The user of every
    // department too: the choice is inside the path's EXISTS, where the project's own policies apply
    c.batch_execute("RESET ROLE").await.unwrap();
    run(&mut c, TASKS, &mode(SecurityMode::Rls)).await.unwrap();
    c.batch_execute("RESET ROLE; INSERT INTO tasks VALUES (100, 1), (200, 2), (300, 3), (400, 4), (500, NULL)").await.unwrap();
    assert_eq!(seen(&mut c, &t, "SELECT id FROM tasks ORDER BY 1").await, vec![100], "200: its project is at 3, above the member's 2");
    assert_eq!(seen(&mut c, &reviewer, "SELECT id FROM tasks ORDER BY 1").await, vec![100, 200, 300],
        "every department, cleared for 5: 400 is of the project at 6; 500 has no project - nobody's");
    assert!(seen(&mut c, &none, "SELECT id FROM tasks").await.is_empty(), "no level, so no project read, so none of their tasks");
    // a row is not made or moved above the user's level, nor into another department
    assert!(may(&mut c, &t, "INSERT INTO projects VALUES (7, 'sales', 2)").await);
    assert!(may(&mut c, &t, "INSERT INTO projects VALUES (7, 'sales', NULL)").await);
    assert!(!may(&mut c, &t, "INSERT INTO projects VALUES (7, 'sales', 3)").await, "above its level");
    assert!(!may(&mut c, &t, "INSERT INTO projects VALUES (7, 'hr', 1)").await);
    assert!(!may(&mut c, &t, "UPDATE projects SET sal = 5 WHERE id = 1").await, "raised above its level");
    assert!(may(&mut c, &t, "UPDATE projects SET sal = 2 WHERE id = 1").await);
    assert!(!may(&mut c, &t, "UPDATE projects SET sal = 1 WHERE id = 2").await, "a row above its level is not its to lower");
    assert!(may(&mut c, &t, "INSERT INTO notices VALUES (9, 2)").await && !may(&mut c, &t, "INSERT INTO notices VALUES (9, 3)").await);
    // a level that is no whole number is said by name, before any SQL
    let text = LEVELS.replace("- column: {name: sal, type: int}", "- column: {name: sal, type: text}").replace("tableName: notices", "tableName: memos");
    let e = run(&mut c, &text, &mode(SecurityMode::Rls)).await.unwrap_err();
    assert!(e.contains("security level sal of public.memos: public.memos.sal must be an integer column"), "{e}");
    // under lsr: registered with its levels, the policies off
    run(&mut c, LEVELS, &mode(SecurityMode::Lsr(true))).await.unwrap();
    // the tasks' file reaches the projects of another one: without the one-file checks
    run(&mut c, TASKS, &mode(SecurityMode::Lsr(false))).await.unwrap();
    assert!(all_policies(&c).await.is_empty());
    let n: i64 = c.query_one("SELECT count(*) FROM labac.tables WHERE levels IS NOT NULL AND NOT rls", &[]).await.unwrap().get(0);
    assert_eq!(n, 3);
}
