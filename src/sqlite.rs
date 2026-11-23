//! [sqlite] SQLite (feature `sqlite`): its catalog read into the schema's model,
//! and a YAML schema deployed to it through the driver: turso, the embedded SQLite yt-lite uses,
//! async. A connection that attaches databases is built with
//! `experimental_attach(true)`.
//!
//! A schema's name is a database of the connection: `main` - the file itself, as `public` is
//! read too, PostgreSQL's usual one - or one the caller attached (`ATTACH ... AS name`). The rest
//! as on PostgreSQL: what the schema has that the database has not is made, nothing is dropped,
//! a column the database has that the schema has not is kept.
//!
//! SQLite's ALTER TABLE adds a column and little more. What it cannot do - a column's type,
//! nullability, default or key changed, a column added that is a key, unique, NOT NULL with no
//! default, or of a default that is no constant - is done by SQLite's own table rebuild: the
//! table made again under a new name with the columns it should have, its rows copied over, the
//! old one dropped, the new one renamed to it, its indexes and triggers made again - in one
//! transaction, foreign keys off around it and checked before the commit
//! (https://www.sqlite.org/lang_altertable.html). A type changed for a narrower one, or for an
//! incompatible one, needs `with_size_cut`, as on PostgreSQL.
//!
//! Not in SQLite: owners and grants (left out), comments (descriptions left out), partitions
//! (refused), a trigger of a function (PostgreSQL's form: refused, unless `exclude_triggers`).
//! Custom SQL of the schema is SQLite's own here and goes in as written.

use std::collections::HashMap;

/// [sqlite-core] A value of a row, as the engine reads one
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
}

/// [sqlite-core] The SQLite connection the engine reads and deploys through (yt-sqlite's
/// migrate): turso's with the `sqlite` feature; any other - rusqlite's, a loadable
/// extension's own - implements these three with `sqlite-core` alone
pub trait SqliteDb {
    /// a query's rows, each value as SQLite holds it
    fn rows(&self, sql: &str) -> impl std::future::Future<Output = Result<Vec<Vec<Value>>, String>>;
    /// statements that return no rows
    fn batch(&self, sql: &str) -> impl std::future::Future<Output = Result<(), String>>;
    /// outside an explicit transaction
    fn autocommit(&self) -> Result<bool, String>;
}

#[cfg(feature = "sqlite")]
impl SqliteDb for turso::Connection {
    async fn rows(&self, sql: &str) -> Result<Vec<Vec<Value>>, String> {
        let mut r = self.query(sql, ()).await.map_err(|e| e.to_string())?;
        let n = r.column_count();
        let mut out = Vec::new();
        while let Some(row) = r.next().await.map_err(|e| e.to_string())? {
            out.push((0..n).map(|i| match row.get_value(i) {
                Ok(turso::Value::Integer(v)) => Value::Integer(v),
                Ok(turso::Value::Real(v)) => Value::Real(v),
                Ok(turso::Value::Text(v)) => Value::Text(v),
                Ok(turso::Value::Blob(v)) => Value::Blob(v),
                _ => Value::Null,
            }).collect());
        }
        Ok(out)
    }

    async fn batch(&self, sql: &str) -> Result<(), String> {
        self.execute_batch(sql).await.map_err(|e| e.to_string())
    }

    fn autocommit(&self) -> Result<bool, String> {
        self.is_autocommit().map_err(|e| e.to_string())
    }
}
use yaml_rust2::Yaml;

use crate::column::Column;
use crate::loader::{FKTable, InfoSchemaType, PgColumnDfn, PgIndex, PgIndexColumn, PgPrimaryKey, PgTable, PgTrigger};
use crate::schema::Schema;
use crate::table::{analyze_type_change, Table, TypeChange};
use crate::MigrationOptions;

/// a column as SQLite has it (PRAGMA table_xinfo)
#[derive(Debug, Clone)]
struct SqColumn {
    name: String,
    decl: String,
    notnull: bool,
    dflt: Option<String>,
    pk: i64,
    hidden: i64,
}

#[derive(Debug, Clone)]
struct SqIndex {
    name: String,
    unique: bool,
    /// c: CREATE INDEX, u: a UNIQUE constraint, pk: the primary key
    origin: String,
    columns: Vec<String>,
    sql: Option<String>,
}

#[derive(Debug, Clone)]
struct SqFk {
    from: String,
    table: String,
    /// the parent's column; empty for its primary key
    to: String,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
struct SqTable {
    name: String,
    columns: Vec<SqColumn>,
    indexes: Vec<SqIndex>,
    fks: Vec<SqFk>,
    triggers: Vec<(String, String)>,
    /// [review 2026-11-18] its CREATE TABLE as SQLite keeps it: what a rebuild keeps of it
    create: Create,
    /// its AUTOINCREMENT's counter, kept over a rebuild
    seq: Option<i64>,
    /// [turso] the views on it: (name, sql)
    views: Vec<(String, String)>,
}

/// a CREATE TABLE split: each column's definition by its name, the table's own constraints, and
/// what follows its parentheses (WITHOUT ROWID, STRICT)
#[derive(Debug, Clone, Default)]
struct Create {
    columns: Vec<(String, String)>,
    constraints: Vec<String>,
    options: String,
}

/// the name a definition starts with, unquoted ("a b", `a`, [a], a)
fn first_name(def: &str) -> String {
    let d = def.trim_start();
    let close = match d.chars().next() {
        Some('"') => Some('"'),
        Some('`') => Some('`'),
        Some('[') => Some(']'),
        _ => None,
    };
    match close {
        Some(c) => d[1..].split(c).next().unwrap_or("").to_string(),
        None => d.split(|ch: char| ch.is_whitespace() || ch == '(' || ch == ',').next().unwrap_or("").to_string(),
    }
}

/// CREATE TABLE's text split at its top-level commas, quotes and parentheses respected
fn split_create(sql: &str) -> Create {
    let chars: Vec<char> = sql.chars().collect();
    let (mut i, mut depth, mut quote): (usize, i32, Option<char>) = (0, 0, None);
    let mut parts = Vec::new();
    let mut cur = String::new();
    let mut end = None;
    while i < chars.len() {
        let ch = chars[i];
        match quote {
            Some(q) => {
                if depth >= 1 { cur.push(ch); }
                if ch == q { quote = None; }
            }
            None => match ch {
                '\'' | '"' | '`' => { quote = Some(ch); if depth >= 1 { cur.push(ch); } }
                '[' => { quote = Some(']'); if depth >= 1 { cur.push(ch); } }
                '(' => { depth += 1; if depth > 1 { cur.push(ch); } }
                ')' => {
                    depth -= 1;
                    if depth == 0 { parts.push(cur.trim().to_string()); cur.clear(); end = Some(i); break; }
                    cur.push(ch);
                }
                ',' if depth == 1 => { parts.push(cur.trim().to_string()); cur.clear(); }
                _ => if depth >= 1 { cur.push(ch); },
            },
        }
        i += 1;
    }
    let mut c = Create::default();
    c.options = end.map(|e| chars[e + 1..].iter().collect::<String>().trim().trim_end_matches(';').trim().to_string()).unwrap_or_default();
    for part in parts.into_iter().filter(|p| !p.is_empty()) {
        let head = part.split_whitespace().next().unwrap_or("").to_uppercase();
        if matches!(head.as_str(), "CONSTRAINT" | "PRIMARY" | "UNIQUE" | "CHECK" | "FOREIGN") {
            c.constraints.push(part);
        } else {
            c.columns.push((first_name(&part), part));
        }
    }
    c
}

/// what a column's definition has beyond its name and type that a schema's column says nothing
/// of: UNIQUE, CHECK (...), COLLATE x, AUTOINCREMENT - kept over a rebuild, nothing dropped
fn extras(def: &str, wanted: &str) -> Vec<String> {
    let up = def.to_uppercase();
    let have = wanted.to_uppercase();
    let mut out = Vec::new();
    let word = |w: &str| up.split(|c: char| !c.is_ascii_alphanumeric() && c != '_').any(|t| t == w);
    if word("UNIQUE") && !have.contains("UNIQUE") { out.push("UNIQUE".to_string()); }
    if word("AUTOINCREMENT") && !have.contains("AUTOINCREMENT") && have.contains("PRIMARY KEY") { out.push("AUTOINCREMENT".to_string()); }
    if let Some(i) = up.find("CHECK") {
        if !have.contains("CHECK") {
            // the CHECK and its parenthesized expression
            let rest = &def[i..];
            if let Some(open) = rest.find('(') {
                let mut depth = 0;
                for (j, ch) in rest[open..].char_indices() {
                    match ch { '(' => depth += 1, ')' => { depth -= 1; if depth == 0 { out.push(rest[..open + j + 1].to_string()); break; } } _ => {} }
                }
            }
        }
    }
    if let Some(i) = up.find("COLLATE ") {
        if !have.contains("COLLATE") {
            let name = def[i + 8..].split_whitespace().next().unwrap_or("");
            if !name.is_empty() { out.push(format!("COLLATE {}", name)); }
        }
    }
    out
}

fn q(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn lit(v: &str) -> String {
    format!("'{}'", v.replace('\'', "''"))
}

fn err<E: std::fmt::Display>(e: E, what: &str) -> String {
    format!("sqlite: {}: {}", what, e)
}

/// a value as text: NULL none, a number written out
fn text(v: &Value) -> Option<String> {
    match v {
        Value::Null => None,
        Value::Text(s) => Some(s.clone()),
        Value::Integer(i) => Some(i.to_string()),
        Value::Real(r) => Some(r.to_string()),
        Value::Blob(_) => None,
    }
}

fn int(v: &Value) -> i64 {
    match v {
        Value::Integer(i) => *i,
        Value::Real(r) => *r as i64,
        Value::Text(s) => s.trim().parse().unwrap_or(0),
        _ => 0,
    }
}

/// [turso] a statement's rows, each a row of values
async fn rows<D: SqliteDb>(conn: &D, sql: &str, what: &str) -> Result<Vec<Vec<Value>>, String> {
    conn.rows(sql).await.map_err(|e| err(e, what))
}

/// the connection's databases: main, and every one attached (not temp)
async fn databases<D: SqliteDb>(conn: &D) -> Result<Vec<String>, String> {
    Ok(rows(conn, "PRAGMA database_list", "database_list").await?.iter()
        .filter_map(|r| r.get(1).and_then(text)).filter(|n| n != "temp").collect())
}

/// the database a schema's name is: main for `main` and `public`, else one attached of its name
async fn database_of<D: SqliteDb>(conn: &D, schema: &str) -> Result<String, String> {
    if schema.is_empty() || schema.eq_ignore_ascii_case("main") || schema.eq_ignore_ascii_case("public") {
        return Ok("main".into());
    }
    databases(conn).await?.into_iter().find(|d| d == schema).ok_or_else(|| format!(
        "sqlite: schema {}: no database of that name - main (or public) is the file itself, any other one is ATTACHed first", schema))
}

/// every table of a database, as SQLite has it
async fn read_tables<D: SqliteDb>(conn: &D, db: &str) -> Result<HashMap<String, SqTable>, String> {
    let mut out = HashMap::new();
    let schema = rows(conn, &format!("SELECT type, name, tbl_name, sql FROM {}.sqlite_schema", q(db)), "sqlite_schema").await?;
    let of = |r: &Vec<Value>, i: usize| r.get(i).and_then(text).unwrap_or_default();
    let has_seq = schema.iter().any(|r| of(r, 0) == "table" && of(r, 1) == "sqlite_sequence");
    let mut names: Vec<String> = schema.iter().filter(|r| of(r, 0) == "table" && !of(r, 1).starts_with("sqlite_"))
        .map(|r| of(r, 1)).collect();
    names.sort();
    for name in names {
        let columns = rows(conn, &format!("PRAGMA {}.table_xinfo({})", q(db), q(&name)), "table_xinfo").await?.iter()
            .map(|r| SqColumn {
                name: of(r, 1), decl: of(r, 2), notnull: int(&r[3]) != 0, dflt: r.get(4).and_then(text),
                pk: int(&r[5]), hidden: r.get(6).map(int).unwrap_or(0),
            }).collect();
        let mut indexes = Vec::new();
        for r in rows(conn, &format!("PRAGMA {}.index_list({})", q(db), q(&name)), "index_list").await? {
            let (iname, unique, origin) = (of(&r, 1), int(&r[2]) != 0, of(&r, 3));
            let cols = rows(conn, &format!("PRAGMA {}.index_info({})", q(db), q(&iname)), "index_info").await?.iter()
                .map(|c| of(c, 2)).collect();
            let sql = schema.iter().find(|s| of(s, 0) == "index" && of(s, 1) == iname).and_then(|s| s.get(3).and_then(text));
            indexes.push(SqIndex { name: iname, unique, origin, columns: cols, sql });
        }
        let fks = rows(conn, &format!("PRAGMA {}.foreign_key_list({})", q(db), q(&name)), "foreign_key_list").await?.iter()
            .map(|r| SqFk { table: of(r, 2), from: of(r, 3), to: of(r, 4) }).collect();
        let mut triggers: Vec<(String, String)> = schema.iter().filter(|r| of(r, 0) == "trigger" && of(r, 2) == name)
            .map(|r| (of(r, 1), of(r, 3))).collect();
        triggers.sort();
        let create_sql = schema.iter().find(|r| of(r, 0) == "table" && of(r, 1) == name).map(|r| of(r, 3)).unwrap_or_default();
        let seq = if has_seq {
            rows(conn, &format!("SELECT seq FROM {}.sqlite_sequence WHERE name = {}", q(db), lit(&name)), "sqlite_sequence").await?
                .first().and_then(|r| r.first()).map(int)
        } else { None };
        // [turso] the views on it, made again around a rebuild: dropped first, as SQLite's own
        // way does (lang_altertable, its step on views)
        let views = schema.iter().filter(|r| of(r, 0) == "view" && mentions(&of(r, 3), &name))
            .map(|r| (of(r, 1), of(r, 3))).collect();
        out.insert(name.clone(), SqTable { name, columns, indexes, fks, triggers, create: split_create(&create_sql), seq, views });
    }
    Ok(out)
}

/// [turso] an index's, trigger's or view's SQL as SQLite keeps it (its name not qualified) made in
/// `db`: unqualified it would go to main [review 2026-11-19: an attached database's rebuild
/// made its indexes and triggers again in main]
fn in_db(sql: &str, db: &str) -> String {
    if db == "main" {
        return sql.to_string();
    }
    let re = regex::Regex::new(r#"(?is)^(\s*CREATE\s+(?:UNIQUE\s+)?(?:TEMP\s+|TEMPORARY\s+)?(?:INDEX|TRIGGER|VIEW)\s+(?:IF\s+NOT\s+EXISTS\s+)?)("[^"]+"|\[[^\]]+\]|`[^`]+`|[A-Za-z_][A-Za-z0-9_$]*)(\s*\.)?"#).unwrap();
    match re.captures(sql) {
        Some(c) if c.get(3).is_none() => format!("{}{}.{}{}", &c[1], q(db), &c[2], &sql[c.get(0).unwrap().end()..]),
        _ => sql.to_string(),
    }
}

/// does a view's SQL name the table: as a word, quoted or not
fn mentions(sql: &str, table: &str) -> bool {
    let low = sql.to_lowercase();
    let t = table.to_lowercase();
    low.split(|c: char| !(c.is_alphanumeric() || c == '_')).any(|w| w == t)
}

/// [turso] the rows of `db`.`table` and of the tables naming it whose foreign key finds no row:
/// SQLite's foreign_key_check, which turso has not
async fn fk_broken<D: SqliteDb>(conn: &D, db: &str, table: &str) -> Result<i64, String> {
    let all = read_tables(conn, db).await?;
    let mut bad = 0;
    for (child, t) in &all {
        for f in t.fks.iter().filter(|f| child == table || f.table == table) {
            let parent = match all.get(&f.table) { Some(p) => p, None => continue };
            let to = if !f.to.is_empty() { f.to.clone() } else {
                parent.columns.iter().find(|c| c.pk == 1).map(|c| c.name.clone()).unwrap_or_else(|| "rowid".into())
            };
            let sql = format!("SELECT count(*) FROM {d}.{c} WHERE {k} IS NOT NULL AND {k} NOT IN (SELECT {to} FROM {d}.{p})",
                              d = q(db), c = q(child), k = q(&f.from), to = q(&to), p = q(&f.table));
            bad += rows(conn, &sql, "foreign keys").await?.first().and_then(|r| r.first()).map(int).unwrap_or(0);
        }
    }
    Ok(bad)
}


/// The catalog read into the schema's model, a schema a database (main, and each attached):
/// what a snapshot of SQLite writes
pub async fn load_info_schema<D: SqliteDb>(conn: &D) -> Result<InfoSchemaType, String> {
    let mut out = InfoSchemaType::new();
    for db in databases(conn).await? {
        let mut tables = HashMap::new();
        for (i, (name, t)) in sorted(read_tables(conn, &db).await?).into_iter().enumerate() {
            let mut columns = HashMap::new();
            for (n, c) in t.columns.iter().filter(|c| c.hidden == 0).enumerate() {
                columns.insert(c.name.clone(), PgColumnDfn {
                    column_name: c.name.clone(), column_type: c.decl.to_lowercase(), column_default: c.dflt.clone(),
                    sql: None, fk: None, pk: c.pk > 0, nullable: !c.notnull, sort_order: n + 1, column_comment: None,
                });
            }
            let mut pk: Vec<&SqColumn> = t.columns.iter().filter(|c| c.pk > 0).collect();
            pk.sort_by_key(|c| c.pk);
            let primary_key = (!pk.is_empty()).then(|| PgPrimaryKey {
                constraint_name: format!("{}_pkey", name), columns: pk.iter().map(|c| c.name.clone()).collect() });
            let mut fks = HashMap::new();
            for f in &t.fks {
                if let Some(c) = columns.get_mut(&f.from) {
                    c.fk = Some((f.table.clone(), String::new()));
                }
                fks.insert(f.from.clone(), FKTable {
                    schema: db.clone(), table: f.table.clone(), column: [f.from.clone()].into_iter().collect(),
                    name: format!("{}_{}_fkey", name, f.from), sql: String::new() });
            }
            let indexes = t.indexes.iter().filter(|x| x.origin != "pk").map(|x| (x.name.clone(), PgIndex {
                index_name: x.name.clone(), is_unique: x.unique, index_method: "btree".into(),
                columns: x.columns.iter().map(|c| PgIndexColumn {
                    column_name: c.clone(), order: String::new(), nulls: String::new(), collation: String::new() }).collect(),
            })).collect();
            let triggers = t.triggers.iter().map(|(n, sql)| (n.clone(), PgTrigger {
                trigger_name: n.clone(), event: String::new(), orientation: String::new(), proc: sql.clone() })).collect();
            tables.insert(name.clone(), PgTable {
                table_name: name, columns, fks, triggers, indexes, grants: HashMap::new(), primary_key,
                sort_order: i + 1, table_comment: None, owner: None, security: None });
        }
        out.insert(db, tables);
    }
    Ok(out)
}

fn sorted(m: HashMap<String, SqTable>) -> Vec<(String, SqTable)> {
    let mut v: Vec<_> = m.into_iter().collect();
    v.sort_by(|a, b| a.0.cmp(&b.0));
    v
}

fn norm(s: &str) -> String {
    // [review 2026-11-18] varchar( 80) is varchar(80): no space by a parenthesis or a comma
    let s = s.split_whitespace().collect::<Vec<_>>().join(" ").to_lowercase();
    s.replace("( ", "(").replace(" )", ")").replace(" ,", ",").replace(", ", ",").replace(" (", "(")
}

/// a default as compared: its spaces collapsed; its case kept inside a quoted string, else not
/// [review 2026-11-18]
fn norm_default(s: &str) -> String {
    let s = s.split_whitespace().collect::<Vec<_>>().join(" ");
    if s.contains('\'') { s } else { s.to_lowercase() }
}

/// a default SQLite takes as written: a literal, a signed number, CURRENT_* or one in parentheses
fn default_sql(d: &str) -> String {
    let t = d.trim();
    let plain = t.starts_with('(') || t.starts_with('\'') || t.parse::<f64>().is_ok()
        || matches!(t.to_uppercase().as_str(), "NULL" | "TRUE" | "FALSE" | "CURRENT_TIME" | "CURRENT_DATE" | "CURRENT_TIMESTAMP");
    if plain { t.to_string() } else { format!("({})", t) }
}

/// a default constant enough for ADD COLUMN: no CURRENT_*, no expression
fn constant_default(d: &str) -> bool {
    let t = d.trim();
    t.starts_with('\'') || t.parse::<f64>().is_ok() || matches!(t.to_uppercase().as_str(), "NULL" | "TRUE" | "FALSE")
}

fn is_pk(c: &Column) -> bool {
    c.constraint.as_ref().and_then(|x| x.primary_key).unwrap_or(false)
}

fn nullable(c: &Column) -> bool {
    c.constraint.as_ref().map(|x| x.nullable).unwrap_or(true) && !(is_pk(c) && c.constraint.as_ref().map(|x| !x.nullable).unwrap_or(false))
}

/// the table a foreign key names, in this database: `schema.table` or `table`
fn fk_table(c: &Column) -> Option<String> {
    let r = c.constraint.as_ref()?.foreign_key.as_ref()?.references.trim().to_string();
    if r.is_empty() {
        return None;
    }
    Some(r.rsplit('.').next().unwrap_or(&r).to_string())
}

/// a column of the schema as SQLite's column definition; `inline_pk` when it is the table's
/// one key column
fn column_def(c: &Column, inline_pk: bool) -> String {
    let mut s = format!("{} {}", q(&c.name), c.column_type.trim());
    if inline_pk {
        s.push_str(" PRIMARY KEY");
    }
    if !nullable(c) {
        s.push_str(" NOT NULL");
    }
    if let Some(d) = c.default_value.as_ref().filter(|d| !d.trim().is_empty()) {
        s.push_str(&format!(" DEFAULT {}", default_sql(d)));
    }
    if let Some(t) = fk_table(c) {
        s.push_str(&format!(" REFERENCES {}", q(&t)));
        if let Some(sql) = c.constraint.as_ref().and_then(|x| x.foreign_key.as_ref()).map(|f| f.sql.trim()).filter(|s| !s.is_empty()) {
            s.push(' ');
            s.push_str(sql);
        }
    }
    if !c.sql.trim().is_empty() {
        s.push(' ');
        s.push_str(c.sql.trim());
    }
    s
}

/// a column SQLite has that the schema does not name, kept in a rebuild as SQLite has it:
/// its whole definition - UNIQUE, a key, CHECK, COLLATE, GENERATED - as its CREATE TABLE wrote it
/// [review 2026-11-18]
fn kept_def(c: &SqColumn, create: &Create) -> String {
    if let Some((_, def)) = create.columns.iter().find(|(n, _)| n == &c.name) {
        return def.clone();
    }
    let mut s = format!("{} {}", q(&c.name), c.decl);
    if c.notnull {
        s.push_str(" NOT NULL");
    }
    if let Some(d) = &c.dflt {
        s.push_str(&format!(" DEFAULT {}", d));
    }
    s
}

/// the CREATE TABLE of the schema's table (and the columns SQLite has that it has not), under
/// `as_name`; `was` the table's CREATE TABLE when it is rebuilt: what of it the schema does not
/// say is kept - a named column's UNIQUE, CHECK, COLLATE, AUTOINCREMENT, the table's own
/// constraints but a key the schema gives, its options (WITHOUT ROWID, STRICT)
fn create_table(db: &str, as_name: &str, t: &Table, kept: &[&SqColumn], was: Option<&Create>) -> String {
    let pks: Vec<&Column> = t.columns.list.iter().filter(|c| is_pk(c)).collect();
    let mut parts: Vec<String> = t.columns.list.iter().map(|c| {
        let mut d = column_def(c, pks.len() == 1 && is_pk(c));
        if let Some(orig) = was.and_then(|w| w.columns.iter().find(|(n, _)| n == &c.name)) {
            for x in extras(&orig.1, &d) {
                d.push(' ');
                d.push_str(&x);
            }
        }
        d
    }).collect();
    if let Some(w) = was {
        parts.extend(kept.iter().map(|c| kept_def(c, w)));
    }
    if pks.len() > 1 {
        parts.push(format!("PRIMARY KEY ({})", pks.iter().map(|c| q(&c.name)).collect::<Vec<_>>().join(", ")));
    }
    if let Some(w) = was {
        let named_fk = |cst: &str| t.columns.list.iter().any(|c| fk_table(c).is_some()
            && cst.to_uppercase().contains("FOREIGN") && cst.contains(&c.name));
        for cst in &w.constraints {
            let up = cst.to_uppercase();
            let is_key = up.starts_with("PRIMARY") || (up.starts_with("CONSTRAINT") && up.contains("PRIMARY KEY"));
            if (is_key && !pks.is_empty()) || named_fk(cst) {
                continue; // the schema gives it
            }
            parts.push(cst.clone());
        }
    }
    if !t.constraint.trim().is_empty() && !t.constraint.trim().starts_with("--") {
        parts.push(t.constraint.trim().to_string());
    }
    let mut s = format!("CREATE TABLE {}.{} (\n  {}\n)", q(db), q(as_name), parts.join(",\n  "));
    if !t.sql.trim().is_empty() && !t.sql.trim().starts_with("--") {
        s.push(' ');
        s.push_str(t.sql.trim());
    } else if let Some(w) = was.filter(|w| !w.options.is_empty()) {
        s.push(' ');
        s.push_str(&w.options);
    }
    s
}

/// the indexes of the schema's columns: (name, unique, column)
fn wanted_indexes(t: &Table) -> Vec<(String, bool, String)> {
    t.columns.list.iter().filter_map(|c| c.index.as_ref().map(|i| {
        let name = if i.name.is_empty() { format!("{}_{}_idx", t.table_name, c.name) } else { i.name.clone() };
        (name, i.unique.unwrap_or(false), c.name.clone())
    })).collect()
}

fn create_index(db: &str, table: &str, name: &str, unique: bool, column: &str) -> String {
    format!("CREATE {}INDEX IF NOT EXISTS {}.{} ON {} ({})", if unique { "UNIQUE " } else { "" }, q(db), q(name), q(table), q(column))
}

/// What a deploy does: its statements, and whether a table is rebuilt (foreign keys off around it)
#[derive(Debug, Default, Clone)]
pub struct SqlitePlan {
    pub statements: Vec<String>,
    pub rebuilt: Vec<String>,
    pub skipped: Vec<String>,
    /// the tables rebuilt, (database, table): their foreign keys checked before the commit
    pub rebuilt_tables: Vec<(String, String)>,
}

/// The statements that make the database what the YAML schema says, without running them
pub async fn plan<D: SqliteDb>(schema: Yaml, conn: &D, file_name: &str, opt: &MigrationOptions) -> Result<SqlitePlan, String> {
    let schemas = crate::parse_yaml_schema(schema, file_name)?;
    let mut p = SqlitePlan::default();
    for s in &schemas.list {
        plan_schema(s, conn, opt, &mut p).await?;
    }
    Ok(p)
}

/// The YAML schema deployed: planned, then run in one transaction - foreign keys off around a
/// rebuild and checked before the commit. With `plan_only` nothing is run. The statements are
/// returned either way
pub async fn migrate<D: SqliteDb>(schema: Yaml, conn: &D, plan_only: bool, file_name: &str, opt: &MigrationOptions)
    -> Result<SqlitePlan, String> {
    let p = plan(schema, conn, file_name, opt).await?;
    if plan_only || p.statements.is_empty() {
        return Ok(p);
    }
    if !conn.autocommit().map_err(|e| err(e, "autocommit"))? {
        // [review 2026-11-18] said, not SQLite's "cannot start a transaction within a transaction"
        return Err("sqlite: the connection is in a transaction: migrate runs its own, and turns foreign keys \
                    off around a rebuild, which only works outside one".into());
    }
    let fk_on = rows(conn, "PRAGMA foreign_keys", "foreign_keys").await?.first().and_then(|r| r.first()).map(int).unwrap_or(0) != 0;
    let off = fk_on && !p.rebuilt.is_empty();
    if off {
        // only outside a transaction does it change anything
        conn.batch("PRAGMA foreign_keys = OFF").await.map_err(|e| err(e, "foreign_keys off"))?;
    }
    conn.batch("BEGIN").await.map_err(|e| err(e, "begin"))?;
    let result = async {
        for st in &p.statements {
            conn.batch(st).await.map_err(|e| format!("sqlite: {}\n  in: {}", e, st))?;
        }
        let mut bad = 0;
        for (db, table) in &p.rebuilt_tables {
            bad += fk_broken(conn, db, table).await?;
        }
        if bad > 0 {
            return Err(format!("sqlite: {} row(s) break a foreign key after rebuilding {}: nothing changed",
                               bad, p.rebuilt.join(", ")));
        }
        conn.batch("COMMIT").await.map_err(|e| err(e, "commit"))
    }.await;
    if result.is_err() {
        let _ = conn.batch("ROLLBACK").await;
    }
    if off {
        let _ = conn.batch("PRAGMA foreign_keys = ON").await;
    }
    result.map(|_| p)
}

async fn plan_schema<D: SqliteDb>(s: &Schema, conn: &D, opt: &MigrationOptions, p: &mut SqlitePlan) -> Result<(), String> {
    let db = database_of(conn, &s.schema_name).await?;
    let existing = read_tables(conn, &db).await?;
    // [review 2026-11-19] a schema's owner reads as its name when none is given (Schema::new):
    // only one of its own is said as left out
    if !s.owner.is_empty() && s.owner != s.schema_name {
        p.skipped.push(format!("schema {}: its owner (SQLite has no users)", s.schema_name));
    }
    for t in &s.tables.list {
        if t.is_template == Some(true) {
            continue;
        }
        if t.columns.list.iter().any(|c| c.partition_by.is_some()) {
            return Err(format!("sqlite: table {}: a partition - SQLite has none", t.table_name));
        }
        if !t.triggers.list.is_empty() && !opt.exclude_triggers {
            return Err(format!("sqlite: table {}: its triggers are a function's (PostgreSQL's form); SQLite's are SQL of \
                               their own - leave them out, or set exclude_triggers", t.table_name));
        }
        if !t.grant.is_empty() {
            p.skipped.push(format!("table {}: its grants (SQLite has no users)", t.table_name));
        }
        match existing.get(&t.table_name) {
            None => {
                p.statements.push(create_table(&db, &t.table_name, t, &[], None));
                for (name, unique, col) in wanted_indexes(t) {
                    p.statements.push(create_index(&db, &t.table_name, &name, unique, &col));
                }
                if !t.data.is_empty() {
                    let cols: Vec<String> = t.columns.list.iter().map(|c| q(&c.name)).collect();
                    for row in &t.data {
                        let n = row.len().min(cols.len());
                        p.statements.push(format!("INSERT INTO {}.{} ({}) VALUES ({})", q(&db), q(&t.table_name),
                            cols[..n].join(", "), row[..n].iter().map(|v| lit(v)).collect::<Vec<_>>().join(", ")));
                    }
                }
            }
            Some(have) => plan_table(&db, t, have, &existing.keys().cloned().collect(), opt, p)?,
        }
    }
    Ok(())
}

fn plan_table(db: &str, t: &Table, have: &SqTable, existing_names: &std::collections::HashSet<String>,
              opt: &MigrationOptions, p: &mut SqlitePlan) -> Result<(), String> {
    let by_name: HashMap<&str, &SqColumn> = have.columns.iter().map(|c| (c.name.as_str(), c)).collect();
    let mut rebuild: Vec<String> = Vec::new();
    let mut adds: Vec<String> = Vec::new();
    for c in &t.columns.list {
        match by_name.get(c.name.as_str()) {
            None => {
                let d = c.default_value.as_ref().filter(|d| !d.trim().is_empty());
                // [review 2026-11-18] a column of its own SQL (UNIQUE, a key ...) by a rebuild too
                let addable = !is_pk(c) && c.index.as_ref().and_then(|i| i.unique) != Some(true) && c.sql.trim().is_empty()
                    && (nullable(c) || d.map(|d| constant_default(d) && d.trim().to_uppercase() != "NULL").unwrap_or(false))
                    && d.map(|d| constant_default(d)).unwrap_or(true)
                    && (fk_table(c).is_none() || d.is_none());
                if addable {
                    adds.push(format!("ALTER TABLE {}.{} ADD COLUMN {}", q(db), q(&t.table_name), column_def(c, false)));
                } else {
                    rebuild.push(format!("{} added (a key, unique, NOT NULL with no constant default, or a default of an expression)", c.name));
                }
            }
            Some(h) => {
                let (want, got) = (norm(&c.column_type), norm(&h.decl));
                if want != got {
                    match analyze_type_change(&got, &want) {
                        TypeChange::NoChange => {}
                        TypeChange::SizeExtension | TypeChange::Compatible => rebuild.push(format!("{}: type {} to {}", c.name, got, want)),
                        TypeChange::SizeReduction | TypeChange::Incompatible => {
                            if opt.with_size_cut {
                                rebuild.push(format!("{}: type {} to {} (with_size_cut)", c.name, got, want));
                            } else if opt.without_failfast {
                                p.skipped.push(format!("table {}: column {} type {} to {}: narrower or incompatible, needs with_size_cut",
                                                       t.table_name, c.name, got, want));
                            } else {
                                return Err(format!("sqlite: table {}: column {} type {} to {} is narrower or incompatible: set with_size_cut",
                                                   t.table_name, c.name, got, want));
                            }
                        }
                    }
                }
                // [review 2026-11-19] a key's nullability as SQLite reports it is no change: its
                // INTEGER PRIMARY KEY says notnull 0 - a snapshot's `nullable: false` on a key rebuilt it
                if nullable(c) == h.notnull && !is_pk(c) {
                    rebuild.push(format!("{}: {}", c.name, if nullable(c) { "NOT NULL dropped" } else { "NOT NULL" }));
                }
                let want_d = c.default_value.as_ref().map(|d| d.trim()).filter(|d| !d.is_empty()).map(default_sql).map(|d| norm_default(&d));
                let got_d = h.dflt.as_ref().map(|d| norm_default(d));
                if want_d != got_d {
                    rebuild.push(format!("{}: default {:?} to {:?}", c.name, got_d, want_d));
                }
                if is_pk(c) != (h.pk > 0) {
                    rebuild.push(format!("{}: primary key {}", c.name, if is_pk(c) { "made" } else { "dropped" }));
                }
                let want_fk = fk_table(c);
                let got_fk = have.fks.iter().find(|f| f.from == c.name).map(|f| f.table.clone());
                if want_fk.is_some() && want_fk != got_fk {
                    rebuild.push(format!("{}: foreign key to {}", c.name, want_fk.clone().unwrap_or_default()));
                }
            }
        }
    }
    let wanted = wanted_indexes(t);
    if !rebuild.is_empty() {
        // [sqlite] its own table rebuild: the new one under a temporary name, the rows over, the
        // old one dropped, the new one renamed, its indexes and triggers made again
        let schema_cols: std::collections::HashSet<&str> = t.columns.list.iter().map(|c| c.name.as_str()).collect();
        // [review 2026-11-18] a generated column (hidden 2, 3) the schema does not name is kept too,
        // and not copied: SQLite computes it
        let kept: Vec<&SqColumn> = have.columns.iter().filter(|c| c.hidden != 1 && !schema_cols.contains(c.name.as_str())).collect();
        let tmp = format!("_sg_new_{}", t.table_name);
        if existing_names.contains(&tmp) {
            return Err(format!("sqlite: table {}: a table {} is in the way of its rebuild - one left by an earlier one? drop it",
                               t.table_name, tmp));
        }
        p.statements.push(create_table(db, &tmp, t, &kept, Some(&have.create)));
        let common: Vec<String> = have.columns.iter().filter(|c| c.hidden == 0)
            .filter(|c| schema_cols.contains(c.name.as_str()) || kept.iter().any(|k| k.name == c.name))
            .map(|c| q(&c.name)).collect();
        p.statements.push(format!("INSERT INTO {}.{} ({}) SELECT {} FROM {}.{}", q(db), q(&tmp), common.join(", "),
                                  common.join(", "), q(db), q(&t.table_name)));
        // [turso] the views on it dropped first and made again after, SQLite's own step on views:
        // a view naming a dropped table fails SQLite's rename, and turso has no legacy_alter_table
        for (v, _) in &have.views {
            p.statements.push(format!("DROP VIEW {}.{}", q(db), q(v)));
        }
        p.statements.push(format!("DROP TABLE {}.{}", q(db), q(&t.table_name)));
        p.statements.push(format!("ALTER TABLE {}.{} RENAME TO {}", q(db), q(&tmp), q(&t.table_name)));
        for (_, sql) in &have.views {
            p.statements.push(in_db(sql, db));
        }
        // [review 2026-11-18] AUTOINCREMENT's counter as it was: a number handed out once is never again
        if let Some(n) = have.seq {
            p.statements.push(format!("UPDATE {db}.sqlite_sequence SET seq = max(seq, {n}) WHERE name = {t}; \
                INSERT INTO {db}.sqlite_sequence (name, seq) SELECT {t}, {n} WHERE NOT EXISTS \
                (SELECT 1 FROM {db}.sqlite_sequence WHERE name = {t})", db = q(db), n = n, t = lit(&t.table_name)));
        }
        for x in &have.indexes {
            if let (Some(sql), "c") = (&x.sql, x.origin.as_str()) {
                if !wanted.iter().any(|(n, _, _)| n == &x.name) {
                    p.statements.push(in_db(sql, db));
                }
            }
        }
        for (_, sql) in &have.triggers {
            p.statements.push(in_db(sql, db));
        }
        for (name, unique, col) in &wanted {
            p.statements.push(create_index(db, &t.table_name, name, *unique, col));
        }
        p.rebuilt.push(format!("{} ({})", t.table_name, rebuild.join("; ")));
        p.rebuilt_tables.push((db.to_string(), t.table_name.clone()));
        return Ok(());
    }
    p.statements.extend(adds);
    for (name, unique, col) in wanted {
        match have.indexes.iter().find(|x| x.name == name) {
            None => p.statements.push(create_index(db, &t.table_name, &name, unique, &col)),
            Some(x) if x.unique != unique || x.columns != vec![col.clone()] => {
                if opt.with_index_drop {
                    p.statements.push(format!("DROP INDEX {}.{}", q(db), q(&name)));
                    p.statements.push(create_index(db, &t.table_name, &name, unique, &col));
                } else if opt.without_failfast {
                    p.skipped.push(format!("table {}: index {} changed: needs with_index_drop", t.table_name, name));
                } else {
                    return Err(format!("sqlite: table {}: index {} changed: set with_index_drop", t.table_name, name));
                }
            }
            Some(_) => {}
        }
    }
    Ok(())
}

#[cfg(all(test, feature = "sqlite"))]
mod tests {
    use super::*;
    use turso::Connection;
    use yaml_rust2::YamlLoader;

    fn yaml(s: &str) -> Yaml {
        crate::load_schema_from_src(s.to_string()).unwrap()
    }

    const BASE: &str = "
database:
  - schema:
    schemaName: public
    tables:
      - table:
          tableName: customers
          columns:
            - column:
                name: id
                type: integer
                constraint:
                  primaryKey: true
            - column:
                name: email
                type: varchar(80)
                constraint:
                  nullable: false
                index:
                  name: customers_email_key
                  unique: true
          data:
            - ['1', 'a@x.com']
            - ['2', 'it''s@x.com']
      - table:
          tableName: orders
          columns:
            - column:
                name: id
                type: integer
                constraint:
                  primaryKey: true
            - column:
                name: customer
                type: integer
                constraint:
                  foreignKey:
                    references: public.customers
            - column:
                name: placed
                type: text
                defaultValue: CURRENT_TIMESTAMP
";

    fn opt() -> MigrationOptions {
        MigrationOptions::default()
    }

    async fn info(conn: &Connection) -> InfoSchemaType {
        load_info_schema(conn).await.unwrap()
    }

    /// [turso] a database in memory, attaching allowed
    async fn mem() -> Connection {
        turso::Builder::new_local(":memory:").experimental_attach(true).build().await.unwrap().connect().unwrap()
    }

    async fn try_exec(c: &Connection, sql: &str) -> Result<(), turso::Error> {
        c.execute_batch(sql).await
    }

    async fn exec(c: &Connection, sql: &str) {
        c.execute_batch(sql).await.unwrap_or_else(|e| panic!("{}: {}", sql, e))
    }

    async fn i64s(c: &Connection, sql: &str) -> Vec<i64> {
        rows(c, sql, "test").await.unwrap().iter().map(|r| int(&r[0])).collect()
    }

    async fn i64_of(c: &Connection, sql: &str) -> i64 {
        i64s(c, sql).await[0]
    }

    async fn str_of(c: &Connection, sql: &str) -> String {
        match &rows(c, sql, "test").await.unwrap()[0][0] { Value::Text(t) => t.clone(), _ => String::new() }
    }

    #[tokio::test]
    async fn a_schema_is_made_and_read_back() {
        let c = mem().await;
        let p = migrate(yaml(BASE), &c, false, "S1__a.yaml", &opt()).await.unwrap();
        assert!(p.rebuilt.is_empty());
        let i = info(&c).await;
        let cu = &i["main"]["customers"];
        assert!(cu.columns["id"].pk);
        assert_eq!(cu.columns["email"].column_type, "varchar(80)");
        assert!(!cu.columns["email"].nullable);
        assert!(cu.indexes["customers_email_key"].is_unique);
        let o = &i["main"]["orders"];
        assert_eq!(o.fks["customer"].table, "customers");
        assert_eq!(o.columns["placed"].column_default.as_deref(), Some("CURRENT_TIMESTAMP"));
        // the rows, a quote in one of them
        let n: i64 = i64_of(&c, "SELECT count(*) FROM customers WHERE email = 'it''s@x.com'").await;
        assert_eq!(n, 1);
    }

    #[tokio::test]
    async fn a_second_run_changes_nothing() {
        let c = mem().await;
        migrate(yaml(BASE), &c, false, "S1__a.yaml", &opt()).await.unwrap();
        let p = migrate(yaml(BASE), &c, false, "S1__a.yaml", &opt()).await.unwrap();
        assert!(p.statements.is_empty(), "{:?}", p.statements);
    }

    #[tokio::test]
    async fn a_nullable_column_is_added_with_alter() {
        let c = mem().await;
        migrate(yaml(BASE), &c, false, "S1__a.yaml", &opt()).await.unwrap();
        let more = BASE.replace("                defaultValue: CURRENT_TIMESTAMP\n",
            "                defaultValue: CURRENT_TIMESTAMP\n            - column:\n                name: note\n                type: text\n");
        let p = migrate(yaml(&more), &c, false, "S2__a.yaml", &opt()).await.unwrap();
        assert_eq!(p.statements.len(), 1, "{:?}", p.statements);
        assert!(p.statements[0].starts_with("ALTER TABLE \"main\".\"orders\" ADD COLUMN \"note\" text"));
        assert!(p.rebuilt.is_empty());
    }

    #[tokio::test]
    async fn what_alter_cannot_do_is_a_rebuild_that_keeps_rows_indexes_triggers_and_columns() {
        let c = mem().await;
        migrate(yaml(BASE), &c, false, "S1__a.yaml", &opt()).await.unwrap();
        exec(&c, "INSERT INTO orders (id, customer) VALUES (10, 1);
                         ALTER TABLE orders ADD COLUMN extra text;
                         UPDATE orders SET extra = 'kept';
                         CREATE INDEX orders_extra ON orders (extra);
                         CREATE TABLE audit (n integer);
                         CREATE TRIGGER orders_audit AFTER INSERT ON orders BEGIN INSERT INTO audit VALUES (1); END;").await;
        // [turso] foreign keys are off by default there: on, to see them off around the rebuild and back
        exec(&c, "PRAGMA foreign_keys = ON").await;
        let fk_before: i64 = i64_of(&c, "PRAGMA foreign_keys").await;
        // placed made NOT NULL: no ALTER for it in SQLite
        let more = BASE.replace("                type: text\n                defaultValue: CURRENT_TIMESTAMP\n",
                                "                type: text\n                defaultValue: CURRENT_TIMESTAMP\n                constraint:\n                  nullable: false\n");
        let p = migrate(yaml(&more), &c, false, "S2__a.yaml", &opt()).await.unwrap();
        assert_eq!(p.rebuilt.len(), 1, "{:?}", p);
        assert!(p.rebuilt[0].contains("placed: NOT NULL"), "{:?}", p.rebuilt);
        let i = info(&c).await;
        let o = &i["main"]["orders"];
        assert!(!o.columns["placed"].nullable);
        // the row, the column the schema does not name, its index, the trigger, the foreign key
        let extra: String = str_of(&c, "SELECT extra FROM orders WHERE id = 10").await;
        assert_eq!(extra, "kept");
        assert!(o.indexes.contains_key("orders_extra"));
        assert!(o.triggers.contains_key("orders_audit"));
        assert_eq!(o.fks["customer"].table, "customers");
        exec(&c, "INSERT INTO orders (id, customer) VALUES (11, 2)").await;
        let audited: i64 = i64_of(&c, "SELECT count(*) FROM audit").await;
        assert_eq!(audited, 1);
        // foreign keys back as they were, off only around it
        let fk: i64 = i64_of(&c, "PRAGMA foreign_keys").await;
        assert_eq!(fk, fk_before);
        assert_eq!(fk_before, 1);
    }

    /// [turso] its foreign keys checked by hand before the commit (turso has no foreign_key_check):
    /// a rebuild that leaves a row whose parent is not there changes nothing
    #[tokio::test]
    async fn a_rebuild_over_a_row_with_no_parent_is_refused() {
        let c = mem().await;
        migrate(yaml(BASE), &c, false, "S1__a.yaml", &opt()).await.unwrap();
        exec(&c, "INSERT INTO orders (id, customer) VALUES (10, 99)").await;
        let more = BASE.replace("                type: text\n                defaultValue: CURRENT_TIMESTAMP\n",
                                "                type: text\n                defaultValue: CURRENT_TIMESTAMP\n                constraint:\n                  nullable: false\n");
        let e = migrate(yaml(&more), &c, false, "S2__a.yaml", &opt()).await.unwrap_err();
        assert!(e.contains("1 row(s) break a foreign key"), "{}", e);
        assert!(info(&c).await["main"]["orders"].columns["placed"].nullable, "rolled back");
    }

    /// [turso] an attached database's table rebuilt: its index, trigger and view made again in it,
    /// not in main
    #[tokio::test]
    async fn a_rebuild_in_an_attached_database_makes_its_objects_there() {
        let c = mem().await;
        exec(&c, "ATTACH DATABASE ':memory:' AS shop").await;
        let other = BASE.replace("schemaName: public", "schemaName: shop");
        migrate(yaml(&other), &c, false, "S1__a.yaml", &opt()).await.unwrap();
        exec(&c, "CREATE INDEX shop.orders_placed ON orders (placed);
                  CREATE TABLE shop.audit (n integer);
                  CREATE TRIGGER shop.orders_audit AFTER INSERT ON orders BEGIN INSERT INTO audit VALUES (1); END;
                  CREATE VIEW shop.placed AS SELECT placed FROM orders;").await;
        let more = other.replace("                type: text\n                defaultValue: CURRENT_TIMESTAMP\n",
                                 "                type: text\n                defaultValue: CURRENT_TIMESTAMP\n                constraint:\n                  nullable: false\n");
        let p = migrate(yaml(&more), &c, false, "S2__a.yaml", &opt()).await.unwrap();
        assert_eq!(p.rebuilt.len(), 1, "{:?}", p);
        let i = info(&c).await;
        assert!(i["shop"]["orders"].indexes.contains_key("orders_placed"));
        assert!(i["shop"]["orders"].triggers.contains_key("orders_audit"));
        assert!(i["main"].is_empty(), "nothing in main: {:?}", i["main"].keys().collect::<Vec<_>>());
        exec(&c, "INSERT INTO shop.orders (id, customer) VALUES (1, 1)").await;
        assert_eq!(i64_of(&c, "SELECT count(*) FROM shop.audit").await, 1);
        assert_eq!(i64_of(&c, "SELECT count(*) FROM shop.placed").await, 1);
    }

    #[tokio::test]
    async fn a_narrower_type_needs_with_size_cut() {
        let c = mem().await;
        migrate(yaml(BASE), &c, false, "S1__a.yaml", &opt()).await.unwrap();
        let narrow = BASE.replace("type: varchar(80)", "type: varchar(20)");
        let e = migrate(yaml(&narrow), &c, false, "S2__a.yaml", &opt()).await.unwrap_err();
        assert!(e.contains("narrower or incompatible: set with_size_cut"), "{}", e);
        let cut = MigrationOptions { with_size_cut: true, ..MigrationOptions::default() };
        let p = migrate(yaml(&narrow), &c, false, "S2__a.yaml", &cut).await.unwrap();
        assert_eq!(p.rebuilt.len(), 1);
        assert_eq!(info(&c).await["main"]["customers"].columns["email"].column_type, "varchar(20)");
        // the unique index made again, the rows kept
        assert!(info(&c).await["main"]["customers"].indexes["customers_email_key"].is_unique);
        let n: i64 = i64_of(&c, "SELECT count(*) FROM customers").await;
        assert_eq!(n, 2);
        // a wider one goes with no option
        let wide = BASE.replace("type: varchar(80)", "type: varchar(200)");
        assert_eq!(migrate(yaml(&wide), &c, false, "S3__a.yaml", &opt()).await.unwrap().rebuilt.len(), 1);
    }

    #[tokio::test]
    async fn a_plan_runs_nothing() {
        let c = mem().await;
        let p = migrate(yaml(BASE), &c, true, "S1__a.yaml", &opt()).await.unwrap();
        assert!(p.statements.iter().any(|s| s.starts_with("CREATE TABLE \"main\".\"customers\"")), "{:?}", p.statements);
        assert!(info(&c).await["main"].is_empty());
    }

    #[tokio::test]
    async fn a_failed_statement_changes_nothing() {
        let c = mem().await;
        migrate(yaml(BASE), &c, false, "S1__a.yaml", &opt()).await.unwrap();
        exec(&c, "INSERT INTO orders (id, customer) VALUES (10, 1)").await;
        // a NOT NULL column with no default over a row: the rebuild's copy fails, all rolled back
        let more = BASE.replace("                defaultValue: CURRENT_TIMESTAMP\n",
            "                defaultValue: CURRENT_TIMESTAMP\n            - column:\n                name: must\n                type: text\n                constraint:\n                  nullable: false\n");
        let e = migrate(yaml(&more), &c, false, "S2__a.yaml", &opt()).await.unwrap_err();
        assert!(e.contains("NOT NULL constraint failed"), "{}", e);
        assert!(!info(&c).await["main"]["orders"].columns.contains_key("must"));
        assert!(!info(&c).await["main"].contains_key("_sg_new_orders"));
    }

    #[tokio::test]
    async fn a_schema_is_main_or_an_attached_database() {
        let c = mem().await;
        let other = BASE.replace("schemaName: public", "schemaName: shop");
        let e = migrate(yaml(&other), &c, false, "S1__a.yaml", &opt()).await.unwrap_err();
        assert!(e.contains("schema shop: no database of that name"), "{}", e);
        exec(&c, "ATTACH DATABASE ':memory:' AS shop").await;
        migrate(yaml(&other), &c, false, "S1__a.yaml", &opt()).await.unwrap();
        assert!(info(&c).await["shop"].contains_key("orders"));
        assert!(info(&c).await["main"].is_empty());
    }

    #[tokio::test]
    async fn what_sqlite_has_not_is_refused_or_left_out() {
        let c = mem().await;
        let trig = BASE.replace("                defaultValue: CURRENT_TIMESTAMP\n",
            "                defaultValue: CURRENT_TIMESTAMP\n          triggers:\n            - trigger:\n                name: t1\n                event: after insert\n                when: for each row\n                proc: app.audit()\n");
        let e = migrate(yaml(&trig), &c, false, "S1__a.yaml", &opt()).await.unwrap_err();
        assert!(e.contains("its triggers are a function's"), "{}", e);
        let skip = MigrationOptions { exclude_triggers: true, ..MigrationOptions::default() };
        assert!(migrate(yaml(&trig), &c, false, "S1__a.yaml", &skip).await.is_ok());
        let part = BASE.replace("                type: text\n", "                type: text\n                partition_by: RANGE\n");
        let c = mem().await;
        assert!(migrate(yaml(&part), &c, false, "S1__a.yaml", &opt()).await.unwrap_err().contains("a partition"));
        // grants: left out, said. [review 2026-11-19] A schema's own `owner` is not read by this crate
        // (Schema::new takes its name), so nothing is said of it - it said "its owner" of every schema
        let owned = BASE.replace("    schemaName: public\n", "    schemaName: public\n    owner: app\n")
            .replace("          data:\n", "          grant:\n            - all: app_rw\n          data:\n");
        let c = mem().await;
        let p = migrate(yaml(&owned), &c, false, "S1__a.yaml", &opt()).await.unwrap();
        assert!(p.skipped.iter().any(|s| s.contains("its grants")), "{:?}", p.skipped);
        assert!(!p.skipped.iter().any(|s| s.contains("its owner")), "{:?}", p.skipped);
    }

    /// [review 2026-11-18] a rebuild keeps what the schema does not say: a column it does not
    /// name whole (UNIQUE, CHECK, COLLATE, GENERATED), a named one's UNIQUE and CHECK, the table's
    /// options, its constraints, AUTOINCREMENT's counter, and a view on it
    #[tokio::test]
    async fn a_rebuild_keeps_what_the_schema_does_not_say() {
        let c = mem().await;
        exec(&c, "CREATE TABLE customers (id integer PRIMARY KEY AUTOINCREMENT, email text NOT NULL UNIQUE,
                           code text UNIQUE COLLATE NOCASE CHECK (length(code) < 9),
                           upper_code text GENERATED ALWAYS AS (upper(code)) STORED,
                           CHECK (id > 0)) STRICT;
                         INSERT INTO customers (email, code) VALUES ('a@x.com', 'ab'), ('b@x.com', 'cd');
                         DELETE FROM customers WHERE id = 2;
                         CREATE VIEW codes AS SELECT code FROM customers;").await;
        // the schema names id and email, and makes email nullable: a rebuild
        let y = BASE.replace("                constraint:\n                  nullable: false\n                index:\n                  name: customers_email_key\n                  unique: true\n", "")
            .replace("          data:\n            - ['1', 'a@x.com']\n            - ['2', 'it''s@x.com']\n", "")
            // a STRICT table takes SQLite's own types only
            .replace("type: varchar(80)", "type: text");
        let p = migrate(yaml(&y), &c, false, "S1__a.yaml", &opt()).await.unwrap();
        assert!(p.rebuilt.iter().any(|r| r.starts_with("customers")), "{:?}", p);
        // code: unique, case-insensitive, checked; upper_code computed; the table STRICT
        assert!(try_exec(&c, "INSERT INTO customers (email, code) VALUES ('c@x.com', 'AB')").await.is_err(), "UNIQUE COLLATE NOCASE kept");
        assert!(try_exec(&c, "INSERT INTO customers (email, code) VALUES ('d@x.com', '123456789')").await.is_err(), "CHECK kept");
        let up: String = str_of(&c, "SELECT upper_code FROM customers WHERE code = 'ab'").await;
        assert_eq!(up, "AB");
        let sql: String = str_of(&c, "SELECT sql FROM sqlite_schema WHERE name = 'customers'").await;
        assert!(sql.trim_end().ends_with("STRICT"), "{}", sql);
        assert!(sql.contains("CHECK (id > 0)"), "{}", sql);
        // the named email: UNIQUE kept, NOT NULL gone as the schema says
        assert!(try_exec(&c, "INSERT INTO customers (email, code) VALUES ('a@x.com', 'zz')").await.is_err(), "email UNIQUE kept");
        exec(&c, "INSERT INTO customers (email, code) VALUES (NULL, 'yy')").await;
        // AUTOINCREMENT's counter: 2 was handed out and deleted, never again
        let ids: Vec<i64> = i64s(&c, "SELECT id FROM customers ORDER BY id").await;
        assert_eq!(ids, vec![1, 3]);
        // the view still reads it
        let n: i64 = i64_of(&c, "SELECT count(*) FROM codes").await;
        assert_eq!(n, 2);
    }

    #[tokio::test]
    async fn a_column_of_its_own_sql_is_added_by_a_rebuild() {
        let c = mem().await;
        migrate(yaml(BASE), &c, false, "S1__a.yaml", &opt()).await.unwrap();
        let more = BASE.replace("                defaultValue: CURRENT_TIMESTAMP\n",
            "                defaultValue: CURRENT_TIMESTAMP\n            - column:\n                name: ref\n                type: text\n                sql: UNIQUE\n");
        let p = migrate(yaml(&more), &c, false, "S2__a.yaml", &opt()).await.unwrap();
        assert_eq!(p.rebuilt.len(), 1, "{:?}", p);
        exec(&c, "INSERT INTO orders (id, customer, ref) VALUES (1, 1, 'r')").await;
        assert!(try_exec(&c, "INSERT INTO orders (id, customer, ref) VALUES (2, 1, 'r')").await.is_err());
    }

    #[tokio::test]
    async fn a_composite_key_and_spaced_types_change_nothing_on_a_second_run() {
        let two = "
database:
  - schemaName: main
    tables:
      - table:
          tableName: lines
          columns:
            - column:
                name: order_id
                type: integer
                constraint:
                  primaryKey: true
                  nullable: false
            - column:
                name: n
                type: integer
                constraint:
                  primaryKey: true
                  nullable: false
            - column:
                name: label
                type: varchar( 40 )
                defaultValue: \"'Hi'\"
";
        let c = mem().await;
        migrate(yaml(two), &c, false, "S1__a.yaml", &opt()).await.unwrap();
        let pk = &info(&c).await["main"]["lines"].primary_key;
        assert_eq!(pk.as_ref().unwrap().columns, vec!["order_id".to_string(), "n".to_string()]);
        let p = migrate(yaml(two), &c, false, "S1__a.yaml", &opt()).await.unwrap();
        assert!(p.statements.is_empty(), "{:?}", p.statements);
        // a default's case inside its quotes is a change
        let p = migrate(yaml(&two.replace("'Hi'", "'hi'")), &c, true, "S2__a.yaml", &opt()).await.unwrap();
        assert_eq!(p.rebuilt.len(), 1, "{:?}", p);
    }

    #[tokio::test]
    async fn a_rebuild_in_the_way_or_an_open_transaction_is_said() {
        let c = mem().await;
        migrate(yaml(BASE), &c, false, "S1__a.yaml", &opt()).await.unwrap();
        let more = BASE.replace("                type: text\n                defaultValue: CURRENT_TIMESTAMP\n",
                                "                type: text\n                defaultValue: CURRENT_TIMESTAMP\n                constraint:\n                  nullable: false\n");
        exec(&c, "CREATE TABLE _sg_new_orders (x)").await;
        assert!(migrate(yaml(&more), &c, false, "S2__a.yaml", &opt()).await.unwrap_err().contains("is in the way"));
        exec(&c, "DROP TABLE _sg_new_orders; BEGIN").await;
        assert!(migrate(yaml(&more), &c, false, "S2__a.yaml", &opt()).await.unwrap_err().contains("is in a transaction"));
        exec(&c, "ROLLBACK").await;
        // a failed rebuild leaves foreign keys as they were
        exec(&c, "INSERT INTO orders (id, customer) VALUES (10, 1)").await;
        let fk_before: i64 = i64_of(&c, "PRAGMA foreign_keys").await;
        let bad = BASE.replace("                defaultValue: CURRENT_TIMESTAMP\n",
            "                defaultValue: CURRENT_TIMESTAMP\n            - column:\n                name: must\n                type: text\n                constraint:\n                  nullable: false\n");
        assert!(migrate(yaml(&bad), &c, false, "S3__a.yaml", &opt()).await.is_err());
        let fk: i64 = i64_of(&c, "PRAGMA foreign_keys").await;
        assert_eq!(fk, fk_before);
    }

    /// [mapping] a MySQL schema through mysql-sqlite.yaml, then deployed here (2026-11-19)
    #[tokio::test]
    async fn a_mysql_schema_mapped_to_sqlite_is_deployed() {
        let my = "
source: mysql.8.0
database:
  - schemaName: public
    tables:
      - table:
          tableName: users
          columns:
            - column:
                name: id
                type: bigint auto_increment
                constraint:
                  primaryKey: true
            - column:
                name: active
                type: tinyint(1)
                defaultValue: \"1\"
            - column:
                name: kind
                type: enum('a','b')
            - column:
                name: born
                type: datetime(6)
                defaultValue: CURRENT_TIMESTAMP(6)
          triggers:
            - trigger:
                name: t1
                event: after insert
                when: for each row
                proc: f()
";
        let y = YamlLoader::load_from_str(my).unwrap().remove(0);
        let m = crate::mapping::Mapping::builtin("mysql", "sqlite").unwrap().unwrap();
        let out = m.apply(&y, &crate::mapping::Source::parse("sqlite.3").unwrap()).unwrap();
        assert!(out.notes.iter().any(|n| n.contains("table.triggers")), "{:?}", out.notes);
        let c = mem().await;
        migrate(out.schema, &c, false, "S1__a.yaml", &opt()).await.unwrap();
        let t = &info(&c).await["main"]["users"];
        assert_eq!(t.columns["id"].column_type, "integer");
        assert_eq!(t.columns["kind"].column_type, "text");
        assert_eq!(t.columns["born"].column_default.as_deref(), Some("CURRENT_TIMESTAMP"));
        exec(&c, "INSERT INTO users (kind) VALUES ('a')").await;
        let (id, active) = (i64_of(&c, "SELECT id FROM users").await, i64_of(&c, "SELECT active FROM users").await);
        assert_eq!((id, active), (1, 1), "the key counts by itself, the boolean defaults to 1");
    }

    #[tokio::test]
    async fn a_postgres_schema_mapped_to_sqlite_is_deployed() {
        let pg = "
source: postgres.16
database:
  - schema:
    schemaName: public
    owner: app_owner
    tables:
      - table:
          tableName: items
          columns:
            - column:
                name: id
                type: bigserial
                constraint:
                  primaryKey: true
            - column:
                name: title
                type: varchar(120)
                sql: UNIQUE
            - column:
                name: added
                type: timestamptz
                defaultValue: now()
          grant:
            - all: app_rw
";
        let y = YamlLoader::load_from_str(pg).unwrap().remove(0);
        let m = crate::mapping::Mapping::builtin("postgres", "sqlite").unwrap().unwrap();
        let out = m.apply(&y, &crate::mapping::Source::parse("sqlite.3").unwrap()).unwrap();
        let c = mem().await;
        migrate(out.schema, &c, false, "S1__a.yaml", &opt()).await.unwrap();
        let i = info(&c).await;
        let t = &i["main"]["items"];
        assert_eq!(t.columns["id"].column_type, "integer");
        assert!(t.columns["id"].pk);
        assert!(t.indexes["items_title_key"].is_unique);
        assert_eq!(t.columns["added"].column_default.as_deref(), Some("CURRENT_TIMESTAMP"));
        exec(&c, "INSERT INTO items (title) VALUES ('a')").await;
        let id: i64 = i64_of(&c, "SELECT id FROM items").await;
        assert_eq!(id, 1, "an integer primary key counts by itself, as the bigserial did");
        // [review 2026-11-18] and a second run of it changes nothing
        let again = m.apply(&YamlLoader::load_from_str(pg).unwrap().remove(0), &crate::mapping::Source::parse("sqlite.3").unwrap()).unwrap();
        let p = migrate(again.schema, &c, false, "S1__a.yaml", &opt()).await.unwrap();
        assert!(p.statements.is_empty(), "{:?}", p.statements);
    }
}
