//! [mysql] MySQL (feature `mysql`, owner 2026-11-19): its catalog (information_schema) read into
//! the schema's model, and a YAML schema deployed to it through the driver - sqlx's MySQL one,
//! pure Rust, on tokio; no client library.
//!
//! A schema's name is a MySQL database: made when it is not there; `public` (PostgreSQL's usual
//! one, a schema written by hand) is the connection's current database (`USE` / the URL's path).
//! The rest as on PostgreSQL: what the schema has that the database has not is made, nothing is
//! dropped, a column the database has that the schema has not is kept. MySQL's ALTER TABLE does
//! what is needed in place: ADD COLUMN, MODIFY COLUMN (a type, nullability, a default), ADD
//! PRIMARY KEY, indexes, foreign keys. A narrower or incompatible type needs `with_size_cut`, a
//! changed index or key `with_index_drop`, as on PostgreSQL.
//!
//! **DDL is not transactional in MySQL:** each statement commits by itself. So everything is
//! planned - every refusal found - before the first statement runs; a statement that then fails
//! stops the run, and the error says how many ran before it.
//!
//! Not in MySQL: owners (left out), PostgreSQL's function triggers (refused unless
//! `exclude_triggers`), partitions (refused), grants (left out: MySQL's are its accounts').
//! Custom SQL of the schema is MySQL's own here and goes in as written.

use std::collections::{BTreeMap, HashMap};

use sqlx::{MySqlConnection, Row};
use yaml_rust2::Yaml;

use crate::column::Column;
use crate::loader::{FKTable, InfoSchemaType, PgColumnDfn, PgIndex, PgIndexColumn, PgPrimaryKey, PgTable, PgTrigger};
use crate::schema::Schema;
use crate::table::{analyze_type_change, Table, TypeChange};
use crate::MigrationOptions;

/// a column as MySQL has it (information_schema.COLUMNS)
#[derive(Debug, Clone, Default)]
pub struct MyColumn {
    pub name: String,
    /// COLUMN_TYPE, lower case: `int`, `varchar(80)`, `tinyint(1)`, `bigint unsigned`
    pub column_type: String,
    pub nullable: bool,
    /// COLUMN_DEFAULT as MySQL keeps it: a literal unquoted, an expression as its text
    pub default: Option<String>,
    /// EXTRA: `auto_increment`, `DEFAULT_GENERATED` (an expression default) ...
    pub extra: String,
    pub position: usize,
}

#[derive(Debug, Clone, Default)]
pub struct MyIndex {
    pub name: String,
    pub unique: bool,
    pub columns: Vec<String>,
}

#[derive(Debug, Clone, Default)]
pub struct MyFk {
    pub name: String,
    pub column: String,
    pub ref_schema: String,
    pub ref_table: String,
    pub ref_column: String,
}

/// a table as MySQL has it
#[derive(Debug, Clone, Default)]
pub struct MyTable {
    pub name: String,
    pub columns: Vec<MyColumn>,
    pub primary_key: Vec<String>,
    pub indexes: Vec<MyIndex>,
    pub fks: Vec<MyFk>,
    pub triggers: Vec<String>,
}

/// What a deploy does: its statements, and what was left out and why
#[derive(Debug, Default, Clone)]
pub struct MysqlPlan {
    pub statements: Vec<String>,
    pub skipped: Vec<String>,
}

fn q(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
}

fn lit(v: &str) -> String {
    format!("'{}'", v.replace('\\', "\\\\").replace('\'', "''"))
}

fn err(e: sqlx::Error, what: &str) -> String {
    format!("mysql: {}: {}", what, e)
}

/// [mysql] a statement's rows, each value as text (NULL as None): the catalog's columns cast to
/// CHAR in the query, as MySQL 8 sends some of them as binary
async fn rows(conn: &mut MySqlConnection, sql: &str, what: &str) -> Result<Vec<Vec<Option<String>>>, String> {
    let rs = sqlx::query(sql).fetch_all(&mut *conn).await.map_err(|e| err(e, what))?;
    Ok(rs.iter().map(|r| (0..r.len()).map(|i| r.try_get::<Option<String>, _>(i).ok().flatten()).collect()).collect())
}

/// the database of a schema's name: `public` the connection's current one
async fn database_of(conn: &mut MySqlConnection, schema: &str) -> Result<String, String> {
    if !schema.is_empty() && !schema.eq_ignore_ascii_case("public") {
        return Ok(schema.to_string());
    }
    let cur = rows(conn, "SELECT CAST(DATABASE() AS CHAR)", "database()").await?;
    cur.first().and_then(|r| r.first().cloned().flatten()).ok_or_else(|| format!(
        "mysql: schema {}: no current database - name one in the connection (its URL's path) or give the schema its database's name",
        if schema.is_empty() { "public" } else { schema }))
}

/// every table of a database, as MySQL has it
pub async fn read_tables(conn: &mut MySqlConnection, db: &str) -> Result<BTreeMap<String, MyTable>, String> {
    let d = lit(db);
    let mut out: BTreeMap<String, MyTable> = BTreeMap::new();
    for r in rows(conn, &format!("SELECT CAST(TABLE_NAME AS CHAR) FROM information_schema.TABLES \
                                  WHERE TABLE_SCHEMA = {} AND TABLE_TYPE = 'BASE TABLE'", d), "tables").await? {
        let name = r[0].clone().unwrap_or_default();
        out.insert(name.clone(), MyTable { name, ..Default::default() });
    }
    for r in rows(conn, &format!("SELECT CAST(TABLE_NAME AS CHAR), CAST(COLUMN_NAME AS CHAR), CAST(COLUMN_TYPE AS CHAR), \
                                  CAST(IS_NULLABLE AS CHAR), CAST(COLUMN_DEFAULT AS CHAR), CAST(EXTRA AS CHAR), \
                                  CAST(ORDINAL_POSITION AS CHAR) FROM information_schema.COLUMNS \
                                  WHERE TABLE_SCHEMA = {} ORDER BY TABLE_NAME, ORDINAL_POSITION", d), "columns").await? {
        if let Some(t) = out.get_mut(r[0].as_deref().unwrap_or("")) {
            t.columns.push(MyColumn {
                name: r[1].clone().unwrap_or_default(), column_type: r[2].clone().unwrap_or_default().to_lowercase(),
                nullable: r[3].as_deref() == Some("YES"), default: r[4].clone(), extra: r[5].clone().unwrap_or_default(),
                position: r[6].as_deref().and_then(|p| p.parse().ok()).unwrap_or(0),
            });
        }
    }
    for r in rows(conn, &format!("SELECT CAST(TABLE_NAME AS CHAR), CAST(INDEX_NAME AS CHAR), CAST(NON_UNIQUE AS CHAR), \
                                  CAST(COLUMN_NAME AS CHAR) FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = {} \
                                  ORDER BY TABLE_NAME, INDEX_NAME, SEQ_IN_INDEX", d), "indexes").await? {
        let Some(t) = out.get_mut(r[0].as_deref().unwrap_or("")) else { continue };
        let (name, col) = (r[1].clone().unwrap_or_default(), r[3].clone().unwrap_or_default());
        if name == "PRIMARY" {
            t.primary_key.push(col);
            continue;
        }
        match t.indexes.iter_mut().find(|x| x.name == name) {
            Some(x) => x.columns.push(col),
            None => t.indexes.push(MyIndex { name, unique: r[2].as_deref() == Some("0"), columns: vec![col] }),
        }
    }
    for r in rows(conn, &format!("SELECT CAST(TABLE_NAME AS CHAR), CAST(CONSTRAINT_NAME AS CHAR), CAST(COLUMN_NAME AS CHAR), \
                                  CAST(REFERENCED_TABLE_SCHEMA AS CHAR), CAST(REFERENCED_TABLE_NAME AS CHAR), \
                                  CAST(REFERENCED_COLUMN_NAME AS CHAR) FROM information_schema.KEY_COLUMN_USAGE \
                                  WHERE TABLE_SCHEMA = {} AND REFERENCED_TABLE_NAME IS NOT NULL", d), "foreign keys").await? {
        if let Some(t) = out.get_mut(r[0].as_deref().unwrap_or("")) {
            let g = |i: usize| r[i].clone().unwrap_or_default();
            t.fks.push(MyFk { name: g(1), column: g(2), ref_schema: g(3), ref_table: g(4), ref_column: g(5) });
        }
    }
    for r in rows(conn, &format!("SELECT CAST(EVENT_OBJECT_TABLE AS CHAR), CAST(TRIGGER_NAME AS CHAR) \
                                  FROM information_schema.TRIGGERS WHERE TRIGGER_SCHEMA = {}", d), "triggers").await? {
        if let Some(t) = out.get_mut(r[0].as_deref().unwrap_or("")) {
            t.triggers.push(r[1].clone().unwrap_or_default());
        }
    }
    Ok(out)
}

/// The catalog read into the schema's model, a schema a database: those given, or every one
/// but MySQL's own when none is
pub async fn load_info_schema(conn: &mut MySqlConnection, databases: &[&str]) -> Result<InfoSchemaType, String> {
    let dbs: Vec<String> = if databases.is_empty() {
        rows(conn, "SELECT CAST(SCHEMA_NAME AS CHAR) FROM information_schema.SCHEMATA WHERE SCHEMA_NAME NOT IN \
                    ('mysql', 'information_schema', 'performance_schema', 'sys')", "schemata").await?
            .into_iter().filter_map(|r| r.into_iter().next().flatten()).collect()
    } else {
        databases.iter().map(|d| d.to_string()).collect()
    };
    let mut out = InfoSchemaType::new();
    for db in dbs {
        let mut tables = HashMap::new();
        for (i, (name, t)) in read_tables(conn, &db).await?.into_iter().enumerate() {
            let auto = |c: &MyColumn| c.extra.to_lowercase().contains("auto_increment");
            let columns = t.columns.iter().map(|c| (c.name.clone(), PgColumnDfn {
                column_name: c.name.clone(),
                column_type: if auto(c) { format!("{} auto_increment", c.column_type) } else { c.column_type.clone() },
                column_default: c.default.as_ref().map(|d| default_as_written(d, &c.extra)),
                sql: None, fk: t.fks.iter().find(|f| f.column == c.name).map(|f| (f.ref_table.clone(), f.ref_column.clone())),
                pk: t.primary_key.contains(&c.name), nullable: c.nullable, sort_order: c.position, column_comment: None,
            })).collect();
            let fks = t.fks.iter().map(|f| (f.column.clone(), FKTable {
                schema: f.ref_schema.clone(), table: f.ref_table.clone(), column: [f.column.clone()].into_iter().collect(),
                name: f.name.clone(), sql: String::new() })).collect();
            let indexes = t.indexes.iter().map(|x| (x.name.clone(), PgIndex {
                index_name: x.name.clone(), is_unique: x.unique, index_method: "btree".into(),
                columns: x.columns.iter().map(|c| PgIndexColumn {
                    column_name: c.clone(), order: String::new(), nulls: String::new(), collation: String::new() }).collect(),
            })).collect();
            let triggers = t.triggers.iter().map(|n| (n.clone(), PgTrigger {
                trigger_name: n.clone(), event: String::new(), orientation: String::new(), proc: String::new() })).collect();
            let primary_key = (!t.primary_key.is_empty()).then(|| PgPrimaryKey {
                constraint_name: "PRIMARY".into(), columns: t.primary_key.clone() });
            tables.insert(name.clone(), PgTable {
                table_name: name, columns, fks, triggers, indexes, grants: HashMap::new(), primary_key,
                sort_order: i + 1, table_comment: None, owner: None });
        }
        out.insert(db, tables);
    }
    Ok(out)
}

/// a default as a schema writes it: an expression MySQL keeps (DEFAULT_GENERATED, or
/// CURRENT_TIMESTAMP) in parentheses unless it is CURRENT_TIMESTAMP, a literal quoted unless a number
fn default_as_written(d: &str, extra: &str) -> String {
    let t = d.trim();
    let up = t.to_uppercase();
    if up.starts_with("CURRENT_TIMESTAMP") || up == "NULL" {
        return t.to_string();
    }
    if extra.to_uppercase().contains("DEFAULT_GENERATED") {
        return format!("({})", t);
    }
    if t.parse::<f64>().is_ok() { t.to_string() } else { lit(t) }
}

/// [mysql] a type compared: lower case, no spaces by a parenthesis, MySQL's own names for the
/// synonyms, an integer's display width dropped (MySQL 8 shows it only for tinyint(1))
pub(crate) fn norm_type(t: &str) -> String {
    let s = t.split_whitespace().collect::<Vec<_>>().join(" ").to_lowercase()
        .replace("( ", "(").replace(" )", ")").replace(", ", ",").replace(" ,", ",").replace(" (", "(");
    let s = s.replace(" auto_increment", "");
    let re = regex::Regex::new(r"^(tinyint|smallint|mediumint|int|integer|bigint)\((\d+)\)(.*)$").unwrap();
    let s = match re.captures(&s) {
        Some(c) if !(&c[1] == "tinyint" && &c[2] == "1") => format!("{}{}", &c[1], &c[3]),
        _ => s,
    };
    let s = s.replace("integer", "int");
    match s.as_str() {
        "bool" | "boolean" => "tinyint(1)".into(),
        "decimal" | "numeric" | "dec" => "decimal(10,0)".into(),
        "double precision" | "real" => "double".into(),
        _ => s.replacen("numeric(", "decimal(", 1).replacen("character varying(", "varchar(", 1),
    }
}

/// [mysql] a default compared: an expression's parentheses off, MySQL's names for the date
/// functions, a literal's quotes off (MySQL keeps it bare)
pub(crate) fn norm_default(d: &str) -> String {
    let mut t = d.trim().to_string();
    while t.starts_with('(') && t.ends_with(')') {
        t = t[1..t.len() - 1].trim().to_string();
    }
    if t.len() >= 2 && t.starts_with('\'') && t.ends_with('\'') {
        return t[1..t.len() - 1].replace("''", "'");
    }
    let low = t.to_lowercase();
    match low.as_str() {
        "current_date" | "current_date()" | "curdate()" => "curdate()".into(),
        "current_time" | "current_time()" | "curtime()" => "curtime()".into(),
        "now()" | "current_timestamp" | "current_timestamp()" => "current_timestamp".into(),
        _ => low.replace("now(", "current_timestamp("),
    }
}

fn is_pk(c: &Column) -> bool {
    c.constraint.as_ref().and_then(|x| x.primary_key).unwrap_or(false)
}

fn nullable(c: &Column) -> bool {
    !is_pk(c) && c.constraint.as_ref().map(|x| x.nullable).unwrap_or(true)
}

fn auto_increment(c: &Column) -> bool {
    c.column_type.to_lowercase().contains("auto_increment")
}

/// a schema's column as MySQL's column definition
fn column_def(c: &Column) -> String {
    let mut s = format!("{} {}", q(&c.name), c.column_type.trim());
    if !nullable(c) {
        s.push_str(" NOT NULL");
    }
    if let Some(d) = c.default_value.as_ref().filter(|d| !d.trim().is_empty()) {
        s.push_str(&format!(" DEFAULT {}", d.trim()));
    }
    if !c.sql.trim().is_empty() {
        s.push(' ');
        s.push_str(c.sql.trim());
    }
    s
}

/// the table a foreign key names, and its database: `schema.table` or `table`
fn fk_of(c: &Column, db: &str) -> Option<(String, String)> {
    let r = c.constraint.as_ref()?.foreign_key.as_ref()?.references.trim().to_string();
    if r.is_empty() {
        return None;
    }
    Some(match r.split_once('.') {
        Some((s, t)) if !s.eq_ignore_ascii_case("public") => (s.to_string(), t.to_string()),
        Some((_, t)) => (db.to_string(), t.to_string()),
        None => (db.to_string(), r),
    })
}

fn wanted_indexes(t: &Table) -> Vec<(String, bool, String)> {
    t.columns.list.iter().filter_map(|c| c.index.as_ref().map(|i| {
        let name = if i.name.is_empty() { format!("{}_{}_idx", t.table_name, c.name) } else { i.name.clone() };
        (name, i.unique.unwrap_or(false), c.name.clone())
    })).collect()
}

fn create_index(db: &str, table: &str, name: &str, unique: bool, column: &str) -> String {
    format!("CREATE {}INDEX {} ON {}.{} ({})", if unique { "UNIQUE " } else { "" }, q(name), q(db), q(table), q(column))
}

/// What the deploy runs, from the schemas and the tables MySQL has (database -> table): no
/// connection, so the plan is the same for a test as for a server
pub fn plan_on(schemas: &[(String, &Schema)], existing: &HashMap<String, BTreeMap<String, MyTable>>, opt: &MigrationOptions)
    -> Result<MysqlPlan, String> {
    let mut p = MysqlPlan::default();
    let mut fk_statements = Vec::new();
    let empty = BTreeMap::new();
    for (db, s) in schemas {
        let have_db = existing.get(db);
        if have_db.is_none() {
            p.statements.push(format!("CREATE DATABASE IF NOT EXISTS {}", q(db)));
        }
        let have_db = have_db.unwrap_or(&empty);
        for t in &s.tables.list {
            if t.is_template == Some(true) {
                continue;
            }
            if t.columns.list.iter().any(|c| c.partition_by.is_some()) {
                return Err(format!("mysql: table {}: a partition - declare MySQL's on the table, apart", t.table_name));
            }
            if !t.triggers.list.is_empty() && !opt.exclude_triggers {
                return Err(format!("mysql: table {}: its triggers are a function's (PostgreSQL's form); MySQL's are SQL of \
                                   their own - leave them out, or set exclude_triggers", t.table_name));
            }
            if !t.grant.is_empty() {
                p.skipped.push(format!("table {}: its grants (MySQL's privileges are its accounts')", t.table_name));
            }
            let fk_cols: Vec<(&Column, (String, String))> = t.columns.list.iter().filter_map(|c| fk_of(c, db).map(|f| (c, f))).collect();
            match have_db.get(&t.table_name) {
                None => {
                    let pks: Vec<&Column> = t.columns.list.iter().filter(|c| is_pk(c)).collect();
                    let mut parts: Vec<String> = t.columns.list.iter().map(column_def).collect();
                    if !pks.is_empty() {
                        parts.push(format!("PRIMARY KEY ({})", pks.iter().map(|c| q(&c.name)).collect::<Vec<_>>().join(", ")));
                    }
                    if !t.constraint.trim().is_empty() && !t.constraint.trim().starts_with("--") {
                        parts.push(t.constraint.trim().to_string());
                    }
                    let mut st = format!("CREATE TABLE {}.{} (\n  {}\n)", q(db), q(&t.table_name), parts.join(",\n  "));
                    if !t.sql.trim().is_empty() && !t.sql.trim().starts_with("--") {
                        st.push(' ');
                        st.push_str(t.sql.trim());
                    }
                    p.statements.push(st);
                    for (name, unique, col) in wanted_indexes(t) {
                        p.statements.push(create_index(db, &t.table_name, &name, unique, &col));
                    }
                    if !t.data.is_empty() {
                        let cols: Vec<String> = t.columns.list.iter().map(|c| q(&c.name)).collect();
                        for row in &t.data {
                            let n = row.len().min(cols.len());
                            p.statements.push(format!("INSERT IGNORE INTO {}.{} ({}) VALUES ({})", q(db), q(&t.table_name),
                                cols[..n].join(", "), row[..n].iter().map(|v| lit(v)).collect::<Vec<_>>().join(", ")));
                        }
                    }
                    for (c, (rs, rt)) in fk_cols {
                        fk_statements.push(add_fk(db, &t.table_name, c, &rs, &rt, existing, schemas));
                    }
                }
                Some(have) => {
                    plan_table(db, t, have, opt, &mut p)?;
                    for (c, (rs, rt)) in fk_cols {
                        if !have.fks.iter().any(|f| f.column == c.name && f.ref_table == rt) {
                            fk_statements.push(add_fk(db, &t.table_name, c, &rs, &rt, existing, schemas));
                        }
                    }
                }
            }
        }
    }
    // foreign keys after every table of the run, as on PostgreSQL
    p.statements.extend(fk_statements);
    Ok(p)
}

/// a foreign key to the parent's primary key: of the schema when it has the table, else of MySQL's
fn add_fk(db: &str, table: &str, c: &Column, rs: &str, rt: &str, existing: &HashMap<String, BTreeMap<String, MyTable>>,
          schemas: &[(String, &Schema)]) -> String {
    let from_schema = schemas.iter().filter(|(d, _)| d == rs).flat_map(|(_, s)| s.tables.list.iter())
        .find(|t| t.table_name == rt).map(|t| t.columns.list.iter().filter(|c| is_pk(c)).map(|c| c.name.clone()).collect::<Vec<_>>());
    let pk = from_schema.filter(|v| !v.is_empty())
        .or_else(|| existing.get(rs).and_then(|d| d.get(rt)).map(|t| t.primary_key.clone()))
        .unwrap_or_else(|| vec!["id".into()]);
    let extra = c.constraint.as_ref().and_then(|x| x.foreign_key.as_ref()).map(|f| f.sql.trim().to_string()).unwrap_or_default();
    format!("ALTER TABLE {}.{} ADD CONSTRAINT {} FOREIGN KEY ({}) REFERENCES {}.{} ({}){}", q(db), q(table),
            q(&format!("fk_{}_{}_{}", table, rt, c.name)), q(&c.name), q(rs), q(rt),
            pk.iter().map(|k| q(k)).collect::<Vec<_>>().join(", "), if extra.is_empty() { String::new() } else { format!(" {}", extra) })
}

fn plan_table(db: &str, t: &Table, have: &MyTable, opt: &MigrationOptions, p: &mut MysqlPlan) -> Result<(), String> {
    let mut alters: Vec<String> = Vec::new();
    for c in &t.columns.list {
        match have.columns.iter().find(|h| h.name.eq_ignore_ascii_case(&c.name)) {
            None => alters.push(format!("ADD COLUMN {}", column_def(c))),
            Some(h) => {
                let (want, got) = (norm_type(&c.column_type), norm_type(&h.column_type));
                let mut modify = false;
                if want != got {
                    match analyze_type_change(&got, &want) {
                        TypeChange::NoChange => {}
                        TypeChange::SizeExtension | TypeChange::Compatible => modify = true,
                        TypeChange::SizeReduction | TypeChange::Incompatible => {
                            if opt.with_size_cut {
                                modify = true;
                            } else if opt.without_failfast {
                                p.skipped.push(format!("table {}: column {} type {} to {}: narrower or incompatible, needs with_size_cut",
                                                       t.table_name, c.name, got, want));
                            } else {
                                return Err(format!("mysql: table {}: column {} type {} to {} is narrower or incompatible: set with_size_cut",
                                                   t.table_name, c.name, got, want));
                            }
                        }
                    }
                }
                if nullable(c) != h.nullable && !is_pk(c) {
                    modify = true;
                }
                let want_d = c.default_value.as_ref().map(|d| d.trim()).filter(|d| !d.is_empty()).map(norm_default);
                let got_d = h.default.as_ref().map(|d| norm_default(d)).filter(|d| d.to_lowercase() != "null");
                if want_d != got_d {
                    modify = true;
                }
                if auto_increment(c) != h.extra.to_lowercase().contains("auto_increment") {
                    modify = true;
                }
                if modify {
                    alters.push(format!("MODIFY COLUMN {}", column_def(c)));
                }
            }
        }
    }
    let want_pk: Vec<String> = t.columns.list.iter().filter(|c| is_pk(c)).map(|c| c.name.clone()).collect();
    if !want_pk.is_empty() && want_pk != have.primary_key {
        if have.primary_key.is_empty() {
            alters.push(format!("ADD PRIMARY KEY ({})", want_pk.iter().map(|k| q(k)).collect::<Vec<_>>().join(", ")));
        } else if opt.with_index_drop {
            alters.push("DROP PRIMARY KEY".into());
            alters.push(format!("ADD PRIMARY KEY ({})", want_pk.iter().map(|k| q(k)).collect::<Vec<_>>().join(", ")));
        } else if opt.without_failfast {
            p.skipped.push(format!("table {}: primary key changed: needs with_index_drop", t.table_name));
        } else {
            return Err(format!("mysql: table {}: primary key {:?} to {:?}: set with_index_drop", t.table_name, have.primary_key, want_pk));
        }
    }
    if !alters.is_empty() {
        p.statements.push(format!("ALTER TABLE {}.{} {}", q(db), q(&t.table_name), alters.join(", ")));
    }
    for (name, unique, col) in wanted_indexes(t) {
        match have.indexes.iter().find(|x| x.name == name) {
            None => {
                // a column's own UNIQUE (`sql: UNIQUE` made into its index) may be there by another name
                if !have.indexes.iter().any(|x| x.unique == unique && x.columns == vec![col.clone()]) {
                    p.statements.push(create_index(db, &t.table_name, &name, unique, &col));
                }
            }
            Some(x) if x.unique != unique || x.columns != vec![col.clone()] => {
                if opt.with_index_drop {
                    p.statements.push(format!("DROP INDEX {} ON {}.{}", q(&name), q(db), q(&t.table_name)));
                    p.statements.push(create_index(db, &t.table_name, &name, unique, &col));
                } else if opt.without_failfast {
                    p.skipped.push(format!("table {}: index {} changed: needs with_index_drop", t.table_name, name));
                } else {
                    return Err(format!("mysql: table {}: index {} changed: set with_index_drop", t.table_name, name));
                }
            }
            Some(_) => {}
        }
    }
    Ok(())
}

/// The statements that make the database what the YAML schema says, without running them
pub async fn plan(schema: Yaml, conn: &mut MySqlConnection, file_name: &str, opt: &MigrationOptions) -> Result<MysqlPlan, String> {
    let schemas = crate::parse_yaml_schema(schema, file_name)?;
    let mut named = Vec::new();
    let mut existing = HashMap::new();
    for s in &schemas.list {
        let db = database_of(conn, &s.schema_name).await?;
        let there = !rows(conn, &format!("SELECT 1 FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = {}", lit(&db)), "schemata")
            .await?.is_empty();
        if there && !existing.contains_key(&db) {
            existing.insert(db.clone(), read_tables(conn, &db).await?);
        }
        named.push((db, s));
    }
    plan_on(&named, &existing, opt)
}

/// The YAML schema deployed: planned in full first, then run statement by statement - MySQL
/// commits each DDL by itself, so a failure stops the run where it is and says how far it got.
/// With `plan_only` nothing is run; the statements are returned either way
pub async fn migrate(schema: Yaml, conn: &mut MySqlConnection, plan_only: bool, file_name: &str, opt: &MigrationOptions)
    -> Result<MysqlPlan, String> {
    let p = plan(schema, conn, file_name, opt).await?;
    if plan_only {
        return Ok(p);
    }
    for (i, st) in p.statements.iter().enumerate() {
        sqlx::raw_sql(st).execute(&mut *conn).await.map_err(|e| format!(
            "mysql: {}\n  in: {}\n  {} of {} statement(s) ran before it, and stay (MySQL commits each DDL by itself)",
            e, st, i, p.statements.len()))?;
    }
    Ok(p)
}

#[cfg(test)]
#[path = "mysql_test.rs"]
mod tests;
