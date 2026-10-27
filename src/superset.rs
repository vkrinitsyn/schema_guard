//! Apache Superset's import bundle for the tables of a schema: the v1 export format
//! (`version: 1.0.0`, Superset 2.1 and later) - one database, a dataset for each table
//! with every column and its SQL type, and the `count` metric Superset gives a new
//! dataset. Made from a YAML schema (`parse_yaml_schema`) or from a live database
//! (`loader::load_info_schema`), and handed back as the bundle's files - a path and its
//! YAML each - or as the ZIP Superset imports (Datasets, Import).
//!
//! Everything in the bundle is named by a UUID made from its names, so a bundle exported
//! again imports over the same datasets instead of beside them. Superset also matches a
//! dataset by its connection, schema and table: an import takes over one made by hand
//! for the same table, the first time too - its descriptions, labels and time column
//! reset, and with an overwrite its own metrics and calculated columns deleted. Leave
//! such tables out: `datasets_of_*`, `retain`, `bundle`.
//! A connection Superset has already is named by its own UUID
//! (`SupersetDatabase::uuid`): the import then keeps it as it is, and asks no password.

use std::collections::HashSet;
use std::convert::TryFrom;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::loader::{InfoSchemaType, PgColumnDfn, PgTable};
use crate::schema::Schema;

/// The bundle format's version, as Superset's own export writes it
const BUNDLE_VERSION: &str = "1.0.0";

/// How Superset's export writes a password in an SQLAlchemy URI: its import asks for it
pub const MASKED_PASSWORD: &str = "XXXXXXXXXX";

/// The Superset database the datasets belong to
#[derive(Clone)]
#[non_exhaustive]
pub struct SupersetDatabase {
    /// the name Superset shows the connection under; the bundle's file of it is named so
    pub database_name: String,
    /// `postgresql://user:password@host:5432/db`: a password in it is written masked, as
    /// Superset's own export writes one - its import asks for it, which only its web form
    /// can: `superset import-datasources` has no way to give one. The connection's
    /// password only: another secret among the query parameters - `sslpassword` - is
    /// written as it is, as Superset's own export writes it (it asks for no other)
    pub sqlalchemy_uri: String,
    /// the UUID of a connection Superset has already - set by `existing`, which checks it
    /// and writes it as Superset keys it; set as given, one in capitals kept the
    /// connection and imported none of its datasets (review of 2026-10-26)
    uuid: Option<String>,
}

impl SupersetDatabase {
    /// A connection named `database_name` at `sqlalchemy_uri`, made or taken over by name
    pub fn new(database_name: &str, sqlalchemy_uri: &str) -> Self {
        SupersetDatabase { database_name: database_name.to_string(), sqlalchemy_uri: sqlalchemy_uri.to_string(), uuid: None }
    }

    /// ...or a connection Superset has already, by its UUID - lower case, as Superset keys
    /// its connections: one in capitals kept the connection, and imported no dataset of it,
    /// reporting success (review of 2026-10-26). An error for a text that is no UUID
    pub fn existing(database_name: &str, sqlalchemy_uri: &str, uuid: &str) -> Result<Self, String> {
        let u = uuid.trim().to_ascii_lowercase();
        let groups: Vec<&str> = u.split('-').collect();
        let shaped = groups.iter().map(|g| g.len()).eq([8, 4, 4, 4, 12])
            && groups.iter().all(|g| g.chars().all(|c| c.is_ascii_hexdigit()));
        if !shaped {
            return Err(tr!("superset.uuid", uuid = uuid.to_string()));
        }
        Ok(SupersetDatabase { uuid: Some(u), ..SupersetDatabase::new(database_name, sqlalchemy_uri) })
    }

    /// The connection's UUID: the one `existing` was given - imported, the connection is
    /// kept as it is, its URI and settings, and no password is asked for - or one made
    /// from its name, and Superset makes the connection - or, holding one of that name
    /// already, takes it over
    pub fn uuid(&self) -> String {
        self.uuid.clone().unwrap_or_else(|| name_uuid(&["database", &self.database_name]))
    }
}

/// A password printed with `{:?}` would be out in the open: masked as in the bundle
impl std::fmt::Debug for SupersetDatabase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SupersetDatabase")
            .field("database_name", &self.database_name)
            .field("sqlalchemy_uri", &mask_password(&self.sqlalchemy_uri))
            .field("uuid", &self.uuid)
            .finish()
    }
}

/// One table, as Superset gets it
#[derive(Debug, Clone, PartialEq)]
pub struct Dataset {
    pub schema: String,
    pub table: String,
    pub description: String,
    pub columns: Vec<DatasetColumn>,
}

/// One column of a dataset
#[derive(Debug, Clone, PartialEq)]
pub struct DatasetColumn {
    pub name: String,
    /// the SQL type as Postgres names it to SQLAlchemy: `INTEGER`, `VARCHAR(64)`, ...
    pub sql_type: String,
    pub description: String,
}

/// The tables of parsed schemas - `parse_yaml_schema`'s list, or schemas built from a
/// database, as sge's snapshot builds them - in their order; a template table is none
pub fn datasets_of_schemas(schemas: &[Schema]) -> Vec<Dataset> {
    let mut out = Vec::new();
    for s in schemas {
        for t in &s.tables.list {
            if t.is_template == Some(true) {
                continue;
            }
            out.push(Dataset {
                schema: s.schema_name.clone(),
                table: t.table_name.clone(),
                description: t.description.clone(),
                columns: t.columns.list.iter().map(|c| DatasetColumn {
                    name: c.name.clone(),
                    sql_type: sql_type(&c.column_type),
                    description: c.description.clone(),
                }).collect(),
            });
        }
    }
    out
}

/// The tables of a database as `loader::load_info_schema` read them: by schema, each
/// schema's tables and each table's columns in their order there
pub fn datasets_of_info(info: &InfoSchemaType) -> Vec<Dataset> {
    let mut out = Vec::new();
    for (schema, tables) in info {
        let mut ts: Vec<&PgTable> = tables.values().collect();
        ts.sort_by(|a, b| (a.sort_order, &a.table_name).cmp(&(b.sort_order, &b.table_name)));
        for t in ts {
            let mut cols: Vec<&PgColumnDfn> = t.columns.values().collect();
            cols.sort_by(|a, b| (a.sort_order, &a.column_name).cmp(&(b.sort_order, &b.column_name)));
            out.push(Dataset {
                schema: schema.clone(),
                table: t.table_name.clone(),
                description: t.table_comment.clone().unwrap_or_default(),
                columns: cols.iter().map(|c| DatasetColumn {
                    name: c.column_name.clone(),
                    sql_type: sql_type(&c.column_type),
                    description: c.column_comment.clone().unwrap_or_default(),
                }).collect(),
            });
        }
    }
    out
}

/// The bundle: `metadata.yaml`, the database's file, and one file for each dataset, each
/// a path and its YAML. `at` is the metadata's timestamp - `SystemTime::now()`, or a
/// fixed one for a bundle that comes out the same every time
pub fn bundle(datasets: &[Dataset], db: &SupersetDatabase, at: SystemTime) -> Vec<(String, String)> {
    let db_uuid = db.uuid();
    let db_file = file_name(&db.database_name)
        .unwrap_or_else(|| format!("db_{}", file_name(db_uuid.get(..8).unwrap_or(&db_uuid)).unwrap_or_default()));
    let mut files = vec![
        ("metadata.yaml".to_string(), yaml(&Y::Map(vec![
            ("version", s(BUNDLE_VERSION)),
            ("type", s("SqlaTable")),
            ("timestamp", s(&utc_text(at))),
        ]))),
        (format!("databases/{}.yaml", db_file), yaml(&Y::Map(vec![
            ("database_name", s(&db.database_name)),
            ("sqlalchemy_uri", s(&mask_password(&db.sqlalchemy_uri))),
            ("cache_timeout", Y::Null),
            ("expose_in_sqllab", Y::Bool(true)),
            ("allow_run_async", Y::Bool(false)),
            ("allow_ctas", Y::Bool(false)),
            ("allow_cvas", Y::Bool(false)),
            ("allow_dml", Y::Bool(false)),
            ("allow_file_upload", Y::Bool(false)),
            ("extra", Y::Map(vec![("allows_virtual_table_explore", Y::Bool(true))])),
            ("uuid", s(&db_uuid)),
            ("version", s(BUNDLE_VERSION)),
        ]))),
    ];
    let mut used: HashSet<String> = HashSet::new();
    for d in datasets {
        let uuid = name_uuid(&["dataset", &db_uuid, &d.schema, &d.table]);
        // two names made the same by file_name ("a b", "a_b"; "a.b" "c", "a" "b.c") get a number
        let base = format!("datasets/{}/{}", db_file,
            file_name(&format!("{}.{}", d.schema, d.table)).unwrap_or_else(|| format!("ds_{}", &uuid[..8])));
        let mut path = format!("{}.yaml", base);
        let mut n = 2;
        while used.contains(&path) {
            path = format!("{}_{}.yaml", base, n);
            n += 1;
        }
        used.insert(path.clone());
        files.push((path, dataset_yaml(d, &uuid, &db_uuid)));
    }
    files
}

/// One dataset's file
fn dataset_yaml(d: &Dataset, uuid: &str, db_uuid: &str) -> String {
    let columns: Vec<Y> = d.columns.iter().map(|c| {
        let t = c.sql_type.as_str();
        Y::Map(vec![
            ("column_name", s(&c.name)),
            ("verbose_name", Y::Null),
            ("is_dttm", Y::Bool(is_dttm(t))),
            ("is_active", Y::Bool(true)),
            ("type", s(t)),
            ("groupby", Y::Bool(!matches!(t, "JSON" | "JSONB" | "BYTEA" | "XML"))),
            ("filterable", Y::Bool(t != "BYTEA")),
            ("expression", Y::Null),
            ("description", text_or_null(&c.description)),
            ("python_date_format", Y::Null),
            ("extra", Y::Null),
        ])
    }).collect();
    let main_dttm = d.columns.iter().find(|c| is_dttm(&c.sql_type)).map_or(Y::Null, |c| s(&c.name));
    yaml(&Y::Map(vec![
        ("table_name", s(&d.table)),
        ("main_dttm_col", main_dttm),
        ("description", text_or_null(&d.description)),
        ("default_endpoint", Y::Null),
        ("offset", Y::Int(0)),
        ("cache_timeout", Y::Null),
        ("schema", s(&d.schema)),
        ("sql", Y::Null),
        ("params", Y::Null),
        ("template_params", Y::Null),
        ("filter_select_enabled", Y::Bool(true)),
        ("fetch_values_predicate", Y::Null),
        ("extra", Y::Null),
        ("uuid", s(uuid)),
        ("metrics", Y::List(vec![Y::Map(vec![
            ("metric_name", s("count")),
            ("verbose_name", s("COUNT(*)")),
            ("metric_type", s("count")),
            ("expression", s("COUNT(*)")),
            ("description", Y::Null),
            ("d3format", Y::Null),
            ("extra", Y::Null),
            ("warning_text", Y::Null),
        ])])),
        ("columns", Y::List(columns)),
        ("version", s(BUNDLE_VERSION)),
        ("database_uuid", s(db_uuid)),
    ]))
}

/// A time column Superset can put on an axis: a date or a timestamp - not an array of
/// them, a `daterange`, nor a type named like one (`date_kind`)
fn is_dttm(sql_type: &str) -> bool {
    sql_type == "DATE" || sql_type.strip_prefix("TIMESTAMP")
        .is_some_and(|rest| (rest.is_empty() || rest.starts_with('(') || rest.starts_with(' ')) && !rest.ends_with("[]"))
}

/// A column's SQL type as Postgres names it to SQLAlchemy - the `type` Superset keeps, and
/// reads a time column off: from a YAML type (`serial`, `int`, `varchar(64)`,
/// `timestamptz`, `int[]`) or the catalog's (`int4`, `_int4`, `NUMERIC(10, 2)`). A type it
/// does not know - an enum, a domain - is written upper case, as given
pub fn sql_type(t: &str) -> String {
    let t = t.trim();
    if let Some(e) = t.strip_suffix("[]") {
        return format!("{}[]", sql_type(e));
    }
    if let Some(e) = t.strip_prefix('_') {
        return format!("{}[]", sql_type(e));
    }
    let lower = t.to_lowercase();
    // `(64)`, `(10,2)`: taken out, the name is what is left - `timestamp(3) with time zone`
    let (base, args) = match (lower.find('('), lower.find(')')) {
        (Some(i), Some(j)) if i < j => {
            let rest = format!("{} {}", &lower[..i], &lower[j + 1..]);
            let args: Vec<String> = lower[i + 1..j].split(',').map(|a| a.trim().to_string()).collect();
            (rest.split_whitespace().collect::<Vec<_>>().join(" "), Some(args))
        }
        _ => (lower.split_whitespace().collect::<Vec<_>>().join(" "), None),
    };
    // `float(p)`: a real up to 24 bits of precision, a double past them; the bits go
    if base == "float" {
        let p = args.as_ref().and_then(|a| a.first()).and_then(|p| p.parse::<u32>().ok()).unwrap_or(53);
        return if p <= 24 { "REAL" } else { "DOUBLE PRECISION" }.to_string();
    }
    let (name, zone) = match base.as_str() {
        "serial" | "serial4" | "int" | "int4" | "integer" => ("INTEGER", ""),
        "bigserial" | "serial8" | "bigint" | "int8" => ("BIGINT", ""),
        "smallserial" | "serial2" | "smallint" | "int2" => ("SMALLINT", ""),
        "varchar" | "character varying" => ("VARCHAR", ""),
        "char" | "character" | "bpchar" => ("CHAR", ""),
        "text" => ("TEXT", ""),
        "bool" | "boolean" => ("BOOLEAN", ""),
        "timestamp" | "timestamp without time zone" => ("TIMESTAMP", " WITHOUT TIME ZONE"),
        "timestamptz" | "timestamp with time zone" => ("TIMESTAMP", " WITH TIME ZONE"),
        "date" => ("DATE", ""),
        "time" | "time without time zone" => ("TIME", " WITHOUT TIME ZONE"),
        "timetz" | "time with time zone" => ("TIME", " WITH TIME ZONE"),
        "numeric" | "decimal" => ("NUMERIC", ""),
        "real" | "float4" => ("REAL", ""),
        "double precision" | "float8" => ("DOUBLE PRECISION", ""),
        "json" => ("JSON", ""),
        "jsonb" => ("JSONB", ""),
        "uuid" => ("UUID", ""),
        "bytea" => ("BYTEA", ""),
        "interval" => ("INTERVAL", ""),
        "inet" => ("INET", ""),
        "cidr" => ("CIDR", ""),
        "macaddr" => ("MACADDR", ""),
        "money" => ("MONEY", ""),
        "xml" => ("XML", ""),
        _ => return t.to_uppercase(),
    };
    format!("{}{}{}", name, args.map(|a| format!("({})", a.join(", "))).unwrap_or_default(), zone)
}

/// The URI with its password, if it has one, as Superset's export writes it - read as
/// SQLAlchemy reads a URI (`make_url`): the user up to `:` or `/`, the password from there
/// up to the first `@` (a `/`, `?` or `#` in it too), the query from the first `?` after
/// them. Superset asks for a password masked there only: one among the query parameters
/// - `password=`, its name percent-encoded or not - is moved there, masked; left in the
/// query masked, the connection was made with the mask for its password (review of
/// 2026-10-26). An empty password stays as it is, as Superset's export leaves it
pub fn mask_password(uri: &str) -> String {
    let Some(start) = uri.find("://").map(|i| i + 3) else { return uri.to_string() };
    let rest = &uri[start..];
    // user info, only when an `@` ends it: the user is what comes before the first `:` or
    // `/` - with a password after that `:` up to the next `@` - or, without one, up to the
    // last `@` before that `:` or `/` (the user may hold an `@`, as the regex's does)
    let seg_end = rest.find(|c| c == ':' || c == '/').unwrap_or(rest.len());
    let seg = &rest[..seg_end];
    let with_password = rest[seg_end..].strip_prefix(':').and_then(|r| r.find('@').map(|at| (r, at)));
    let (user, password, after) = match (with_password, seg.rfind('@')) {
        (Some((r, at)), _) => (Some(seg), Some(&r[..at]), start + seg_end + 1 + at + 1),
        (None, Some(at)) => (Some(&seg[..at]), None, start + at + 1),
        (None, None) => (None, None, start),
    };
    // the query from the first `?` after the user info. A malformed host - an IPv6 one
    // with junk after it - SQLAlchemy may stop reading inside; masked all the same: a
    // password asked for needlessly costs less than one written in clear
    let (host_db, query) = match uri[after..].find('?') {
        Some(q) => (&uri[after..after + q], Some(&uri[after + q + 1..])),
        None => (&uri[after..], None),
    };
    let mut in_query = false;
    // a blank one - `password=`, a bare `password` - SQLAlchemy drops: no password, none asked
    let kept: Vec<&str> = query.map(|q| q.split('&').filter(|p| {
        let is_password = p.split_once('=').is_some_and(|(k, v)| !v.is_empty() && percent_decoded(k) == "password");
        in_query |= is_password;
        !is_password
    }).collect()).unwrap_or_default();
    let masked = in_query || password.is_some_and(|p| !p.is_empty());
    let mut out = uri[..start].to_string();
    match (user, masked) {
        (u, true) => out.push_str(&format!("{}:{}@", u.unwrap_or(""), MASKED_PASSWORD)),
        (Some(u), false) => match password {
            Some(p) => out.push_str(&format!("{}:{}@", u, p)),
            None => out.push_str(&format!("{}@", u)),
        },
        (None, false) => {}
    }
    out.push_str(host_db);
    if let Some(q) = query {
        if !in_query {
            out.push('?');
            out.push_str(q);
        } else if !kept.is_empty() {
            out.push('?');
            out.push_str(&kept.join("&"));
        }
    }
    out
}

/// `%XX` in a URI query's parameter name decoded; anything else as it is
fn percent_decoded(t: &str) -> String {
    let b = t.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        match (b[i], b.get(i + 1..i + 3).and_then(|h| std::str::from_utf8(h).ok()).and_then(|h| u8::from_str_radix(h, 16).ok())) {
            (b'%', Some(v)) => {
                out.push(v);
                i += 3;
            }
            (c, _) => {
                out.push(c);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// A UUID made from names - the same names, the same UUID: FNV-1a 128 of them, each
/// ended by a NUL, which no name holds (so `a.b` `c` and `a` `b.c` are two), as a
/// version 8 UUID (RFC 9562), the variant bits set
pub fn name_uuid(names: &[&str]) -> String {
    let mut h: u128 = 0x6c62272e07bb014262b821756295c58d;
    for b in "schema_guard/superset".bytes().chain(names.iter().flat_map(|n| n.bytes().chain(std::iter::once(0)))) {
        h ^= b as u128;
        h = h.wrapping_mul(0x0000000001000000000000000000013B);
    }
    let h = (h & !(0xF_u128 << 76)) | (0x8_u128 << 76);
    let h = (h & !(0x3_u128 << 62)) | (0x2_u128 << 62);
    let x = format!("{:032x}", h);
    format!("{}-{}-{}-{}-{}", &x[0..8], &x[8..12], &x[12..16], &x[16..20], &x[20..32])
}

/// A name fit for a file in the bundle, as werkzeug's `secure_filename` makes one -
/// letters, digits, `.`, `_` and `-` kept, the rest `_`, and no `.` or `_` to begin or
/// end it: Superset's import passes over a file whose name begins with either, so a
/// connection named in Cyrillic, or a schema `_staging`, imported nothing - and said it
/// had (review of 2026-10-26). None when nothing is left of it
fn file_name(name: &str) -> Option<String> {
    let f: String = name.chars().map(|c| if c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-') { c } else { '_' }).collect();
    let f = f.trim_matches(|c| c == '.' || c == '_');
    if f.is_empty() { None } else { Some(f.to_string()) }
}

/// `at` as Superset's export writes its timestamp: `2026-10-26T20:15:00.123456+00:00`
fn utc_text(at: SystemTime) -> String {
    let d = at.duration_since(UNIX_EPOCH).unwrap_or_default();
    let secs = d.as_secs();
    let (y, m, day) = civil_from_days((secs / 86_400) as i64);
    let t = secs % 86_400;
    format!("{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:06}+00:00", y, m, day, t / 3600, t / 60 % 60, t % 60, d.subsec_micros())
}

/// The calendar date of a day count since 1970-01-01 (H. Hinnant's civil_from_days)
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    let y = yoe as i64 + era * 400 + if m <= 2 { 1 } else { 0 };
    (y, m, d)
}

// ── the files' YAML ──────────────────────────────────────────────────────────

/// A value of the bundle's YAML. Every string is written double-quoted: a plain one is
/// read by type in YAML 1.1 - PyYAML, which Superset loads with, makes `on` a boolean,
/// `1_000` a number and a date-like text a date
enum Y {
    Null,
    Bool(bool),
    Int(i64),
    Str(String),
    Map(Vec<(&'static str, Y)>),
    List(Vec<Y>),
}

fn s(v: &str) -> Y {
    Y::Str(v.to_string())
}

fn text_or_null(v: &str) -> Y {
    if v.trim().is_empty() { Y::Null } else { s(v) }
}

/// A map as a YAML document, each list item `- ` under its key, as PyYAML writes one
fn yaml(v: &Y) -> String {
    let mut out = String::new();
    if let Y::Map(entries) = v {
        write_map(&mut out, entries, 0);
    }
    out
}

fn write_map(out: &mut String, entries: &[(&'static str, Y)], indent: usize) {
    let mut first = true;
    for (k, v) in entries {
        if !first || indent == 0 {
            out.push_str(&" ".repeat(indent));
        }
        first = false;
        out.push_str(k);
        out.push(':');
        match v {
            Y::Map(m) if m.is_empty() => out.push_str(" {}\n"),
            Y::Map(m) => {
                out.push('\n');
                out.push_str(&" ".repeat(indent + 2));
                write_map(out, m, indent + 2);
            }
            Y::List(l) if l.is_empty() => out.push_str(" []\n"),
            Y::List(l) => {
                out.push('\n');
                for item in l {
                    out.push_str(&" ".repeat(indent));
                    out.push_str("- ");
                    match item {
                        Y::Map(m) if !m.is_empty() => write_map(out, m, indent + 2),
                        other => {
                            out.push_str(&scalar(other));
                            out.push('\n');
                        }
                    }
                }
            }
            other => {
                out.push(' ');
                out.push_str(&scalar(other));
                out.push('\n');
            }
        }
    }
}

fn scalar(v: &Y) -> String {
    match v {
        Y::Null => "null".to_string(),
        Y::Bool(b) => b.to_string(),
        Y::Int(i) => i.to_string(),
        Y::Str(t) => quoted(t),
        Y::Map(_) => "{}".to_string(),
        Y::List(_) => "[]".to_string(),
    }
}

/// A YAML double-quoted string: `\` and `"` escaped, and every character YAML does not
/// let stand - the C0 and C1 controls, DEL, U+FFFE and U+FFFF: one left in, PyYAML
/// refused the file, and Superset's import failed whole (review of 2026-10-26)
fn quoted(t: &str) -> String {
    let mut q = String::with_capacity(t.len() + 2);
    q.push('"');
    for c in t.chars() {
        match c {
            '"' => q.push_str("\\\""),
            '\\' => q.push_str("\\\\"),
            '\n' => q.push_str("\\n"),
            '\r' => q.push_str("\\r"),
            '\t' => q.push_str("\\t"),
            '\u{85}' => q.push_str("\\N"),
            '\u{2028}' => q.push_str("\\L"),
            '\u{2029}' => q.push_str("\\P"),
            c if (c as u32) < 0x20 || ('\u{7f}'..='\u{9f}').contains(&c) => q.push_str(&format!("\\x{:02X}", c as u32)),
            '\u{fffe}' | '\u{ffff}' => q.push_str(&format!("\\u{:04X}", c as u32)),
            c => q.push(c),
        }
    }
    q.push('"');
    q
}

// ── the ZIP ──────────────────────────────────────────────────────────────────

/// The bundle as the ZIP Superset imports: every file under one folder, `root` - Superset
/// drops the first part of each path - stored, not compressed, with its CRC-32. An error
/// for a bundle past what a ZIP holds without ZIP64: 65,535 files, 4 GiB
pub fn zip(files: &[(String, String)], root: &str) -> Result<Vec<u8>, String> {
    if files.len() > u16::MAX as usize {
        return Err(tr!("superset.zip_limit", what = format!("{} files", files.len())));
    }
    let (time, date) = dos_time(files.iter().find(|(p, _)| p == "metadata.yaml")
        .and_then(|(_, y)| y.lines().find_map(|l| l.strip_prefix("timestamp: \"")).map(|t| t.to_string()))
        .as_deref().unwrap_or(""));
    let root = file_name(root).unwrap_or_else(|| "superset_export".to_string());
    let mut out: Vec<u8> = Vec::new();
    let mut central: Vec<u8> = Vec::new();
    for (path, text) in files {
        let name = format!("{}/{}", root, path);
        let data = text.as_bytes();
        let (size, offset) = match (u32::try_from(data.len()), u32::try_from(out.len())) {
            (Ok(s), Ok(o)) if name.len() <= u16::MAX as usize => (s, o),
            _ => return Err(tr!("superset.zip_limit", what = path.clone())),
        };
        let crc = crc32(data);
        // local file header
        out.extend_from_slice(&0x0403_4b50u32.to_le_bytes());
        out.extend_from_slice(&20u16.to_le_bytes()); // version needed: 2.0
        out.extend_from_slice(&0x0800u16.to_le_bytes()); // the name is UTF-8
        out.extend_from_slice(&0u16.to_le_bytes()); // stored
        out.extend_from_slice(&time.to_le_bytes());
        out.extend_from_slice(&date.to_le_bytes());
        out.extend_from_slice(&crc.to_le_bytes());
        out.extend_from_slice(&size.to_le_bytes());
        out.extend_from_slice(&size.to_le_bytes());
        out.extend_from_slice(&(name.len() as u16).to_le_bytes());
        out.extend_from_slice(&0u16.to_le_bytes());
        out.extend_from_slice(name.as_bytes());
        out.extend_from_slice(data);
        // its central directory entry
        central.extend_from_slice(&0x0201_4b50u32.to_le_bytes());
        central.extend_from_slice(&(0x0300u16 | 20).to_le_bytes()); // made by: unix, 2.0
        central.extend_from_slice(&20u16.to_le_bytes());
        central.extend_from_slice(&0x0800u16.to_le_bytes());
        central.extend_from_slice(&0u16.to_le_bytes());
        central.extend_from_slice(&time.to_le_bytes());
        central.extend_from_slice(&date.to_le_bytes());
        central.extend_from_slice(&crc.to_le_bytes());
        central.extend_from_slice(&size.to_le_bytes());
        central.extend_from_slice(&size.to_le_bytes());
        central.extend_from_slice(&(name.len() as u16).to_le_bytes());
        central.extend_from_slice(&0u16.to_le_bytes()); // extra
        central.extend_from_slice(&0u16.to_le_bytes()); // comment
        central.extend_from_slice(&0u16.to_le_bytes()); // disk
        central.extend_from_slice(&0u16.to_le_bytes()); // internal attributes
        central.extend_from_slice(&(0o100644u32 << 16).to_le_bytes()); // a regular file, rw-r--r--
        central.extend_from_slice(&offset.to_le_bytes());
        central.extend_from_slice(name.as_bytes());
    }
    let (cd_size, cd_offset) = match (u32::try_from(central.len()), u32::try_from(out.len())) {
        (Ok(s), Ok(o)) => (s, o),
        _ => return Err(tr!("superset.zip_limit", what = "the central directory".to_string())),
    };
    out.extend_from_slice(&central);
    // end of central directory
    out.extend_from_slice(&0x0605_4b50u32.to_le_bytes());
    out.extend_from_slice(&0u16.to_le_bytes());
    out.extend_from_slice(&0u16.to_le_bytes());
    out.extend_from_slice(&(files.len() as u16).to_le_bytes());
    out.extend_from_slice(&(files.len() as u16).to_le_bytes());
    out.extend_from_slice(&cd_size.to_le_bytes());
    out.extend_from_slice(&cd_offset.to_le_bytes());
    out.extend_from_slice(&0u16.to_le_bytes());
    Ok(out)
}

/// A file's time and date in the ZIP's DOS form, from `YYYY-MM-DDTHH:MM:SS...`; 1980-01-01,
/// the first the form holds, for anything else
fn dos_time(ts: &str) -> (u16, u16) {
    let n = |a: usize, b: usize| ts.get(a..b).and_then(|v| v.parse::<u16>().ok());
    match (n(0, 4), n(5, 7), n(8, 10), n(11, 13), n(14, 16), n(17, 19)) {
        (Some(y), Some(mo), Some(d), Some(h), Some(mi), Some(s)) if y >= 1980 && y < 2108 =>
            ((h << 11) | (mi << 5) | (s / 2), ((y - 1980) << 9) | (mo << 5) | d),
        _ => (0, (1 << 5) | 1),
    }
}

/// CRC-32, the ZIP's (IEEE 802.3, reflected)
fn crc32(data: &[u8]) -> u32 {
    let mut crc = 0xFFFF_FFFFu32;
    for b in data {
        crc ^= *b as u32;
        for _ in 0..8 {
            crc = if crc & 1 != 0 { (crc >> 1) ^ 0xEDB8_8320 } else { crc >> 1 };
        }
    }
    !crc
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use yaml_rust2::{Yaml, YamlLoader};

    fn load(text: &str) -> Yaml {
        YamlLoader::load_from_str(text).expect("valid YAML").remove(0)
    }

    fn db() -> SupersetDatabase {
        SupersetDatabase::new("app db", "postgresql://app:s3cr@t@db.local:5432/app")
    }

    /// 2026-10-26T20:15:00Z
    fn at() -> SystemTime {
        UNIX_EPOCH + Duration::from_secs(1_793_045_700)
    }

    /// the tables of `d`, as a bundle's datasets' files
    fn ds(schema: &str, table: &str, columns: Vec<DatasetColumn>) -> Dataset {
        Dataset { schema: schema.into(), table: table.into(), description: String::new(), columns }
    }

    /// the YAML schema's tables, templates left out, every column with the SQL type
    /// Superset keeps; the files a database and a dataset each, named apart
    #[test]
    fn a_yaml_schema_is_a_bundle_of_its_tables() {
        let schemas = crate::parse_yaml_schema(crate::load_schema_from_file("tests/example.yaml").unwrap(), "").unwrap();
        let d = datasets_of_schemas(&schemas.list);
        assert_eq!(d.len(), 1);
        assert_eq!((d[0].schema.as_str(), d[0].table.as_str(), d[0].description.as_str()),
            ("test_schema", "test_table", "The new table"));
        assert_eq!(d[0].columns.iter().map(|c| (c.name.as_str(), c.sql_type.as_str())).collect::<Vec<_>>(),
            vec![("id", "INTEGER"), ("test", "VARCHAR(250)")]);
        let files = bundle(&d, &db(), at());
        assert_eq!(files.iter().map(|(p, _)| p.as_str()).collect::<Vec<_>>(),
            vec!["metadata.yaml", "databases/app_db.yaml", "datasets/app_db/test_schema.test_table.yaml"]);

        let meta = load(&files[0].1);
        assert_eq!(meta["version"].as_str(), Some("1.0.0"), "a string, as Superset's schema wants it");
        assert_eq!(meta["type"].as_str(), Some("SqlaTable"));
        assert_eq!(meta["timestamp"].as_str(), Some("2026-10-26T20:15:00.000000+00:00"));

        let d = load(&files[1].1);
        assert_eq!(d["database_name"].as_str(), Some("app db"));
        assert_eq!(d["sqlalchemy_uri"].as_str(), Some("postgresql://app:XXXXXXXXXX@t@db.local:5432/app"),
            "masked up to the first @, as SQLAlchemy reads it");
        assert_eq!(d["expose_in_sqllab"].as_bool(), Some(true));
        assert!(d["cache_timeout"].is_null());
        assert_eq!(d["extra"]["allows_virtual_table_explore"].as_bool(), Some(true));
        let db_uuid = d["uuid"].as_str().unwrap().to_string();

        let t = load(&files[2].1);
        assert_eq!(t["table_name"].as_str(), Some("test_table"));
        assert_eq!(t["schema"].as_str(), Some("test_schema"));
        assert_eq!(t["database_uuid"].as_str(), Some(db_uuid.as_str()), "the dataset names its database");
        assert_eq!(t["offset"].as_i64(), Some(0));
        assert!(t["sql"].is_null() && t["main_dttm_col"].is_null());
        assert_eq!(t["metrics"][0]["expression"].as_str(), Some("COUNT(*)"));
        let cols = t["columns"].as_vec().unwrap();
        assert_eq!(cols.len(), 2);
        assert_eq!(cols[1]["column_name"].as_str(), Some("test"));
        assert_eq!(cols[1]["type"].as_str(), Some("VARCHAR(250)"));
        assert_eq!(cols[1]["groupby"].as_bool(), Some(true));
        assert!(cols[0]["description"].is_null(), "an empty description is null");
    }

    /// a table exported again keeps its UUIDs: imported, it replaces its dataset; a
    /// connection Superset has already is named by its own UUID, and a dataset of it
    /// by that one
    #[test]
    fn a_bundle_exported_again_names_the_same_things() {
        let d = vec![ds("public", "orders", vec![])];
        let a = bundle(&d, &db(), UNIX_EPOCH + Duration::from_secs(1_767_225_600));
        let b = bundle(&d, &db(), UNIX_EPOCH + Duration::from_secs(1_769_904_000));
        assert_eq!(a[1], b[1], "the database's file");
        assert_eq!(a[2], b[2], "the dataset's file");
        assert_ne!(name_uuid(&["x", "public", "orders"]), name_uuid(&["x", "public", "order"]));
        assert_ne!(name_uuid(&["x", "a.b", "c"]), name_uuid(&["x", "a", "b.c"]), "a dot in a name is no separator");
        let u = name_uuid(&["schema_guard"]);
        assert_eq!(u.len(), 36);
        assert_eq!(&u[14..15], "8", "version 8: {}", u);
        assert!(matches!(&u[19..20], "8" | "9" | "a" | "b"), "the RFC variant: {}", u);
        let known = SupersetDatabase::existing("app db", "postgresql://app@db/app", " 6B9C2D1E-0000-4000-8000-00000000ABCD ")
            .expect("a UUID, in capitals: as Superset keys it");
        assert!(SupersetDatabase::existing("x", "postgresql://h/db", "6b9c2d1e").unwrap_err().starts_with("SG-E073 "));
        assert!(SupersetDatabase::existing("x", "postgresql://h/db", "6b9c2d1e-0000-4000-8000-00000000abcz").is_err());
        let c = bundle(&d, &known, at());
        assert_eq!(load(&c[1].1)["uuid"].as_str(), Some("6b9c2d1e-0000-4000-8000-00000000abcd"));
        assert_eq!(load(&c[2].1)["database_uuid"].as_str(), Some("6b9c2d1e-0000-4000-8000-00000000abcd"));
        assert_ne!(load(&c[2].1)["uuid"], load(&a[2].1)["uuid"], "a dataset of another connection is another");
    }

    /// Superset's import passes over a file whose name begins with `.` or `_`: no name
    /// in the bundle does - a Cyrillic connection's, a schema `_staging`'s (review of
    /// 2026-10-26)
    #[test]
    fn no_file_is_one_the_import_passes_over() {
        let ru = SupersetDatabase::new("Аналитика", "postgresql://a@h/db");
        let files = bundle(&[ds("_staging", "orders", vec![]), ds("Отчёты", "Итоги", vec![]), ds("a.b", "c", vec![]),
            ds("a", "b.c", vec![])], &ru, at());
        let names: Vec<&str> = files.iter().map(|(p, _)| p.as_str()).collect();
        for p in &names {
            let base = p.rsplit('/').next().unwrap();
            assert!(!base.starts_with('.') && !base.starts_with('_'), "{}", p);
            for dir in p.split('/') {
                assert!(!dir.starts_with('.') && !dir.starts_with('_'), "{}", p);
            }
        }
        assert!(names[1].starts_with("databases/db_"), "a name with nothing left: the UUID's: {}", names[1]);
        assert!(names[2].ends_with("/staging.orders.yaml"), "{}", names[2]);
        assert!(names[3].contains("/ds_"), "{}", names[3]);
        assert_eq!(names[4].rsplit('/').next(), Some("a.b.c.yaml"));
        assert_eq!(names[5].rsplit('/').next(), Some("a.b.c_2.yaml"), "two made one: numbered");
    }

    /// a time column is one Superset can chart by, the first of them the main one
    #[test]
    fn a_timestamp_is_the_main_time_column() {
        let c = |n: &str, t: &str| DatasetColumn { name: n.into(), sql_type: sql_type(t), description: String::new() };
        let d = vec![Dataset { schema: "s".into(), table: "t".into(), description: "x".into(),
            columns: vec![c("id", "bigserial"), c("span", "daterange"), c("kind", "date_kind"), c("tags", "timestamptz[]"),
                c("at", "timestamptz"), c("day", "date"), c("doc", "jsonb"), c("p", "timestamp(3)")] }];
        let t = load(&bundle(&d, &db(), at())[2].1);
        assert_eq!(t["main_dttm_col"].as_str(), Some("at"), "not a range, a type named like a date, nor an array");
        let cols = t["columns"].as_vec().unwrap();
        assert_eq!(cols.iter().map(|c| c["is_dttm"].as_bool().unwrap()).collect::<Vec<_>>(),
            vec![false, false, false, false, true, true, false, true]);
        assert_eq!(cols[6]["groupby"].as_bool(), Some(false), "jsonb");
    }

    #[test]
    fn a_type_is_named_as_postgres_names_it_to_sqlalchemy() {
        for (t, want) in [("serial", "INTEGER"), ("int4", "INTEGER"), ("bigserial", "BIGINT"), ("int8", "BIGINT"),
            ("smallint", "SMALLINT"), ("varchar(64)", "VARCHAR(64)"), ("character varying(10)", "VARCHAR(10)"),
            ("varchar", "VARCHAR"), ("text", "TEXT"), ("bool", "BOOLEAN"), ("numeric(10,2)", "NUMERIC(10, 2)"),
            ("NUMERIC(10, 2)", "NUMERIC(10, 2)"), ("timestamp", "TIMESTAMP WITHOUT TIME ZONE"),
            ("timestamptz", "TIMESTAMP WITH TIME ZONE"), ("timestamp(3) with time zone", "TIMESTAMP(3) WITH TIME ZONE"),
            ("date", "DATE"), ("float8", "DOUBLE PRECISION"), ("float", "DOUBLE PRECISION"), ("float(10)", "REAL"),
            ("float(25)", "DOUBLE PRECISION"), ("jsonb", "JSONB"), ("uuid", "UUID"),
            ("int[]", "INTEGER[]"), ("_int4", "INTEGER[]"), ("varchar(5)[]", "VARCHAR(5)[]"), ("mood", "MOOD")] {
            assert_eq!(sql_type(t), want, "{}", t);
        }
    }

    /// as SQLAlchemy reads a URI: the password up to the first `@`, a `/` `?` `#` in it
    /// too; and a `password=` parameter (review of 2026-10-26)
    #[test]
    fn a_password_is_masked_and_nothing_else() {
        let m = |u: &str| mask_password(u);
        assert_eq!(m("postgresql://u:p@h:5432/db"), "postgresql://u:XXXXXXXXXX@h:5432/db");
        assert_eq!(m("postgresql://app:Xy/9+ab==@db/app"), "postgresql://app:XXXXXXXXXX@db/app", "base64 with a /");
        assert_eq!(m("postgresql://app:pa?ss@db/app"), "postgresql://app:XXXXXXXXXX@db/app");
        assert_eq!(m("postgresql://app:p#w@db/app"), "postgresql://app:XXXXXXXXXX@db/app");
        assert_eq!(m("postgresql://u:p@ss@h/db?sslmode=require"), "postgresql://u:XXXXXXXXXX@ss@h/db?sslmode=require");
        assert_eq!(m("postgresql://u@h/db?sslmode=require&password=x1&a=b"),
            "postgresql://u:XXXXXXXXXX@h/db?sslmode=require&a=b", "a parameter: moved where Superset asks for it");
        assert_eq!(m("postgresql://h/db?pass%77ord=x1"), "postgresql://:XXXXXXXXXX@h/db", "its name encoded; no user");
        assert_eq!(m("postgresql://u:p@h/db?password=x1"), "postgresql://u:XXXXXXXXXX@h/db", "both: one, masked");
        assert_eq!(m("postgresql://u?x:p@h/db"), "postgresql://u?x:XXXXXXXXXX@h/db", "a ? in the user");
        assert_eq!(m("postgresql://u:@h/db"), "postgresql://u:@h/db", "an empty password, as Superset leaves it");
        assert_eq!(m("postgresql://u@h/db?password=&sslmode=require"), "postgresql://u@h/db?password=&sslmode=require",
            "a blank parameter: no password, as SQLAlchemy reads it");
        assert_eq!(m("postgresql://u@h/db?password"), "postgresql://u@h/db?password", "a bare one too");
        assert_eq!(m("postgresql://u@h/db?PASSWORD=x"), "postgresql://u@h/db?PASSWORD=x", "no keyword libpq knows");
        assert_eq!(m("postgresql://u@[::1]x/db?password=x"), "postgresql://u:XXXXXXXXXX@[::1]x/db",
            "a malformed host: masked all the same");
        assert_eq!(m("postgresql://u@[::1]:5432/db?password=x"), "postgresql://u:XXXXXXXXXX@[::1]:5432/db");
        assert_eq!(m("postgresql://u@h:5432/db"), "postgresql://u@h:5432/db", "a user, no password");
        assert_eq!(m("postgresql://a@b:pw@h/db"), "postgresql://a@b:XXXXXXXXXX@h/db", "an @ in the user");
        assert_eq!(m("postgresql://u@h/db"), "postgresql://u@h/db", "no password");
        assert_eq!(m("postgresql://h:5432/db"), "postgresql://h:5432/db", "a port is no password");
        assert_eq!(m("not a uri"), "not a uri");
        assert!(!format!("{:?}", SupersetDatabase::new("x", "postgresql://u:hunter2@h/db")).contains("hunter2"),
            "nor in a Debug print");
    }

    /// every string double-quoted: read back as the same text, never as a boolean, a
    /// number or a date; a character YAML does not let stand, escaped
    #[test]
    fn a_string_reads_back_as_itself() {
        for t in ["on", "1_000", "2026-10-26", "null", "a \"quoted\" \\ back", "two\nlines\ttab", "ünï", "\u{1}x",
            "- dash", "# hash", "c1 \u{80}\u{9f} del \u{7f} nel \u{85} \u{fffe}\u{ffff}"] {
            let doc = yaml(&Y::Map(vec![("v", s(t))]));
            assert!(!doc.contains('\u{80}') && !doc.contains('\u{ffff}'), "escaped: {:?}", doc);
            assert_eq!(load(&doc)["v"].as_str(), Some(t), "{}", doc);
        }
    }

    /// the ZIP: one folder above the files, each stored with its CRC-32, the central
    /// directory where the end record says; past 65,535 files, an error, not a broken one
    #[test]
    fn the_zip_holds_every_file_under_one_folder() {
        let files = vec![("metadata.yaml".to_string(), "timestamp: \"2026-10-26T20:15:42.000000+00:00\"\n".to_string()),
            ("datasets/d/t.yaml".to_string(), "x: 1\n".to_string())];
        let z = zip(&files, "dataset_export").unwrap();
        let u32_at = |i: usize| u32::from_le_bytes([z[i], z[i + 1], z[i + 2], z[i + 3]]);
        let u16_at = |i: usize| u16::from_le_bytes([z[i], z[i + 1]]);
        let end = z.len() - 22;
        assert_eq!(u32_at(end), 0x0605_4b50);
        assert_eq!(u16_at(end + 10), 2, "two entries");
        let (cd_size, cd_at) = (u32_at(end + 12) as usize, u32_at(end + 16) as usize);
        assert_eq!(cd_at + cd_size, end);
        let mut at = cd_at;
        for (path, text) in &files {
            assert_eq!(u32_at(at), 0x0201_4b50);
            assert_eq!(u32_at(at + 16), crc32(text.as_bytes()));
            let n = u16_at(at + 28) as usize;
            assert_eq!(&z[at + 46..at + 46 + n], format!("dataset_export/{}", path).as_bytes());
            let local = u32_at(at + 42) as usize;
            assert_eq!(u32_at(local), 0x0403_4b50);
            let data_at = local + 30 + u16_at(local + 26) as usize;
            assert_eq!(&z[data_at..data_at + text.len()], text.as_bytes());
            at += 46 + n;
        }
        assert_eq!(crc32(b"123456789"), 0xCBF4_3926, "the check value");
        assert_eq!(dos_time("2026-10-26T20:15:42"), ((20 << 11) | (15 << 5) | 21, (46 << 9) | (10 << 5) | 26));
        let many: Vec<(String, String)> = (0..65_536).map(|i| (format!("datasets/d/t{}.yaml", i), String::new())).collect();
        assert!(zip(&many, "x").unwrap_err().starts_with("SG-E072 "), "past a ZIP's 65,535 files");
    }

    #[test]
    fn a_timestamp_is_written_as_superset_writes_it() {
        assert_eq!(utc_text(UNIX_EPOCH), "1970-01-01T00:00:00.000000+00:00");
        assert_eq!(utc_text(at() + Duration::from_micros(42)), "2026-10-26T20:15:00.000042+00:00");
        assert_eq!(utc_text(UNIX_EPOCH + Duration::from_secs(951_782_400)), "2000-02-29T00:00:00.000000+00:00", "a leap day");
    }
}
