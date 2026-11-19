//! [mapping] A schema of one database vendor written as another's (owner, 2026-11-18): the
//! compatibility guard, as a declarative mapping from one vendor's YAML to another's, before any
//! statement runs.
//!
//! A schema says where it was taken from at its top level, `source: <vendor>.<version>` -
//! `postgres.16`, `mysql.8.0`, `sqlite` - none given being PostgreSQL's, as every schema written
//! before it was. A mapping is one file a pair of vendors (`from`, `to`): its `default` rules hold
//! for every version of both, and a version's rules (`versions`, `for: mysql.5`) come first when
//! that version is set: of each side's cases, the newest one at or below its version - a newer
//! server takes what an older one takes, by backward compatibility (`mysql.8` applies to
//! `mysql.8.4` and `mysql.9.1`, `mysql.5` to `mysql.5.7`; none to `mysql.4`). A version is the
//! vendor's own: a major (`postgres.16`) or more parts (`mysql.8.0`), compared part by part. So a
//! few files, each with a few version cases, stand in for every pair of every version.
//!
//! The rules, the first that matches deciding:
//! - `types`: a column's type matched (a regex, whole and case-insensitive) and written as the
//!   target's (`to`, with `$1`... of the match), kept (`keep: true`) or refused (`refuse: why`)
//! - `defaults`: a column's defaultValue the same way; a literal (a number, a quoted string,
//!   true, false, null) is kept with no rule
//! - `features`: a feature of the schema at its place - `column.index.using`, `column.partition_by`,
//!   `table.triggers`, `table.grant`, `schema.owner`, ... - optionally of one value (`value: gin`),
//!   kept, dropped (`drop: true`) or refused
//!
//! What no rule maps is refused: a type, a default, every refusal listed at once, nothing written.
//! Custom SQL - the `sql` of a table, a column, an index or a foreign key, and a table's
//! `constraint` - is one vendor's own and is left out on a cross-vendor apply, each one said. A
//! column's `sql` of keywords every vendor reads alike is converted instead, to the schema's own
//! fields (owner, 2026-11-18): `UNIQUE` to its index's `unique: true` (the index named
//! `<table>_<column>_key` when it has none), `NOT NULL` and `NULL` to `nullable`, `PRIMARY KEY` to
//! `primaryKey` - as a snapshot writes `sql: UNIQUE` for a unique column.
//! The same vendor maps nothing: the schema is applied as it is.

use regex::Regex;
use yaml_rust2::yaml::Hash;
use yaml_rust2::{Yaml, YamlLoader};

/// The vendor a schema was taken from, and its version when known: `postgres.16`
#[derive(Debug, Clone, PartialEq)]
pub struct Source {
    pub vendor: String,
    pub version: Option<String>,
}

/// A schema without a `source` is PostgreSQL's
pub const DEFAULT_VENDOR: &str = "postgres";

impl Source {
    /// `vendor` or `vendor.version`: a vendor of letters, digits and `_`, a version of
    /// dot-separated parts of letters, digits, `_` and `-`
    pub fn parse(s: &str) -> Result<Source, String> {
        let s = s.trim();
        let (vendor, version) = match s.split_once('.') {
            Some((v, ver)) => (v, Some(ver)),
            None => (s, None),
        };
        let word = |w: &str| !w.is_empty() && w.chars().all(|c| c.is_ascii_alphanumeric() || c == '_');
        if !word(vendor) {
            return Err(format!("source `{}`: a vendor, then an optional .version (postgres.16)", s));
        }
        if let Some(ver) = version {
            if !ver.split('.').all(|p| !p.is_empty() && p.chars().all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')) {
                return Err(format!("source `{}`: its version is dot-separated parts (postgres.16, mysql.8.0)", s));
            }
        }
        Ok(Source { vendor: vendor.to_lowercase(), version: version.map(|v| v.to_lowercase()) })
    }

    /// The schema's `source`, PostgreSQL's when it gives none
    pub fn of(schema: &Yaml) -> Result<Source, String> {
        match &schema["source"] {
            Yaml::BadValue | Yaml::Null => Ok(Source { vendor: DEFAULT_VENDOR.into(), version: None }),
            Yaml::String(s) => Source::parse(s),
            Yaml::Real(s) => Source::parse(s),
            other => Err(format!("source: a string (postgres.16), not {:?}", other)),
        }
    }

    /// `vendor.version`, or the vendor alone
    pub fn name(&self) -> String {
        match &self.version {
            Some(v) => format!("{}.{}", self.vendor, v),
            None => self.vendor.clone(),
        }
    }

    /// a version case `for` (`mysql.8`) may apply to this source: its vendor, and a version
    /// at or below this one's - only when a version is set
    fn is_case_of(&self, case: &Source) -> bool {
        case.vendor == self.vendor && match (&self.version, &case.version) {
            (Some(mine), Some(theirs)) => cmp_versions(theirs, mine) != std::cmp::Ordering::Greater,
            _ => false,
        }
    }
}

/// a version's parts in order, each its leading number and the rest: `23ai` is (23, "ai")
fn version_parts(v: &str) -> Vec<(u64, String)> {
    v.split('.').map(|p| {
        let digits: String = p.chars().take_while(|c| c.is_ascii_digit()).collect();
        (digits.parse().unwrap_or(0), p[digits.len()..].to_string())
    }).collect()
}

/// two versions part by part, a part missing being the lesser: 8 < 8.0 < 8.0.13 < 8.4 < 9 < 23ai
fn cmp_versions(a: &str, b: &str) -> std::cmp::Ordering {
    version_parts(a).cmp(&version_parts(b))
}

/// What a rule does with what it matched
#[derive(Debug, Clone, PartialEq)]
enum Action {
    /// written as this, `$1`... of the match put in
    To(String),
    Keep,
    Drop,
    Refuse(String),
}

#[derive(Debug, Clone)]
struct ValueRule {
    matcher: Regex,
    action: Action,
    /// [review 2026-11-18] said when the rule maps: what the target loses by it
    note: Option<String>,
    /// [review 2026-11-18] a default's rule for columns of this type only (the source's type)
    when_type: Option<Regex>,
}

#[derive(Debug, Clone)]
struct FeatureRule {
    at: String,
    value: Option<String>,
    action: Action,
    note: Option<String>,
}

#[derive(Debug, Clone, Default)]
struct Rules {
    types: Vec<ValueRule>,
    defaults: Vec<ValueRule>,
    features: Vec<FeatureRule>,
}

/// A mapping from one vendor's schemas to another's: the default rules, and the rules of the
/// versions that differ
#[derive(Debug, Clone)]
pub struct Mapping {
    pub from: String,
    pub to: String,
    default: Rules,
    versions: Vec<(Source, Rules)>,
}

/// What a mapping made of a schema: the target's schema, and what it left out, said
#[derive(Debug, Clone)]
pub struct Mapped {
    pub schema: Yaml,
    pub notes: Vec<String>,
}

// the mappings schema_guard carries: one a pair of vendors
const BUILTIN: &[(&str, &str, &str)] = &[
    ("postgres", "mysql", include_str!("mappings/postgres-mysql.yaml")),
    ("postgres", "sqlite", include_str!("mappings/postgres-sqlite.yaml")),
    // [mapping] from SQLite and from MySQL (owner, 2026-11-19)
    ("sqlite", "postgres", include_str!("mappings/sqlite-postgres.yaml")),
    ("sqlite", "mysql", include_str!("mappings/sqlite-mysql.yaml")),
    ("mysql", "postgres", include_str!("mappings/mysql-postgres.yaml")),
    ("mysql", "sqlite", include_str!("mappings/mysql-sqlite.yaml")),
];

fn action_of(rule: &Yaml, place: &str) -> Result<Action, String> {
    if let Some(to) = rule["to"].as_str() {
        return Ok(Action::To(to.to_string()));
    }
    if let Some(why) = rule["refuse"].as_str() {
        return Ok(Action::Refuse(why.to_string()));
    }
    if rule["drop"].as_bool() == Some(true) {
        return Ok(Action::Drop);
    }
    if rule["keep"].as_bool() == Some(true) {
        return Ok(Action::Keep);
    }
    Err(format!("{}: a rule says what it does - to, keep: true, drop: true or refuse: why", place))
}

fn value_rules(list: &Yaml, place: &str) -> Result<Vec<ValueRule>, String> {
    let mut out = Vec::new();
    for (i, r) in list.as_vec().map(|v| v.as_slice()).unwrap_or(&[]).iter().enumerate() {
        let at = format!("{}[{}]", place, i);
        let m = r["match"].as_str().ok_or_else(|| format!("{}: no match", at))?;
        let matcher = Regex::new(&format!("(?i)^(?:{})$", m)).map_err(|e| format!("{}: match `{}`: {}", at, m, e))?;
        let action = action_of(r, &at)?;
        if action == Action::Drop {
            return Err(format!("{}: a type or a default is mapped, kept or refused, not dropped", at));
        }
        let when_type = match r["type"].as_str() {
            Some(t) => Some(Regex::new(&format!("(?i)^(?:{})$", t)).map_err(|e| format!("{}: type `{}`: {}", at, t, e))?),
            None => None,
        };
        out.push(ValueRule { matcher, action, note: r["note"].as_str().map(String::from), when_type });
    }
    Ok(out)
}

fn rules_of(y: &Yaml, place: &str) -> Result<Rules, String> {
    let mut features = Vec::new();
    for (i, r) in y["features"].as_vec().map(|v| v.as_slice()).unwrap_or(&[]).iter().enumerate() {
        let at = format!("{}.features[{}]", place, i);
        let path = r["at"].as_str().ok_or_else(|| format!("{}: no at", at))?;
        if !FEATURES.contains(&path) {
            return Err(format!("{}: at `{}` is none of {}", at, path, FEATURES.join(", ")));
        }
        let value = match &r["value"] {
            Yaml::String(s) => Some(s.to_lowercase()),
            Yaml::Boolean(b) => Some(b.to_string()),
            Yaml::BadValue | Yaml::Null => None,
            other => return Err(format!("{}: value {:?} is no string", at, other)),
        };
        let action = action_of(r, &at)?;
        if matches!(action, Action::To(_)) {
            return Err(format!("{}: a feature is kept, dropped or refused", at));
        }
        features.push(FeatureRule { at: path.to_string(), value, action, note: r["note"].as_str().map(String::from) });
    }
    Ok(Rules {
        types: value_rules(&y["types"], &format!("{}.types", place))?,
        defaults: value_rules(&y["defaults"], &format!("{}.defaults", place))?,
        features,
    })
}

/// The places a feature rule names, and what a schema holds there
const FEATURES: &[&str] = &[
    "schema.owner",
    "table.owner",
    "table.triggers",
    "table.grant",
    "table.data",
    "table.data_file",
    "column.partition_by",
    "column.index",
    "column.index.using",
    "column.index.concurrently",
    "column.index.order",
    "column.index.nulls",
    "column.index.collate",
    "column.constraint.foreignKey",
];

impl Mapping {
    /// A mapping file's text
    pub fn parse(text: &str) -> Result<Mapping, String> {
        let docs = YamlLoader::load_from_str(text).map_err(|e| format!("mapping: {}", e))?;
        let y = docs.first().ok_or("mapping: an empty file")?;
        let from = y["from"].as_str().ok_or("mapping: no from")?.to_lowercase();
        let to = y["to"].as_str().ok_or("mapping: no to")?.to_lowercase();
        let default = rules_of(&y["default"], "default")?;
        let mut versions = Vec::new();
        for (i, v) in y["versions"].as_vec().map(|v| v.as_slice()).unwrap_or(&[]).iter().enumerate() {
            let place = format!("versions[{}]", i);
            let case = Source::parse(v["for"].as_str().ok_or_else(|| format!("{}: no for", place))?)?;
            if case.version.is_none() || (case.vendor != from && case.vendor != to) {
                return Err(format!("{}: for is a version of {} or {} (mysql.5.7)", place, from, to));
            }
            versions.push((case, rules_of(v, &place)?));
        }
        Ok(Mapping { from, to, default, versions })
    }

    /// The one schema_guard carries for that pair of vendors, if any
    pub fn builtin(from: &str, to: &str) -> Option<Result<Mapping, String>> {
        BUILTIN.iter().find(|(f, t, _)| *f == from && *t == to).map(|(_, _, text)| Mapping::parse(text))
    }

    /// the rules in the order they are tried: the target's version case, the source's, then
    /// the default - of each side's cases the newest at or below its version, the one a newer
    /// server keeps by backward compatibility
    fn rules_for(&self, source: &Source, target: &Source) -> Vec<&Rules> {
        let nearest = |side: &Source| self.versions.iter()
            .filter(|(case, _)| side.is_case_of(case))
            .max_by(|a, b| cmp_versions(a.0.version.as_deref().unwrap_or(""), b.0.version.as_deref().unwrap_or("")))
            .map(|(_, r)| r);
        let mut out: Vec<&Rules> = Vec::new();
        out.extend(nearest(target));
        out.extend(nearest(source));
        out.push(&self.default);
        out
    }

    /// The schema written as the target's: its types and defaults mapped, its features kept,
    /// dropped or refused, its custom SQL left out, its `source` the target's. Every refusal
    /// at once when there is any
    pub fn apply(&self, schema: &Yaml, target: &Source) -> Result<Mapped, Vec<String>> {
        let source = Source::of(schema).map_err(|e| vec![e])?;
        if source.vendor == target.vendor {
            return Ok(Mapped { schema: schema.clone(), notes: Vec::new() });
        }
        if source.vendor != self.from || target.vendor != self.to {
            return Err(vec![format!("this mapping is {} to {}, the schema is {} and the target {}",
                                    self.from, self.to, source.name(), target.name())]);
        }
        let rules = self.rules_for(&source, target);
        let mut out = schema.clone();
        let mut w = Walk { rules, refused: Vec::new(), notes: Vec::new() };
        if let Yaml::Hash(top) = &mut out {
            top.insert(Yaml::String("source".into()), Yaml::String(target.name()));
            if let Some(Yaml::Array(schemas)) = top.get_mut(&Yaml::String("database".into())) {
                for s in schemas.iter_mut() {
                    w.schema(s);
                }
            }
        }
        if w.refused.is_empty() {
            Ok(Mapped { schema: out, notes: w.notes })
        } else {
            Err(w.refused)
        }
    }
}

struct Walk<'a> {
    rules: Vec<&'a Rules>,
    refused: Vec<String>,
    notes: Vec<String>,
}

fn key(k: &str) -> Yaml {
    Yaml::String(k.to_string())
}

/// a value as a rule's text: a string, a number or a bool as written
fn text_of(y: &Yaml) -> Option<String> {
    match y {
        Yaml::String(s) | Yaml::Real(s) => Some(s.clone()),
        Yaml::Integer(i) => Some(i.to_string()),
        Yaml::Boolean(b) => Some(b.to_string()),
        _ => None,
    }
}

// a default every vendor reads alike: a number, a quoted string ('' inside it is a quote), true,
// false, null
fn is_literal(v: &str) -> bool {
    let v = v.trim();
    v.parse::<f64>().is_ok()
        || Regex::new(r"^'(?:[^']|'')*'$").map(|r| r.is_match(v)).unwrap_or(false)
        || matches!(v.to_lowercase().as_str(), "true" | "false" | "null")
}

fn fill(template: &str, caps: &regex::Captures) -> String {
    let mut out = String::new();
    caps.expand(template, &mut out);
    out
}

impl<'a> Walk<'a> {
    /// a type or a default through the value rules: the first that matches (a default's,
    /// of a rule for its column's type, or of any)
    fn value(&mut self, pick: fn(&Rules) -> &Vec<ValueRule>, value: &str, what: &str, of_type: Option<&str>) -> Option<String> {
        for r in &self.rules {
            for rule in pick(r) {
                if let (Some(want), Some(ty)) = (&rule.when_type, of_type) {
                    if !want.is_match(ty.trim()) {
                        continue;
                    }
                }
                if let Some(caps) = rule.matcher.captures(value.trim()) {
                    if let (Some(n), false) = (&rule.note, matches!(rule.action, Action::Refuse(_))) {
                        self.notes.push(format!("{} `{}`: {}", what, value, n));
                    }
                    return match &rule.action {
                        Action::To(t) => Some(fill(t, &caps)),
                        Action::Keep => Some(value.to_string()),
                        Action::Refuse(why) => {
                            self.refused.push(format!("{} `{}`: {}", what, value, why));
                            None
                        }
                        Action::Drop => None,
                    };
                }
            }
        }
        None
    }

    /// a feature at its place: Keep, Drop or a refusal; Keep when no rule names it
    fn feature(&mut self, at: &str, value: Option<&str>, what: &str) -> Action {
        for r in &self.rules {
            for f in &r.features {
                let hit = f.at == at && match (&f.value, value) {
                    (None, _) => true,
                    (Some(want), Some(v)) => want == &v.to_lowercase(),
                    (Some(_), None) => false,
                };
                if hit {
                    if let (Some(n), Action::Keep) = (&f.note, &f.action) {
                        self.notes.push(format!("{} ({}): {}", what, at, n));
                    }
                    if let Action::Refuse(why) = &f.action {
                        self.refused.push(format!("{} ({}{}): {}", what, at, value.map(|v| format!(" {}", v)).unwrap_or_default(), why));
                    }
                    return f.action.clone();
                }
            }
        }
        Action::Keep
    }

    /// a key of a hash through a feature rule: dropped, or refused, or kept
    fn feature_key(&mut self, h: &mut Hash, k: &str, at: &str, what: &str) {
        let Some(v) = h.get(&key(k)) else { return };
        if matches!(v, Yaml::Null | Yaml::BadValue) {
            return;
        }
        let value = text_of(v);
        if self.feature(at, value.as_deref(), what) == Action::Drop {
            h.remove(&key(k));
            self.notes.push(format!("{}: {} left out", what, at));
        }
    }

    /// custom SQL: one vendor's own, left out on a cross-vendor apply
    fn sql(&mut self, h: &mut Hash, k: &str, what: &str) {
        if let Some(v) = h.get(&key(k)) {
            if !matches!(v, Yaml::Null | Yaml::BadValue) && text_of(v).map(|s| !s.trim().is_empty()).unwrap_or(true) {
                h.remove(&key(k));
                self.notes.push(format!("{}: its custom SQL ({}) left out on a cross-vendor apply", what, k));
            }
        }
    }

    fn schema(&mut self, s: &mut Yaml) {
        let name = s["schema"]["schemaName"].as_str().or(s["schemaName"].as_str()).unwrap_or("").to_string();
        // a schema's entry is `- schema:` with its keys beside it, or under it
        let h = match s {
            Yaml::Hash(h) => h,
            _ => return,
        };
        let what = format!("schema {}", name);
        self.feature_key(h, "owner", "schema.owner", &what);
        if let Some(Yaml::Hash(inner)) = h.get_mut(&key("schema")) {
            self.feature_key(inner, "owner", "schema.owner", &what);
        }
        let tables = match h.get_mut(&key("tables")) {
            Some(Yaml::Array(t)) => t,
            _ => match h.get_mut(&key("schema")).and_then(|i| if let Yaml::Hash(i) = i { i.get_mut(&key("tables")) } else { None }) {
                Some(Yaml::Array(t)) => t,
                _ => return,
            },
        };
        let mut tables = std::mem::take(tables);
        for t in tables.iter_mut() {
            if let Some(Yaml::Hash(t)) = match t { Yaml::Hash(h) => h.get_mut(&key("table")), _ => None } {
                self.table(t, &name);
            }
        }
        if let Some(Yaml::Array(t)) = h.get_mut(&key("tables")) {
            *t = tables;
        } else if let Some(Yaml::Hash(inner)) = h.get_mut(&key("schema")) {
            if let Some(Yaml::Array(t)) = inner.get_mut(&key("tables")) {
                *t = tables;
            }
        }
    }

    fn table(&mut self, t: &mut Hash, schema: &str) {
        let name = format!("{}.{}", schema, t.get(&key("tableName")).and_then(|v| v.as_str()).unwrap_or(""));
        let what = format!("table {}", name);
        self.sql(t, "sql", &what);
        // a table's constraint is SQL of its own; a column's is a map, read below
        if t.get(&key("constraint")).map(|c| !matches!(c, Yaml::Hash(_))).unwrap_or(false) {
            self.sql(t, "constraint", &what);
        }
        self.feature_key(t, "owner", "table.owner", &what);
        self.feature_key(t, "triggers", "table.triggers", &what);
        self.feature_key(t, "grant", "table.grant", &what);
        self.feature_key(t, "data", "table.data", &what);
        self.feature_key(t, "data_file", "table.data_file", &what);
        if let Some(Yaml::Array(cols)) = t.get_mut(&key("columns")) {
            for c in cols.iter_mut() {
                if let Yaml::Hash(c) = c {
                    if let Some(Yaml::Hash(c)) = c.get_mut(&key("column")) {
                        self.column(c, &name);
                    }
                }
            }
        }
    }

    /// a column's `sql` of keywords every vendor reads alike, as the schema's own fields: UNIQUE,
    /// NOT NULL, NULL, PRIMARY KEY; anything else in it stays custom SQL
    fn keywords(&mut self, c: &mut Hash, table: &str, column: &str, what: &str) {
        let Some(sql) = c.get(&key("sql")).and_then(text_of) else { return };
        let words: Vec<String> = sql.split_whitespace().map(|w| w.to_uppercase()).collect();
        if words.is_empty() {
            return;
        }
        let (mut unique, mut nullable, mut pk) = (false, None, false);
        let mut i = 0;
        while i < words.len() {
            match (words[i].as_str(), words.get(i + 1).map(|w| w.as_str())) {
                ("UNIQUE", _) => { unique = true; i += 1; }
                ("NOT", Some("NULL")) => { nullable = Some(false); i += 2; }
                ("NULL", _) => { nullable = Some(true); i += 1; }
                ("PRIMARY", Some("KEY")) => { pk = true; i += 2; }
                _ => return, // other SQL: custom, left out by sql()
            }
        }
        c.remove(&key("sql"));
        if unique {
            let auto = Yaml::String(format!("{}_{}_key", table, column));
            match c.get_mut(&key("index")) {
                Some(Yaml::Hash(idx)) => {
                    idx.entry(key("name")).or_insert(auto);
                    idx.insert(key("unique"), Yaml::Boolean(true));
                }
                _ => {
                    let mut idx = Hash::new();
                    idx.insert(key("name"), auto);
                    idx.insert(key("unique"), Yaml::Boolean(true));
                    c.insert(key("index"), Yaml::Hash(idx));
                }
            }
        }
        if nullable.is_some() || pk {
            if !matches!(c.get(&key("constraint")), Some(Yaml::Hash(_))) {
                c.insert(key("constraint"), Yaml::Hash(Hash::new()));
            }
            if let Some(Yaml::Hash(cons)) = c.get_mut(&key("constraint")) {
                if let Some(n) = nullable {
                    cons.insert(key("nullable"), Yaml::Boolean(n));
                }
                if pk {
                    cons.insert(key("primaryKey"), Yaml::Boolean(true));
                }
            }
        }
        self.notes.push(format!("{}: sql `{}` converted to the schema's own fields", what, sql.trim()));
    }

    fn column(&mut self, c: &mut Hash, table: &str) {
        let table_name = table.rsplit('.').next().unwrap_or(table).to_string();
        let column_name = c.get(&key("name")).and_then(|v| v.as_str()).unwrap_or("").to_string();
        let name = format!("{}.{}", table, c.get(&key("name")).and_then(|v| v.as_str()).unwrap_or(""));
        let what = format!("column {}", name);
        let source_type = c.get(&key("type")).and_then(text_of);
        if let Some(ty) = source_type.clone() {
            match self.value(|r| &r.types, &ty, &format!("{}: type", what), None) {
                Some(mapped) => { c.insert(key("type"), Yaml::String(mapped)); }
                None if !self.refused.iter().any(|r| r.starts_with(&format!("{}: type", what))) =>
                    self.refused.push(format!("{}: type `{}`: no rule maps it", what, ty)),
                None => {}
            }
        }
        if let Some(dv) = c.get(&key("defaultValue")).and_then(text_of) {
            // [mapping] a literal goes through the rules too, and stays as written when none
            // names it: MySQL's and SQLite's booleans default to 0 and 1, PostgreSQL's to false
            // and true (2026-11-19)
            let literal = is_literal(&dv);
            if !dv.trim().is_empty() {
                match self.value(|r| &r.defaults, &dv, &format!("{}: default", what), source_type.as_deref()) {
                    Some(mapped) => { c.insert(key("defaultValue"), Yaml::String(mapped)); }
                    None if !self.refused.iter().any(|r| r.starts_with(&format!("{}: default", what))) => if !literal {
                        self.refused.push(format!("{}: default `{}`: no rule maps it", what, dv))
                    },
                    None => {}
                }
            }
        }
        self.keywords(c, &table_name, &column_name, &what);
        self.sql(c, "sql", &what);
        self.feature_key(c, "partition_by", "column.partition_by", &what);
        if let Some(Yaml::Hash(cons)) = c.get_mut(&key("constraint")) {
            if let Some(Yaml::Hash(fk)) = cons.get_mut(&key("foreignKey")) {
                let what_fk = format!("{} foreign key", what);
                self.sql(fk, "sql", &what_fk);
            }
            self.feature_key(cons, "foreignKey", "column.constraint.foreignKey", &what);
        }
        if c.get(&key("index")).map(|i| !matches!(i, Yaml::Null | Yaml::BadValue)).unwrap_or(false) {
            let idx_value = c.get(&key("index")).and_then(text_of);
            if self.feature("column.index", idx_value.as_deref(), &what) == Action::Drop {
                c.remove(&key("index"));
                self.notes.push(format!("{}: column.index left out", what));
            } else if let Some(Yaml::Hash(idx)) = c.get_mut(&key("index")) {
                let what_idx = format!("{} index", what);
                self.sql(idx, "sql", &what_idx);
                for (k, at) in [("using", "column.index.using"), ("concurrently", "column.index.concurrently"),
                                ("order", "column.index.order"), ("nulls", "column.index.nulls"),
                                ("collate", "column.index.collate")] {
                    self.feature_key(idx, k, at, &what_idx);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    const SCHEMA: &str = "
source: postgres.16
database:
  - schema:
    schemaName: app
    owner: app_owner
    tables:
      - table:
          tableName: orders
          sql: WITH (fillfactor = 70)
          constraint: CHECK (total >= 0)
          columns:
            - column:
                name: id
                type: bigserial
                constraint:
                  primaryKey: true
            - column:
                name: placed
                type: timestamptz
                defaultValue: now()
            - column:
                name: total
                type: numeric(12,2)
                defaultValue: \"0\"
            - column:
                name: tags
                type: jsonb
                index:
                  name: orders_tags
                  using: gin
            - column:
                name: customer
                type: varchar(80)
                sql: COLLATE \"C\"
                constraint:
                  foreignKey:
                    references: app.customers
                    sql: ON DELETE CASCADE
          triggers:
            - trigger:
                name: orders_audit
                event: after insert
                when: for each row
                proc: app.audit()
          grant:
            - all: app_rw
";

    fn schema() -> Yaml {
        YamlLoader::load_from_str(SCHEMA).unwrap().remove(0)
    }

    fn col<'y>(y: &'y Yaml, name: &str) -> &'y Yaml {
        y["database"][0]["tables"][0]["table"]["columns"].as_vec().unwrap().iter()
            .map(|c| &c["column"]).find(|c| c["name"].as_str() == Some(name)).unwrap()
    }

    #[test]
    fn a_schema_with_its_source_is_valid_and_reads_as_before() {
        // the validator takes the top level's source, and the schema reads as without it
        let y = crate::load_schema_from_src(SCHEMA.to_string()).unwrap();
        assert_eq!(y["source"].as_str(), Some("postgres.16"));
        let with = crate::parse_yaml_schema(y, "S1__a.yaml").unwrap();
        let without = crate::parse_yaml_schema(
            crate::load_schema_from_src(SCHEMA.replace("source: postgres.16\n", "")).unwrap(), "S1__a.yaml").unwrap();
        assert_eq!(with.list.len(), without.list.len());
        assert!(crate::load_schema_from_src(SCHEMA.replace("source: postgres.16", "source: [1]")).is_err());
    }

    #[test]
    fn a_source_is_a_vendor_and_its_version() {
        assert_eq!(Source::parse("postgres.16").unwrap(), Source { vendor: "postgres".into(), version: Some("16".into()) });
        assert_eq!(Source::parse("MySQL.8.0").unwrap(), Source { vendor: "mysql".into(), version: Some("8.0".into()) });
        assert_eq!(Source::parse("sqlite").unwrap().version, None);
        assert!(Source::parse("").is_err());
        assert!(Source::parse("postgres..16").is_err());
        assert!(Source::parse("my sql").is_err());
        // none given: PostgreSQL's, as every schema before it
        let y = YamlLoader::load_from_str("database: []").unwrap().remove(0);
        assert_eq!(Source::of(&y).unwrap().vendor, "postgres");
        assert_eq!(Source::of(&schema()).unwrap().name(), "postgres.16");
    }

    #[test]
    fn a_version_case_applies_to_its_version_and_every_newer_one() {
        let case = Source::parse("mysql.8").unwrap();
        assert!(Source::parse("mysql.8.0").unwrap().is_case_of(&case));
        assert!(Source::parse("mysql.8").unwrap().is_case_of(&case));
        // a newer server takes an older one's case: backward compatible
        assert!(Source::parse("mysql.9.1").unwrap().is_case_of(&case));
        assert!(Source::parse("mysql.80").unwrap().is_case_of(&case));
        assert!(!Source::parse("mysql.5.7").unwrap().is_case_of(&case));
        // no version set: only the default applies
        assert!(!Source::parse("mysql").unwrap().is_case_of(&case));
        assert!(!Source::parse("mariadb.8.0").unwrap().is_case_of(&case));
        // part by part, a part missing the lesser; a vendor's own forms (23ai) by their number
        use std::cmp::Ordering::*;
        assert_eq!(cmp_versions("8", "8.0"), Less);
        assert_eq!(cmp_versions("8.0.13", "8.4"), Less);
        assert_eq!(cmp_versions("9", "8.4"), Greater);
        assert_eq!(cmp_versions("19c", "23ai"), Less);
        assert_eq!(cmp_versions("16", "16"), Equal);
    }

    #[test]
    fn of_its_cases_the_newest_at_or_below_the_version_applies() {
        let m = Mapping::parse("
from: postgres
to: mysql
default:
  types:
    - { match: 'jsonb', to: 'json' }
versions:
  - for: mysql.5
    types:
      - { match: 'jsonb', to: 'longtext' }
  - for: mysql.8
    types:
      - { match: 'jsonb', to: 'json /* 8 */' }
").unwrap();
        let y = YamlLoader::load_from_str("database:\n  - schemaName: a\n    tables:\n      - table:\n          tableName: t\n          columns:\n            - column:\n                name: j\n                type: jsonb\n").unwrap().remove(0);
        let ty = |target: &str| {
            let out = m.apply(&y, &Source::parse(target).unwrap()).unwrap();
            out.schema["database"][0]["tables"][0]["table"]["columns"][0]["column"]["type"].as_str().unwrap().to_string()
        };
        assert_eq!(ty("mysql.5.7"), "longtext");
        assert_eq!(ty("mysql.8.0"), "json /* 8 */");
        assert_eq!(ty("mysql.9.1"), "json /* 8 */"); // the newest case at or below 9.1
        assert_eq!(ty("mysql.4.1"), "json"); // no case that old: the default
        assert_eq!(ty("mysql"), "json"); // no version: the default
    }

    #[test]
    fn the_builtin_mappings_parse() {
        for (from, to, _) in BUILTIN {
            let m = Mapping::builtin(from, to).unwrap().unwrap();
            assert_eq!((m.from.as_str(), m.to.as_str()), (*from, *to));
        }
        assert!(Mapping::builtin("oracle", "postgres").is_none());
    }

    #[test]
    fn the_same_vendor_maps_nothing() {
        let m = Mapping::builtin("postgres", "mysql").unwrap().unwrap();
        let out = m.apply(&schema(), &Source::parse("postgres.17").unwrap()).unwrap();
        assert_eq!(out.schema, schema());
        assert!(out.notes.is_empty());
    }

    #[test]
    fn a_postgres_schema_written_as_mysqls() {
        let m = Mapping::builtin("postgres", "mysql").unwrap().unwrap();
        // GIN is refused by the default rules: the schema as given cannot go
        let refused = m.apply(&schema(), &Source::parse("mysql.8.0").unwrap()).unwrap_err();
        assert!(refused.iter().any(|r| r.contains("app.orders.tags") && r.contains("using gin")), "{:?}", refused);
        // without it, the rest maps
        let text = SCHEMA.replace("                  using: gin\n", "");
        let y = YamlLoader::load_from_str(&text).unwrap().remove(0);
        let out = m.apply(&y, &Source::parse("mysql.8.0").unwrap()).unwrap();
        let s = &out.schema;
        assert_eq!(s["source"].as_str(), Some("mysql.8.0"));
        assert_eq!(col(s, "id")["type"].as_str(), Some("bigint auto_increment"));
        assert_eq!(col(s, "placed")["type"].as_str(), Some("datetime(6)"));
        assert_eq!(col(s, "placed")["defaultValue"].as_str(), Some("CURRENT_TIMESTAMP(6)"));
        assert_eq!(col(s, "total")["type"].as_str(), Some("decimal(12,2)"));
        assert_eq!(col(s, "total")["defaultValue"].as_str(), Some("0"));
        assert_eq!(col(s, "tags")["type"].as_str(), Some("json"));
        assert_eq!(col(s, "customer")["type"].as_str(), Some("varchar(80)"));
        // custom SQL left out, each one said
        assert!(col(s, "customer")["sql"].is_badvalue());
        assert!(col(s, "customer")["constraint"]["foreignKey"]["sql"].is_badvalue());
        assert_eq!(col(s, "customer")["constraint"]["foreignKey"]["references"].as_str(), Some("app.customers"));
        let t = &s["database"][0]["tables"][0]["table"];
        assert!(t["sql"].is_badvalue() && t["constraint"].is_badvalue());
        assert!(out.notes.iter().filter(|n| n.contains("custom SQL")).count() >= 4, "{:?}", out.notes);
        // PostgreSQL's own: its triggers (their functions are its), its grants and owner, left out
        assert!(t["triggers"].is_badvalue() && t["grant"].is_badvalue());
        assert!(s["database"][0]["owner"].is_badvalue());
        // a column's constraint is a map, kept
        assert_eq!(col(s, "id")["constraint"]["primaryKey"].as_bool(), Some(true));
    }

    #[test]
    fn a_columns_unique_becomes_its_index_and_a_quoted_default_is_a_literal() {
        let m = Mapping::builtin("postgres", "mysql").unwrap().unwrap();
        let text = SCHEMA.replace("                  using: gin\n", "")
            .replace("                sql: COLLATE \"C\"\n", "                sql: unique\n                defaultValue: \"'it''s'\"\n");
        let y = YamlLoader::load_from_str(&text).unwrap().remove(0);
        let out = m.apply(&y, &Source::parse("mysql.8.0").unwrap()).unwrap();
        // [owner, 2026-11-18] sql: UNIQUE converted to unique: true, its index named as PostgreSQL would
        let c = col(&out.schema, "customer");
        assert!(c["sql"].is_badvalue());
        assert_eq!(c["index"]["unique"].as_bool(), Some(true));
        assert_eq!(c["index"]["name"].as_str(), Some("orders_customer_key"));
        assert!(out.notes.iter().any(|n| n.contains("app.orders.customer: sql `unique` converted")), "{:?}", out.notes);
        assert_eq!(col(&out.schema, "customer")["defaultValue"].as_str(), Some("'it''s'"));
        assert!(is_literal("'a'") && is_literal("''") && is_literal("-1.5") && is_literal("NULL"));
        assert!(!is_literal("'a'::text") && !is_literal("now()") && !is_literal("'a' || 'b'"));
    }

    /// [review 2026-11-18] what the first review found, each held by a test
    #[test]
    fn what_a_target_would_lose_is_refused_or_said() {
        let m = Mapping::builtin("postgres", "mysql").unwrap().unwrap();
        let apply = |from: &str, to: &str| {
            let text = SCHEMA.replace("                  using: gin\n", "").replace(from, to);
            let y = YamlLoader::load_from_str(&text).unwrap().remove(0);
            m.apply(&y, &Source::parse("mysql.8.0").unwrap())
        };
        // a snapshot's bpchar has no length: refused, not written char(1)
        let r = apply("type: varchar(80)", "type: bpchar").unwrap_err();
        assert!(r.iter().any(|e| e.contains("customer: type `bpchar`")), "{:?}", r);
        // one quoted string and its cast, not two
        let r = apply("defaultValue: now()", "defaultValue: \"'a'::text || 'b'::text\"").unwrap_err();
        assert!(r.iter().any(|e| e.contains("placed: default")), "{:?}", r);
        let out = apply("defaultValue: now()", "defaultValue: \"'it''s'::character varying\"").unwrap();
        assert_eq!(col(&out.schema, "placed")["defaultValue"].as_str(), Some("'it''s'"));
        // now() on a date column: the date
        let out = apply("type: timestamptz", "type: date").unwrap();
        assert_eq!(col(&out.schema, "placed")["defaultValue"].as_str(), Some("(CURRENT_DATE)"));
        // an index method MySQL has not: refused; hash, which InnoDB builds as a b-tree: left out, said
        let text = SCHEMA.replace("using: gin", "using: spgist");
        let r = m.apply(&YamlLoader::load_from_str(&text).unwrap().remove(0), &Source::parse("mysql.8.0").unwrap()).unwrap_err();
        assert!(r.iter().any(|e| e.contains("using spgist")), "{:?}", r);
        let text = SCHEMA.replace("using: gin", "using: hash");
        let out = m.apply(&YamlLoader::load_from_str(&text).unwrap().remove(0), &Source::parse("mysql.8.0").unwrap()).unwrap();
        assert!(col(&out.schema, "tags")["index"]["using"].is_badvalue());
        assert!(out.notes.iter().any(|n| n.contains("column.index.using left out")), "{:?}", out.notes);
        // a table's owner left out; its rows kept, said they are PostgreSQL's
        let out = apply("          tableName: orders\n", "          tableName: orders\n          owner: app_owner\n          data:\n            - ['1', 't']\n").unwrap();
        let t = &out.schema["database"][0]["tables"][0]["table"];
        assert!(t["owner"].is_badvalue());
        assert_eq!(t["data"][0][1].as_str(), Some("t"));
        assert!(out.notes.iter().any(|n| n.contains("table.data") && n.contains("not converted")), "{:?}", out.notes);
        // SQLite: a length it does not hold, said
        let s = Mapping::builtin("postgres", "sqlite").unwrap().unwrap();
        let y = YamlLoader::load_from_str(&SCHEMA.replace("                  using: gin\n", "")).unwrap().remove(0);
        let out = s.apply(&y, &Source::parse("sqlite.3").unwrap()).unwrap();
        assert!(out.notes.iter().any(|n| n.contains("customer: type `varchar(80)`: SQLite holds no length")), "{:?}", out.notes);
    }

    #[test]
    fn a_columns_keywords_become_its_fields_and_other_sql_stays_custom() {
        let m = Mapping::builtin("postgres", "mysql").unwrap().unwrap();
        let with = |sql: &str| {
            let text = SCHEMA.replace("                  using: gin\n", "")
                .replace("                sql: COLLATE \"C\"\n", &format!("                sql: {}\n", sql));
            let y = YamlLoader::load_from_str(&text).unwrap().remove(0);
            m.apply(&y, &Source::parse("mysql.8.0").unwrap()).unwrap()
        };
        let out = with("UNIQUE NOT NULL");
        let c = col(&out.schema, "customer");
        assert_eq!(c["index"]["unique"].as_bool(), Some(true));
        assert_eq!(c["constraint"]["nullable"].as_bool(), Some(false));
        // the foreign key of the constraint it already had is still there
        assert_eq!(c["constraint"]["foreignKey"]["references"].as_str(), Some("app.customers"));
        // an index it has takes unique, its own name kept
        let out = with("unique");
        assert_eq!(col(&out.schema, "tags")["index"]["name"].as_str(), Some("orders_tags"));
        let out = with("PRIMARY KEY");
        assert_eq!(col(&out.schema, "customer")["constraint"]["primaryKey"].as_bool(), Some(true));
        // anything else is one vendor's SQL: left out, said
        let out = with("UNIQUE DEFERRABLE");
        let c = col(&out.schema, "customer");
        assert!(c["sql"].is_badvalue() && c["index"].is_badvalue());
        assert!(out.notes.iter().any(|n| n.contains("app.orders.customer: its custom SQL")), "{:?}", out.notes);
    }

    #[test]
    fn a_version_case_goes_before_the_default() {
        let m = Mapping::builtin("postgres", "mysql").unwrap().unwrap();
        let text = SCHEMA.replace("                  using: gin\n", "");
        let y = YamlLoader::load_from_str(&text).unwrap().remove(0);
        // MySQL 5.7: datetime with fractions, and json, came in 5.6.4 and 5.7.8 - the 5.7 case
        // refuses jsonb before the default maps it
        let refused = m.apply(&y, &Source::parse("mysql.5.7").unwrap()).unwrap_err();
        assert!(refused.iter().any(|r| r.contains("app.orders.tags: type `jsonb`")), "{:?}", refused);
        // no version given: the default alone
        assert!(m.apply(&y, &Source::parse("mysql").unwrap()).is_ok());
    }

    #[test]
    fn every_refusal_is_listed_at_once() {
        let m = Mapping::parse("
from: postgres
to: mysql
default:
  types:
    - { match: 'int|integer', keep: true }
").unwrap();
        let refused = m.apply(&schema(), &Source::parse("mysql.8.0").unwrap()).unwrap_err();
        for c in ["id", "placed", "total", "tags", "customer"] {
            assert!(refused.iter().any(|r| r.contains(&format!("app.orders.{}: type", c))), "{}: {:?}", c, refused);
        }
        // a default that is no literal and no rule maps: refused too
        assert!(refused.iter().any(|r| r.contains("app.orders.placed: default `now()`: no rule maps it")), "{:?}", refused);
        // a literal default needs no rule
        assert!(!refused.iter().any(|r| r.contains("app.orders.total: default")), "{:?}", refused);
    }

    #[test]
    fn a_mapping_file_says_what_it_is_wrong_in() {
        assert!(Mapping::parse("to: mysql").unwrap_err().contains("no from"));
        assert!(Mapping::parse("from: postgres\nto: mysql\ndefault:\n  types:\n    - { match: 'int' }").unwrap_err()
            .contains("a rule says what it does"));
        assert!(Mapping::parse("from: postgres\nto: mysql\ndefault:\n  types:\n    - { match: '(', keep: true }").unwrap_err()
            .contains("match `(`"));
        assert!(Mapping::parse("from: postgres\nto: mysql\ndefault:\n  features:\n    - { at: column.nothing, drop: true }").unwrap_err()
            .contains("at `column.nothing`"));
        assert!(Mapping::parse("from: postgres\nto: mysql\nversions:\n  - for: oracle.19").unwrap_err()
            .contains("a version of postgres or mysql"));
        assert!(Mapping::parse("from: postgres\nto: mysql\nversions:\n  - for: mysql").unwrap_err()
            .contains("a version of"));
        // a mapping of another pair is not used on this one
        let m = Mapping::builtin("postgres", "sqlite").unwrap().unwrap();
        assert!(m.apply(&schema(), &Source::parse("mysql.8.0").unwrap()).unwrap_err()[0].contains("postgres to sqlite"));
    }

    #[test]
    fn a_postgres_schema_written_as_sqlites() {
        let m = Mapping::builtin("postgres", "sqlite").unwrap().unwrap();
        let text = SCHEMA.replace("                  using: gin\n", "");
        let y = YamlLoader::load_from_str(&text).unwrap().remove(0);
        let out = m.apply(&y, &Source::parse("sqlite.3").unwrap()).unwrap();
        let s = &out.schema;
        assert_eq!(s["source"].as_str(), Some("sqlite.3"));
        assert_eq!(col(s, "id")["type"].as_str(), Some("integer"));
        assert_eq!(col(s, "placed")["type"].as_str(), Some("text"));
        assert_eq!(col(s, "placed")["defaultValue"].as_str(), Some("CURRENT_TIMESTAMP"));
        assert_eq!(col(s, "tags")["type"].as_str(), Some("text"));
        assert_eq!(col(s, "total")["type"].as_str(), Some("numeric(12,2)"));
    }

    // [mapping] the matrix (src/mappings/matrix.yaml): every cell, and every rule reached
    const MATRIX: &str = include_str!("mappings/matrix.yaml");
    const SOURCES: [&str; 3] = ["postgres", "sqlite", "mysql"];

    fn matrix() -> Yaml {
        YamlLoader::load_from_str(MATRIX).unwrap().remove(0)
    }

    /// a schema of one table t with one column c, its type and default, and `extra` YAML at the
    /// column's level, the table's or the schema's
    fn one(source: &str, ty: &str, default: Option<&str>, col_extra: &str, table_extra: &str, schema_extra: &str) -> Yaml {
        let d = default.map(|d| format!("                defaultValue: {}\n", yaml_str(d))).unwrap_or_default();
        let text = format!("source: {}\ndatabase:\n  - schemaName: public\n{}    tables:\n      - table:\n          tableName: t\n{}          columns:\n            - column:\n                name: c\n                type: {}\n{}{}",
                           source, schema_extra, table_extra, yaml_str(ty), d, col_extra);
        YamlLoader::load_from_str(&text).unwrap().remove(0)
    }

    fn yaml_str(v: &str) -> String {
        format!("\"{}\"", v.replace('\\', "\\\\").replace('"', "\\\""))
    }

    /// each mapping parsed once for the matrix's every cell: a quick test
    fn mapping_for(from: &str, target: &str) -> &'static Mapping {
        static MAPPINGS: std::sync::OnceLock<HashMap<(&'static str, &'static str), Mapping>> = std::sync::OnceLock::new();
        let to = target.split('.').next().unwrap();
        let all = MAPPINGS.get_or_init(|| BUILTIN.iter()
            .map(|(f, t, _)| ((*f, *t), Mapping::builtin(f, t).unwrap().unwrap())).collect());
        all.iter().find(|((f, t), _)| *f == from && *t == to).map(|(_, m)| m).unwrap()
    }

    fn column_of(y: &Yaml) -> &Yaml {
        &y["database"][0]["tables"][0]["table"]["columns"][0]["column"]
    }

    #[test]
    fn every_cell_of_the_matrix_holds() {
        let all = matrix();
        let mut wrong = Vec::new();
        for src in SOURCES {
        let m = &all[src];
        let source = m["source"].as_str().unwrap();
        let targets: Vec<&str> = m["targets"].as_vec().unwrap().iter().map(|t| t.as_str().unwrap()).collect();
        let default_type = m["default_type"].as_str().unwrap();
        let feature_type = m["feature_type"].as_str().unwrap();
        for row in m["types"].as_vec().unwrap() {
            let from = text_of(&row["from"]).unwrap();
            for &t in &targets {
                let want = text_of(&row[t]).unwrap();
                let got = mapping_for(src, t).apply(&one(source, &from, None, "", "", ""), &Source::parse(t).unwrap());
                let ok = match (&got, want.as_str()) {
                    (Err(r), "refused") => r.iter().any(|e| e.contains("public.t.c: type")),
                    (Ok(out), w) => column_of(&out.schema)["type"].as_str() == Some(w),
                    _ => false,
                };
                if !ok { wrong.push(format!("{} type {} on {}: want {}, got {:?}", src, from, t, want, got.map(|o| column_of(&o.schema)["type"].clone()))); }
            }
        }
        for row in m["defaults"].as_vec().unwrap() {
            let from = text_of(&row["from"]).unwrap();
            let ty = row["type"].as_str().unwrap_or(default_type).to_string();
            for &t in &targets {
                let want = text_of(&row[t]).unwrap();
                let got = mapping_for(src, t).apply(&one(source, &ty, Some(&from), "", "", ""), &Source::parse(t).unwrap());
                let ok = match (&got, want.as_str()) {
                    (Err(r), "refused") => r.iter().any(|e| e.contains("public.t.c: default")),
                    (Ok(out), w) => text_of(&column_of(&out.schema)["defaultValue"]).as_deref() == Some(w),
                    _ => false,
                };
                if !ok { wrong.push(format!("{} default {} ({}) on {}: want {}, got {:?}", src, from, ty, t, want, got.map(|o| column_of(&o.schema)["defaultValue"].clone()))); }
            }
        }
        for row in m["features"].as_vec().unwrap() {
            let at = row["at"].as_str().unwrap();
            let value = text_of(&row["value"]).unwrap_or_default();
            let (col, table, schema, find): (String, String, String, fn(&Yaml) -> &Yaml) = match at {
                "column.index.using" | "column.index.concurrently" | "column.index.nulls" | "column.index.collate" => {
                    let k = at.rsplit('.').next().unwrap();
                    (format!("                index:\n                  name: i\n                  {}: {}\n", k, value), String::new(), String::new(),
                     |y| &column_of(y)["index"])
                }
                "column.partition_by" => (format!("                partition_by: {}\n", value), String::new(), String::new(), |y| column_of(y)),
                "table.triggers" => (String::new(), "          triggers:\n            - trigger:\n                name: tr\n                event: after insert\n                when: for each row\n                proc: f()\n".into(), String::new(), |y| &y["database"][0]["tables"][0]["table"]),
                "table.grant" => (String::new(), "          grant:\n            - all: r\n".into(), String::new(), |y| &y["database"][0]["tables"][0]["table"]),
                "table.owner" => (String::new(), "          owner: o\n".into(), String::new(), |y| &y["database"][0]["tables"][0]["table"]),
                "table.data" => (String::new(), "          data:\n            - ['1']\n".into(), String::new(), |y| &y["database"][0]["tables"][0]["table"]),
                "table.data_file" => (String::new(), "          data_file: rows.csv\n".into(), String::new(), |y| &y["database"][0]["tables"][0]["table"]),
                "schema.owner" => (String::new(), String::new(), "    owner: o\n".into(), |y| &y["database"][0]),
                other => panic!("no matrix case for {}", other),
            };
            let key = match at { "table.triggers" => "triggers", "table.grant" => "grant", "table.owner" | "schema.owner" => "owner",
                                 "table.data" => "data", "table.data_file" => "data_file", "column.partition_by" => "partition_by",
                                 a => a.rsplit('.').next().unwrap() };
            for &t in &targets {
                let want = row[t].as_str().unwrap();
                let got = mapping_for(src, t).apply(&one(source, feature_type, None, &col, &table, &schema), &Source::parse(t).unwrap());
                let ok = match (&got, want) {
                    (Err(r), "refused") => r.iter().any(|e| e.contains(at)),
                    (Ok(out), "keep") => !find(&out.schema)[key].is_badvalue(),
                    (Ok(out), "drop") => find(&out.schema)[key].is_badvalue() && out.notes.iter().any(|n| n.contains(at)),
                    _ => false,
                };
                if !ok { wrong.push(format!("{} feature {} {} on {}: want {}, got {:?}", src, at, value, t, want, got.map(|o| o.notes))); }
            }
        }
        }
        assert!(wrong.is_empty(), "the matrix's cells that do not hold:\n{}", wrong.join("\n"));
    }

    #[test]
    fn every_rule_of_every_mapping_is_reached_by_the_matrix() {
        let all = matrix();
        let mut missing = Vec::new();
        for (from, to, _) in BUILTIN {
        let m = &all[*from];
        let default_type = m["default_type"].as_str().unwrap();
        let types: Vec<String> = m["types"].as_vec().unwrap().iter().map(|r| text_of(&r["from"]).unwrap()).collect();
        let defaults: Vec<(String, String)> = m["defaults"].as_vec().unwrap().iter()
            .map(|r| (text_of(&r["from"]).unwrap(), r["type"].as_str().unwrap_or(default_type).to_string())).collect();
        let features: Vec<(String, Option<String>)> = m["features"].as_vec().unwrap().iter()
            .map(|r| (r["at"].as_str().unwrap().to_string(), text_of(&r["value"]).map(|v| v.to_lowercase()))).collect();
            let mapping = Mapping::builtin(from, to).unwrap().unwrap();
            let sets: Vec<(String, &Rules)> = std::iter::once(("default".to_string(), &mapping.default))
                .chain(mapping.versions.iter().map(|(c, r)| (c.name(), r))).collect();
            for (set, rules) in sets {
                for (i, r) in rules.types.iter().enumerate() {
                    let first = |v: &str| rules.types.iter().position(|x| x.matcher.is_match(v.trim()));
                    if !types.iter().any(|v| first(v) == Some(i)) {
                        missing.push(format!("{}-{} {}: types[{}] `{}`", from, to, set, i, r.matcher.as_str()));
                    }
                }
                for (i, r) in rules.defaults.iter().enumerate() {
                    let first = |v: &str, ty: &str| rules.defaults.iter().position(|x|
                        x.when_type.as_ref().map(|w| w.is_match(ty)).unwrap_or(true) && x.matcher.is_match(v.trim()));
                    if !defaults.iter().any(|(v, ty)| first(v, ty) == Some(i)) {
                        missing.push(format!("{}-{} {}: defaults[{}] `{}`", from, to, set, i, r.matcher.as_str()));
                    }
                }
                for (i, f) in rules.features.iter().enumerate() {
                    let first = |at: &str, v: &Option<String>| rules.features.iter().position(|x| x.at == at
                        && match (&x.value, v) { (None, _) => true, (Some(w), Some(v)) => w == v, (Some(_), None) => false });
                    if !features.iter().any(|(at, v)| first(at, v) == Some(i)) {
                        missing.push(format!("{}-{} {}: features[{}] at {} {:?}", from, to, set, i, f.at, f.value));
                    }
                }
            }
        }
        assert!(missing.is_empty(), "rules no row of the matrix reaches - add a row to src/mappings/matrix.yaml:\n{}", missing.join("\n"));
    }

}
