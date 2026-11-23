//! [security] Row access declared on a table (the key was `labac` before):
//! `security: {schemaPath: [dept], alias: dept, softDelete: deleted_at}`. Whether it is deployed, and
//! how, is the deploy's to say - [`SecurityMode`]: off (the default), lsr or rls.
//!
//! - **`schemaPath`** (`schema_path`) - where a row's attribute value is, as `schema.table.column`
//!   steps (`table.column` in the table's schema; the first step may be the column alone): one
//!   column of the table (`[dept]`), or the join hops to one
//!   (`[public.order_items.order_id, public.orders.dept]`). Each step's column leads to the next
//!   step's table, by that table's one-column primary key, and the last step's column - the
//!   **endpoint** - holds the value. A list is that path - the dml one - alone; a map names several
//!   paths of the table: `dml`, and the row's levels by name - `schemaPath: {dml: [dept], sal: [sal],
//!   review: [review_level]}` (`sal` is the security access level; a level may stand alone, no `dml`).
//! - **`alias`** - the name of the user's attribute the value is compared with: signon's resource
//!   kind. It is a name, never a path, and it is defined once: by the table that declares the
//!   endpoint as its own column (a one-step path - the **defining** declaration; left out, the
//!   column's name). A table whose path reaches that entity takes its alias and does not repeat it;
//!   one that says another is refused. A path to an entity nobody defines takes the alias it says,
//!   else the last column's name.
//! - **Actions** are no part of it: the statement decides the action (select, insert, update,
//!   soft_delete, delete), the user's levels the values for it.
//! - **The file's checks** ([`link`]) run under `SecurityMode::Lsr(true)` only - under lsr nothing
//!   in the database tests a path, under rls the database does: every step of a path is a column
//!   of a table defined in the same file, each hop's table has a one-column primary key, a foreign
//!   key the YAML declares on a hop's column points at the next step's table, and the aliases are
//!   unique - an alias names one entity, an entity has one alias (the children of one template
//!   count as one). `Lsr(false)` leaves them out: sge, which reads several files.
//! - **The SQL part** ([`SQL`]): schema `labac`, its key (`labac.key`, never in the YAML: ytserv
//!   writes it), `labac.checked` / `labac.enter` / `labac.enter_session` / `labac.ids` - a token
//!   ytproxy signed with the key and the ids it carries per alias and action - and the registry of
//!   the covered tables (`labac.tables`: the path, the alias, the endpoint). Installed before the
//!   first policy, and again when the database has another version of it ([`SQL_VERSION`]).
//!   signon's `labac::SQL` is this one.
//! - **The deploy** ([`deploy_all`], a table's [`deploy`]) runs after every table of the file
//!   exists, the defining declarations first - by `migrate_plan`, or by a caller that steps through
//!   the tables itself (sge), which also runs [`link`]. The join keys come from the catalog then; nothing of them is stored. In
//!   the database the registry answers what the file cannot: a path's alias is its entity's
//!   registered one, and a defining table whose alias changed carries it to the registered tables
//!   that reach it - their rows, and their policies.
//! - **`SecurityMode::Rls`** - PostgreSQL row-level security on every declared table: ENABLE and FORCE (the owner's views and
//!   functions are filtered too), a policy per action comparing the endpoint with
//!   `labac.ids(alias, action)` - through an EXISTS over the path's joins; an UPDATE needs
//!   `soft_delete` ids where it touches a row the soft-delete column marks, or marks one. The
//!   EXISTS reads the path's tables as the querying role, so their own policies apply too; a path
//!   that would come back to its own table through them (a tree, or two tables reading each other)
//!   is refused - PostgreSQL would recurse: such a schema deploys under lsr.
//! - **`SecurityMode::Lsr`** - registered only: ytproxy rewrites the statements; labac's policies
//!   are taken off where a deploy under rls made them.
//! - **`SecurityMode::Off`** (the default) - the declarations ignored: nothing installed,
//!   registered or changed.
//!
//! Names are schema_guard's own: unquoted, as its other DDL. As for every other object, schema_guard
//! drops nothing a YAML no longer declares.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use serde::Serialize;
use yaml_rust2::Yaml;

use crate::schema::Schema;
use crate::table::Table;
use crate::utils::OrderedHashMap;

/// [security] The SQL part: schema `labac`, the key table, the token check, the ids and the registry
pub const SQL: &str = r#"
CREATE EXTENSION IF NOT EXISTS pgcrypto;
CREATE SCHEMA IF NOT EXISTS labac;
CREATE TABLE IF NOT EXISTS labac.key (id int PRIMARY KEY DEFAULT 1 CHECK (id = 1), secret bytea NOT NULL);
REVOKE ALL ON labac.key FROM PUBLIC;
-- [review 2026-11-21] a token's body when it is one ytproxy signed and not expired, else NULL:
-- the signatures compared as HMACs of themselves, so the compare leaks nothing of the real one
CREATE OR REPLACE FUNCTION labac.checked(token text) RETURNS jsonb
    LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path = pg_catalog, public, pg_temp AS $labac$
DECLARE
    payload text := split_part(token, '.', 1);
    sig     text := split_part(token, '.', 2);
    k       bytea;
    body    jsonb;
BEGIN
    SELECT secret INTO k FROM labac.key WHERE id = 1;
    IF k IS NULL OR payload = '' OR sig = ''
        OR hmac(encode(hmac(payload::bytea, k, 'sha256'), 'hex')::bytea, k, 'sha256') <> hmac(sig::bytea, k, 'sha256') THEN
        RETURN NULL;
    END IF;
    body := convert_from(decode(payload, 'base64'), 'UTF8')::jsonb;
    IF jsonb_typeof(body -> 'exp') IS DISTINCT FROM 'number' OR (body ->> 'exp')::numeric < extract(epoch from now()) THEN
        RETURN NULL;
    END IF;
    -- [review 2026-11-21] a token made for a role is the token of that login only: copied to another
    -- role's session (SET labac.subject needs no grant) it is no subject
    IF body ? 'role' AND body ->> 'role' IS DISTINCT FROM session_user::text THEN
        RETURN NULL;
    END IF;
    RETURN body;
EXCEPTION WHEN OTHERS THEN
    RETURN NULL;
END
$labac$;
REVOKE ALL ON FUNCTION labac.checked(text) FROM PUBLIC;
-- the token checked and held for the transaction - its signed text, which labac.ids checks
-- again: a value a session sets with set_config itself is no subject
CREATE OR REPLACE FUNCTION labac.enter(token text) RETURNS void
    LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, public, pg_temp AS $labac$
BEGIN
    IF labac.checked(token) IS NULL THEN
        RAISE EXCEPTION 'labac: a token ytproxy did not sign, or an expired one' USING ERRCODE = '28000';
    END IF;
    PERFORM set_config('labac.subject', token, true);
END
$labac$;
REVOKE ALL ON FUNCTION labac.enter(text) FROM PUBLIC;
CREATE OR REPLACE FUNCTION labac.ids(kind text, action text) RETURNS text[]
    LANGUAGE sql STABLE SECURITY DEFINER SET search_path = pg_catalog, public, pg_temp AS $labac$
    SELECT coalesce(ARRAY(SELECT jsonb_array_elements_text(
        labac.checked(coalesce(current_setting('labac.subject', true), '')) -> 'ids' -> (kind || ':' || action))), '{}')
$labac$;
-- [levels] the level of a name the entered token holds - the user's security access level (sal) or
-- another named one; none: 0, which reaches no row
CREATE OR REPLACE FUNCTION labac.level(name text) RETURNS int
    LANGUAGE sql STABLE SECURITY DEFINER SET search_path = pg_catalog, public, pg_temp AS $labac$
    SELECT coalesce((labac.checked(coalesce(current_setting('labac.subject', true), '')) -> 'levels' ->> name)::int, 0)
$labac$;
-- [levels] whether the token gives the action on every resource of a kind, whatever its id: said
-- apart from the ids, never as one of them
CREATE OR REPLACE FUNCTION labac.any_id(kind text, action text) RETURNS boolean
    LANGUAGE sql STABLE SECURITY DEFINER SET search_path = pg_catalog, public, pg_temp AS $labac$
    SELECT coalesce(labac.checked(coalesce(current_setting('labac.subject', true), '')) -> 'any' ? (kind || ':' || action), false)
$labac$;
-- [labac] the token held for the session: ytproxy enters it as it hands a pooled connection to a
-- session, and the connection is reset (DISCARD ALL) before another session gets it
CREATE OR REPLACE FUNCTION labac.enter_session(token text) RETURNS void
    LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, public, pg_temp AS $labac$
BEGIN
    IF labac.checked(token) IS NULL THEN
        RAISE EXCEPTION 'labac: a token ytproxy did not sign, or an expired one' USING ERRCODE = '28000';
    END IF;
    PERFORM set_config('labac.subject', token, false);
END
$labac$;
REVOKE ALL ON FUNCTION labac.enter_session(text) FROM PUBLIC;
-- [labac] the tables labac covers, as schema_guard declared them: what ytproxy and ytserv read
-- (column_name: the schemaPath, its steps joined by |; kind: the alias)
CREATE TABLE IF NOT EXISTS labac.tables (
    schema_name text NOT NULL,
    table_name  text NOT NULL,
    column_name text NOT NULL,
    kind        text NOT NULL,
    soft_delete text,
    rls         boolean NOT NULL DEFAULT false,
    PRIMARY KEY (schema_name, table_name)
);
-- [security] where the path ends, `schema.table.column`: the entity an alias names. Tables whose paths
-- reach one entity share its alias; the entity's own table (a one-step path) defines it
ALTER TABLE labac.tables ADD COLUMN IF NOT EXISTS endpoint text;
-- [levels] the levels the table declares: a JSON map, name -> its path's steps joined by |. A table
-- that declares only levels has an empty column_name and kind
ALTER TABLE labac.tables ADD COLUMN IF NOT EXISTS levels text;
REVOKE ALL ON labac.tables FROM PUBLIC;
GRANT SELECT ON labac.tables TO PUBLIC;
-- a policy calls labac.ids as the querying role: the schema is open to every role, its key is not
GRANT USAGE ON SCHEMA labac TO PUBLIC;
-- [review 2026-11-21] the version of this SQL: a database with an older one gets it again
COMMENT ON FUNCTION labac.checked(text) IS 'labac:sql:5';
"#;

/// the version of the declaration a policy's comment says: a change of how policies are made
/// makes them again
const SIGNATURE: &str = "labac:v6";

/// [review 2026-11-21] the version of [`SQL`] its `labac.checked` carries as its comment: an
/// older one is installed again
pub const SQL_VERSION: &str = "labac:sql:5";

/// the actions a policy is made for, and the policy's name
const POLICIES: &[(&str, &str)] = &[("select", "labac_select"), ("insert", "labac_insert"), ("update", "labac_update"),
    ("delete", "labac_delete")];

/// [security] How a deploy takes the tables' declarations ([`crate::MigrationOptions::security`]):
/// one mode for the deploy - a table does not say its own
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SecurityMode {
    /// the declarations ignored: nothing installed, registered or changed - the default
    #[default]
    Off,
    /// registered for ytproxy's rewrite only: no policy, labac's taken off where they were. `true` -
    /// the file's traversal is checked ([`link`]): nothing in the database tests an lsr path, so its
    /// tables are expected in the same file and its aliases unique in it; `false` - not checked (a
    /// schema spread over several files)
    Lsr(bool),
    /// row-level security policies on every declared table: the database tests the paths
    Rls,
}

impl SecurityMode {
    /// the file's checks run: under `Lsr(true)` only
    pub fn traversal(self) -> bool {
        self == SecurityMode::Lsr(true)
    }
}

impl std::str::FromStr for SecurityMode {
    type Err = std::convert::Infallible;

    /// `lsr` (its traversal checked) or `rls`, any case; anything else is the default, `Off`
    fn from_str(s: &str) -> Result<SecurityMode, std::convert::Infallible> {
        Ok(match s.trim().to_lowercase().as_str() {
            "lsr" => SecurityMode::Lsr(true),
            "rls" => SecurityMode::Rls,
            _ => SecurityMode::Off,
        })
    }
}

/// [security] One step of a path as a deploy resolves it: a column of a table, and the key of that
/// table the step before joins on (empty for the first step: the table declared). Never stored.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Step {
    pub schema: String,
    pub table: String,
    pub column: String,
    pub key: String,
}

/// [security] A table's declaration
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Security {
    /// the table's paths, by name - each `schema.table.column` steps: one column of the table, or the
    /// join hops to one. [`DML`] (`dml`) is the path to the attribute's value, what the user's values
    /// of the alias are compared with; every other name is a level's - `sal`, the security access
    /// level, or any name of the schema's own: the user's level of that name must be the same as or
    /// higher than the value at the path's end. In the YAML `schemaPath`: this map, or - the dml
    /// path alone - a list
    pub schema_path: BTreeMap<String, Vec<String>>,
    /// the name of the user's attribute (signon's resource kind); `None` - the entity's own: its
    /// defining table's, else the last column's name
    pub alias: Option<String>,
    /// the column a soft delete sets: an UPDATE that sets it needs the soft_delete level
    pub soft_delete: Option<String>,
    /// row-level security policies. Not a table's to say: the deploy's mode sets it
    /// ([`Security::under`]); in a declaration read back from the registry, what the database has
    pub rls: bool,
}

/// [levels] the name of the dml path in `schemaPath` - `schemaPath: {dml: [..], sal: [..]}`; every
/// other name of the map is a level's
pub const DML: &str = "dml";

/// the dml path of a map of named paths; none, empty
fn dml_of<T>(paths: &BTreeMap<String, Vec<T>>) -> &[T] {
    paths.get(DML).map(Vec::as_slice).unwrap_or(&[])
}

/// the levels of a map of named paths: every path but the dml one, by name
fn levels_of<T>(paths: &BTreeMap<String, Vec<T>>) -> impl Iterator<Item = (&String, &Vec<T>)> {
    paths.iter().filter(|(n, _)| n.as_str() != DML)
}

/// a map of named paths of the dml one alone; none for an empty path
fn dml_alone<T>(path: Vec<T>) -> BTreeMap<String, Vec<T>> {
    if path.is_empty() { BTreeMap::new() } else { [(DML.to_string(), path)].into_iter().collect() }
}

/// As a YAML says it: `schemaPath` the dml path alone as a list, or - with levels - the map of the
/// named paths, `dml` first; `alias` and `softDelete` where said. `rls` is the deploy's, never written
impl Serialize for Security {
    fn serialize<S: serde::Serializer>(&self, ser: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;
        struct Paths<'a>(&'a Security);
        impl Serialize for Paths<'_> {
            fn serialize<S: serde::Serializer>(&self, ser: S) -> Result<S::Ok, S::Error> {
                let d = self.0;
                if d.levels().next().is_none() {
                    return d.dml().serialize(ser);
                }
                let mut m = ser.serialize_map(Some(d.schema_path.len()))?;
                if d.has_dml() {
                    m.serialize_entry(DML, d.dml())?;
                }
                for (name, path) in d.levels() {
                    m.serialize_entry(name, path)?;
                }
                m.end()
            }
        }
        let mut m = ser.serialize_map(Some(1 + usize::from(self.alias.is_some()) + usize::from(self.soft_delete.is_some())))?;
        m.serialize_entry("schemaPath", &Paths(self))?;
        if let Some(a) = &self.alias {
            m.serialize_entry("alias", a)?;
        }
        if let Some(d) = &self.soft_delete {
            m.serialize_entry("softDelete", d)?;
        }
        m.end()
    }
}

/// [levels] the name of the level a declaration does not name: the security access level
pub const SAL: &str = "sal";

/// [security] A declaration as a deploy applies it: its paths with the join keys, and its alias
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Resolved {
    /// the table's paths, by name, as [`Security::schema_path`] has them: [`DML`] the dml one - none
    /// for a table that declares only levels - and each level's
    pub paths: BTreeMap<String, Vec<Step>>,
    pub alias: String,
}

impl Resolved {
    /// the dml path; empty for a table that declares only levels
    pub fn dml(&self) -> &[Step] {
        dml_of(&self.paths)
    }

    /// [levels] each level's path, by name
    pub fn levels(&self) -> impl Iterator<Item = (&String, &Vec<Step>)> {
        levels_of(&self.paths)
    }

    /// where the dml path ends: `schema.table.column`
    pub fn endpoint(&self) -> String {
        self.dml().last().map(|s| format!("{}.{}.{}", s.schema, s.table, s.column)).unwrap_or_default()
    }
}

/// [security] A row of the registry (`labac.tables`)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Registered {
    pub schema: String,
    pub table: String,
    /// its paths, by name, as [`Security::schema_path`]: with a level the dml path, and the alias, may be none
    pub schema_path: BTreeMap<String, Vec<String>>,
    pub alias: String,
    pub soft_delete: Option<String>,
    pub rls: bool,
    /// where its dml path ends; `None` in a row an older schema_guard wrote
    pub endpoint: Option<String>,
}

impl Registered {
    /// the declaration it is of
    pub fn security(&self) -> Security {
        Security { schema_path: self.schema_path.clone(), alias: Some(self.alias.clone()).filter(|a| !a.is_empty()),
            soft_delete: self.soft_delete.clone(), rls: self.rls }
    }

    /// the dml path; empty for a table of levels alone
    pub fn dml(&self) -> &[String] {
        dml_of(&self.schema_path)
    }

    /// its table declares the entity itself: a one-step dml path
    pub fn defining(&self) -> bool {
        self.dml().len() == 1
    }

    /// [review 2026-11-22] the entity its path ends at, `schema.table.column`: as registered, else -
    /// a row a schema_guard before the `endpoint` column wrote - as its path says
    pub fn entity(&self) -> Option<String> {
        self.endpoint.clone().filter(|e| !e.is_empty()).or_else(|| self.security().path_of(&self.schema, &self.table).ok()
            .and_then(|p| p.last().map(|(s, t, c)| format!("{s}.{t}.{c}"))))
    }
}

/// [security] What a database has of a table's labac
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Have {
    /// the SQL part is there
    pub installed: bool,
    /// the registry's row of the table
    pub registered: Option<Registered>,
    /// row-level security on, and forced
    pub enabled: bool,
    pub forced: bool,
    /// the labac policies, by name, and their comments
    pub policies: HashMap<String, String>,
    /// [review 2026-11-21] the table's other policies, not labac's: row-level security stays on for them
    pub others: usize,
    /// the roles of the table's grants that may not run labac.enter yet
    pub enter_missing: Vec<String>,
}

/// a plain name: letters, digits and _
fn name_ok(n: &str) -> bool {
    !n.is_empty() && n.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// a path as a YAML says it: a list of steps, each a string, or one step as a string; nothing, none
/// [review 2026-11-22] a step that is not a string is refused, not dropped: a path one step
/// shorter would be another path
fn yaml_path(path: &Yaml, key: &str) -> Result<Vec<String>, String> {
    let not_steps = || format!("security {key} is a list of schema.table.column steps, each a string");
    Ok(match path {
        Yaml::Array(a) => {
            let mut steps = Vec::new();
            for s in a {
                // [review 2026-11-22] as it is compared and registered: without the spaces around it
                steps.push(s.as_str().map(|s| s.trim().to_string()).ok_or_else(not_steps)?);
            }
            steps
        }
        Yaml::String(s) => vec![s.trim().to_string()],
        p if p.is_null() || p.is_badvalue() => Vec::new(),
        _ => return Err(not_steps()),
    })
}

/// a step as written: schema, table, column - the missing ones `None`
type Written = (Option<String>, Option<String>, String);

/// [security] A path's steps as written: the first may be the column alone, a later one names its table
fn steps(parts: &[String]) -> Result<Vec<Written>, String> {
    let text = parts.join(", ");
    if parts.is_empty() {
        return Err("security needs a schemaPath".to_string());
    }
    let mut out = Vec::new();
    for (i, p) in parts.iter().map(|p| p.trim()).enumerate() {
        let names: Vec<&str> = p.split('.').collect();
        if !names.iter().all(|n| name_ok(n)) {
            return Err(format!("security path [{text}]: {p} is not schema.table.column of plain names"));
        }
        out.push(match names.as_slice() {
            [c] if i == 0 => (None, None, c.to_string()),
            [t, c] => (None, Some(t.to_string()), c.to_string()),
            [s, t, c] => (Some(s.to_string()), Some(t.to_string()), c.to_string()),
            _ => return Err(format!("security path [{text}]: step {} is not schema.table.column", i + 1)),
        });
    }
    Ok(out)
}

/// a step's column: its last name
fn column_of(step: &str) -> &str {
    step.rsplit('.').next().unwrap_or(step).trim()
}

impl Security {
    /// [security] The declaration as a deploy of `mode` takes it: `None` - not at all (`Off`)
    pub fn under(&self, mode: SecurityMode) -> Option<Security> {
        match mode {
            SecurityMode::Off => None,
            SecurityMode::Lsr(_) => Some(Security { rls: false, ..self.clone() }),
            SecurityMode::Rls => Some(Security { rls: true, ..self.clone() }),
        }
    }

    /// the declaration under a table's `security`; `None` none. `schemaPath` (`schema_path` read as
    /// well) is the dml path - a list of steps, or one step as a string - or a map of named paths:
    /// `dml`, and each level's by its name (`sal`, the security access level, or any other). `kind`,
    /// the alias's name before, and `level` / `levels`, keys of their own before, are refused by
    /// name; an alias is a name, not a path.
    pub fn read(input: &Yaml) -> Result<Option<Security>, String> {
        if input.is_null() || input.is_badvalue() {
            return Ok(None);
        }
        if !input["kind"].is_badvalue() && !input["kind"].is_null() {
            return Err("security `kind` is `alias` now: the name of the user's attribute, written once, on the table that defines it".to_string());
        }
        // rls was a table's key: refused by that name, not dropped - a table that says `rls: true`
        // would be left without its policies under another mode, and nobody told
        if !input["rls"].is_badvalue() && !input["rls"].is_null() {
            return Err("security `rls` is the deploy's mode now, not a table's key: SecurityMode::Rls (sge: security=rls)".to_string());
        }
        // [review 2026-11-22] one key, two spellings: both said, one would be passed by - and a level
        // or the dml path with it
        let said = |k: &str| !input[k].is_badvalue();
        if said("schemaPath") && said("schema_path") {
            return Err("security schemaPath and schema_path are one key: say it once".to_string());
        }
        let path = if said("schemaPath") { &input["schemaPath"] } else { &input["schema_path"] };
        // [levels] keys of their own before: refused by name, not dropped - a table would lose its
        // level, and with it every row be open to a user of its department
        for old in ["level", "levels"] {
            if !input[old].is_badvalue() && !input[old].is_null() {
                return Err(format!("security `{old}` is a name of schemaPath now - schemaPath: {{{DML}: [..], {SAL}: [..]}}: the dml path and each level's, by name"));
            }
        }
        // a map names each path - `dml`, and the levels; a list, or one step, is the dml path alone
        let schema_path: BTreeMap<String, Vec<String>> = match path {
            Yaml::Hash(h) => {
                if h.is_empty() {
                    return Err(format!("security schemaPath as a map names its paths: {DML}, and each level's ({SAL}, ..)"));
                }
                let mut named = BTreeMap::new();
                for (k, v) in h {
                    // [review 2026-11-22] a name is taken as it is written: trimmed, ' sal' would be sal
                    // again and one of the two paths lost - as written it is no plain name, and refused
                    let name = k.as_str()
                        .ok_or_else(|| format!("security schemaPath as a map: a path's name - {DML}, or a level's - then its steps"))?;
                    let steps = yaml_path(v, &format!("schemaPath {name}"))?;
                    // a name with no path is no declaration of it: left out, not said empty
                    if steps.is_empty() {
                        return Err(format!("security schemaPath {name} has no step: leave the name out, or say its path"));
                    }
                    named.insert(name.to_string(), steps);
                }
                named
            }
            p => dml_alone(yaml_path(p, "schemaPath")?),
        };
        let alias = input["alias"].as_str().map(|s| s.trim().to_string()).filter(|s| !s.is_empty());
        if alias.as_deref().is_some_and(|a| a.contains('.') || a.contains('|')) {
            return Err("security alias is a name, not a path: the path is schemaPath".to_string());
        }
        Ok(Some(Security {
            schema_path,
            alias,
            soft_delete: input["softDelete"].as_str().map(|s| s.to_string()).filter(|s| !s.is_empty()),
            rls: false,
        }))
    }

    /// a declaration of the dml path alone - `schemaPath: [..]`
    pub fn of_dml(path: Vec<String>) -> Security {
        Security { schema_path: dml_alone(path), ..Default::default() }
    }

    /// the dml path - `schema_path[dml]`: where the value the user's values of the alias are compared
    /// with is. Empty for a declaration of levels alone
    pub fn dml(&self) -> &[String] {
        dml_of(&self.schema_path)
    }

    /// [levels] the levels: every path of `schema_path` but the dml one, by name
    pub fn levels(&self) -> impl Iterator<Item = (&String, &Vec<String>)> {
        levels_of(&self.schema_path)
    }

    /// [levels] the declaration has a dml path. Without one it declares levels only
    pub fn has_dml(&self) -> bool {
        !self.dml().is_empty()
    }

    /// its table declares the entity itself: a one-step dml path
    pub fn defining(&self) -> bool {
        self.dml().len() == 1
    }

    /// [review 2026-11-22] a template's declaration as its child takes it: a step that names the
    /// template's table - which is no table of the database - names the child's, in the child's schema
    pub fn of_child(&self, template: &str, child: &str) -> Security {
        let renamed = |path: &[String]| -> Vec<String> {
            path.iter().map(|step| {
                let names: Vec<&str> = step.trim().split('.').collect();
                match names.as_slice() {
                    [t, c] | [_, t, c] if *t == template => format!("{child}.{c}"),
                    _ => step.clone(),
                }
            }).collect()
        };
        Security { schema_path: self.schema_path.iter().map(|(n, p)| (n.clone(), renamed(p))).collect(), ..self.clone() }
    }

    /// the alias it says, else its last column's name
    pub fn alias_or_column(&self) -> String {
        match &self.alias {
            Some(a) => a.clone(),
            None => self.dml().last().map(|s| column_of(s).to_string()).unwrap_or_default(),
        }
    }

    /// what is wrong with it on table `table` of `columns` (empty: not known); `None` nothing
    pub fn problem(&self, table: &str, columns: &[String]) -> Option<String> {
        if self.schema_path.values().all(Vec::is_empty) {
            return Some(format!("security needs a schemaPath: the dml path, or a map with {DML} or a level's"));
        }
        if !self.has_dml() && (self.alias.is_some() || self.soft_delete.is_some()) {
            return Some("security alias and softDelete are of the dml path: a declaration of levels alone has neither".to_string());
        }
        if self.alias.as_deref().is_some_and(|a| a.contains('.') || a.contains('|')) {
            return Some("security alias is a name, not a path: the path is schemaPath".to_string());
        }
        if self.soft_delete.as_deref().is_some_and(|s| !name_ok(s)) {
            return Some("security softDelete is a plain name: letters, digits and _".to_string());
        }
        if let Some((n, _)) = self.levels().find(|(n, _)| !name_ok(n)) {
            return Some(format!("security level {n}: a level's name is letters, digits and _"));
        }
        // [review 2026-11-22] in any case: `DML` beside `dml` would be a level nobody has
        if let Some((n, _)) = self.levels().find(|(n, _)| n.eq_ignore_ascii_case(DML)) {
            return Some(format!("security level {n}: {DML} names the dml path, never a level"));
        }
        // each path - the dml one and every level's - starts at the table declared: its first column is the table's
        let paths = self.schema_path.iter().filter(|(n, p)| n.as_str() != DML || !p.is_empty())
            .map(|(n, p)| (if n == DML { "schemaPath".to_string() } else { format!("level {n}") }, p));
        for (what, path) in paths {
            let st = match steps(path) {
                Ok(s) => s,
                Err(e) => return Some(if what == "schemaPath" { e } else { format!("{e} ({what})") }),
            };
            if let Some((_, Some(t), _)) = st.first()
                && t != table {
                return Some(format!("security path [{}]: its first step is a column of {table}, not of {t}", path.join(", ")));
            }
            if !columns.is_empty()
                && let Some(c) = st.first().map(|s| &s.2).filter(|c| !columns.iter().any(|x| x == *c)) {
                return Some(format!("security column {c} is not a column of the table"));
            }
        }
        if !columns.is_empty()
            && let Some(c) = self.soft_delete.as_ref().filter(|c| !columns.iter().any(|x| x == *c)) {
            return Some(format!("security column {c} is not a column of the table"));
        }
        None
    }

    /// [security] The path as `(schema, table, column)` steps for a table `schema.table`: a step that
    /// names no schema is of the table's
    pub fn path_of(&self, schema: &str, table: &str) -> Result<Vec<(String, String, String)>, String> {
        columns_of(self.dml(), schema, table)
    }

    /// [levels] each level's path as `(schema, table, column)` steps, by name
    pub fn level_paths(&self, schema: &str, table: &str) -> Result<BTreeMap<String, Vec<Column>>, String> {
        self.levels().map(|(n, p)| columns_of(p, schema, table).map(|c| (n.clone(), c))).collect()
    }

    /// [security] The dml path resolved for `schema.table`: each step's table, and the primary key the
    /// step before joins on, from `dbc` (the database's tables, and those this run made). Empty for a
    /// declaration of levels alone.
    pub fn resolve(&self, schema: &str, table: &str, dbc: &crate::loader::InfoSchemaType) -> Result<Vec<Step>, String> {
        if !self.has_dml() {
            return Ok(Vec::new());
        }
        resolve_path(self.dml(), schema, table, dbc)
    }

    /// [levels] each level's path resolved as [`Security::resolve`] does the dml one
    pub fn resolve_levels(&self, schema: &str, table: &str, dbc: &crate::loader::InfoSchemaType) -> Result<BTreeMap<String, Vec<Step>>, String> {
        self.levels().map(|(n, p)| resolve_path(p, schema, table, dbc).map(|s| (n.clone(), s))).collect()
    }

    /// every path resolved, by name: the dml one ([`Security::resolve`]) and each level's
    pub fn resolve_paths(&self, schema: &str, table: &str, dbc: &crate::loader::InfoSchemaType) -> Result<BTreeMap<String, Vec<Step>>, String> {
        let mut paths = self.resolve_levels(schema, table, dbc)?;
        let dml = self.resolve(schema, table, dbc)?;
        if !dml.is_empty() {
            paths.insert(DML.to_string(), dml);
        }
        Ok(paths)
    }
}

/// a path's steps as `(schema, table, column)` for a table `schema.table`
fn columns_of(path: &[String], schema: &str, table: &str) -> Result<Vec<(String, String, String)>, String> {
    Ok(steps(path)?.into_iter().map(|(s, t, c)| (s.unwrap_or_else(|| schema.to_string()),
        t.unwrap_or_else(|| table.to_string()), c)).collect())
}

/// a path resolved for `schema.table`: its steps with the keys they are joined on
fn resolve_path(path: &[String], schema: &str, table: &str, dbc: &crate::loader::InfoSchemaType) -> Result<Vec<Step>, String> {
    let text = path.join(", ");
    let mut out = Vec::new();
    for (i, (s, t, c)) in columns_of(path, schema, table)?.into_iter().enumerate() {
        let key = if i == 0 {
            // [review 2026-11-21] a path starts at the table declared, schema and all
            if s != schema || t != table {
                return Err(format!("security path of {schema}.{table}: its first step is {s}.{t}"));
            }
            String::new()
        } else {
            let pk = dbc.get(&s).and_then(|ts| ts.get(&t)).and_then(|pt| pt.primary_key.as_ref())
                .ok_or_else(|| format!("security path [{text}]: {s}.{t} is not known, or has no primary key, when {schema}.{table} is deployed"))?;
            match pk.columns.as_slice() {
                [k] => k.clone(),
                _ => return Err(format!("security path [{text}]: {s}.{t}'s primary key is not one column")),
            }
        };
        out.push(Step { schema: s, table: t, column: c, key });
    }
    Ok(out)
}

/// the comment each policy of a declaration carries - its path with the keys, its alias, the
/// soft-delete column's kind of mark and its levels: any of them changed makes the policies again
fn signature(r: &Resolved, soft_delete: Option<&str>, soft_bool: bool) -> String {
    let path = |steps: &[Step]| steps.iter().map(|x| format!("{}.{}.{}>{}", x.schema, x.table, x.column, x.key)).collect::<Vec<_>>().join("|");
    let levels = r.levels().map(|(n, st)| format!("{n}={}", path(st))).collect::<Vec<_>>().join(";");
    format!("{SIGNATURE}:{}:{}:{}:{}:{levels}", path(r.dml()), r.alias, soft_delete.unwrap_or(""),
        if soft_delete.is_some() && soft_bool { "bool" } else { "" })
}

/// a path's joins: the FROM chain, and its join condition to the row's own column
type Joined = (String, String);

/// a path over its joins: the FROM chain and its join condition to `table`'s first column, and the
/// last step's column as the chain names it. No join: `(None, table.column)`
fn chain_of(table: &str, steps: &[Step]) -> (Option<Joined>, String) {
    let mut last = format!("{}.{}", table, steps.first().map(|s| s.column.as_str()).unwrap_or(""));
    let mut chain = String::new();
    let mut on = String::new();
    for (i, s) in steps.iter().enumerate().skip(1) {
        let j = format!("j{i}");
        if chain.is_empty() {
            chain = format!("{}.{} {j}", s.schema, s.table);
            on = format!("{j}.{} = {last}", s.key);
        } else {
            chain.push_str(&format!(" JOIN {}.{} {j} ON {j}.{} = {last}", s.schema, s.table, s.key));
        }
        last = format!("{j}.{}", s.column);
    }
    ((!chain.is_empty()).then_some((chain, on)), last)
}

/// [security] the dml condition a row of `table` meets for `action`: its endpoint one of the alias's
/// `labac.ids` - or ([levels]) the action given on every resource of the alias, whatever the row's
/// value (`labac.any_id`: an entry on `*`). Over the path's joins an EXISTS, the choice inside it.
/// [review 2026-11-22] A hidden parent denies - the user of every resource too: the path's rows are
/// read whatever the compare, so the policies of the tables it goes through decide with it (their
/// levels), and a row whose path reaches no row is reached by nobody
fn condition(table: &str, steps: &[Step], alias: &str, action: &str) -> String {
    let (chain, last) = chain_of(table, steps);
    let reached = format!("(labac.any_id('{}', '{action}') OR ({last})::text = ANY (labac.ids('{}', '{action}')))", lit(alias), lit(alias));
    match chain {
        None => reached,
        Some((chain, on)) => format!("EXISTS (SELECT 1 FROM {chain} WHERE {on} AND {reached})"),
    }
}

/// [levels] the condition a row of `table` meets for the level `name`: the user's level of that
/// name is 1 or more and no lower than the row's - a NULL level counts as 0. Over the path's joins
/// an EXISTS with the compare inside: a row whose path reaches no row is reached by nobody
fn level_condition(table: &str, name: &str, steps: &[Step]) -> String {
    let mine = format!("labac.level('{}')", lit(name));
    let (chain, last) = chain_of(table, steps);
    let reached = match chain {
        None => format!("coalesce({last}, 0) <= {mine}"),
        Some((chain, on)) => format!("EXISTS (SELECT 1 FROM {chain} WHERE {on} AND coalesce({last}, 0) <= {mine})"),
    };
    format!("({mine} >= 1 AND {reached})")
}

/// the dml condition (when the declaration has one) and every level's, all of them
fn whole(dml: Option<String>, table: &str, r: &Resolved) -> String {
    dml.into_iter().chain(r.levels().map(|(n, st)| level_condition(table, n, st))).collect::<Vec<_>>().join(" AND ")
}

/// [security] the condition a row of `table` meets for `action`: the dml one and the levels
fn row_condition(table: &str, r: &Resolved, action: &str) -> String {
    whole((!r.dml().is_empty()).then(|| condition(table, r.dml(), &r.alias, action)), table, r)
}

/// the condition an UPDATE needs on the row before and after: its ids, and the soft_delete ids
/// where the row is marked - and the levels
fn update_condition(table: &str, r: &Resolved, soft_delete: Option<&str>, soft_bool: bool) -> String {
    let dml = (!r.dml().is_empty()).then(|| match soft_delete {
        None => condition(table, r.dml(), &r.alias, "update"),
        Some(sd) => {
            let unmarked = if soft_bool { format!("{table}.{sd} IS NOT TRUE") } else { format!("{table}.{sd} IS NULL") };
            format!("{} AND ({unmarked} OR {})", condition(table, r.dml(), &r.alias, "update"), condition(table, r.dml(), &r.alias, "soft_delete"))
        }
    });
    whole(dml, table, r)
}

/// the policies of `schema.table` made (again): all of them when `have` is `None`, else those
/// whose comment is not this declaration's
fn policies_sql(schema: &str, table: &str, r: &Resolved, soft_delete: Option<&str>, soft_bool: bool, have: Option<&HashMap<String, String>>) -> String {
    let t = format!("{schema}.{table}");
    let sig = signature(r, soft_delete, soft_bool);
    let mut sql = String::new();
    for (action, name) in POLICIES {
        if have.is_some_and(|h| h.get(*name) == Some(&sig)) {
            continue;
        }
        let body = match *action {
            "select" => format!("FOR SELECT USING ({})", row_condition(table, r, "select")),
            "insert" => format!("FOR INSERT WITH CHECK ({})", row_condition(table, r, "insert")),
            "update" => {
                let c = update_condition(table, r, soft_delete, soft_bool);
                format!("FOR UPDATE USING ({c}) WITH CHECK ({c})")
            }
            _ => format!("FOR DELETE USING ({})", row_condition(table, r, "delete")),
        };
        sql.push_str(&format!("DROP POLICY IF EXISTS {name} ON {t};\nCREATE POLICY {name} ON {t} {body};\n\
            COMMENT ON POLICY {name} ON {t} IS '{}';\n", lit(&sig)));
    }
    sql
}

impl Security {
    /// [security] The SQL that makes `schema.table` so, given what the database `have`s: empty when
    /// it already is. `r` its path and alias resolved; `soft_bool` the soft-delete column is a boolean.
    pub fn sql(&self, schema: &str, table: &str, r: &Resolved, have: &Have, soft_bool: bool) -> String {
        let mut sql = String::new();
        if !have.installed {
            sql.push_str(SQL);
            sql.push('\n');
        }
        let endpoint = r.endpoint();
        // [levels] a table of levels alone has no path, alias or endpoint: empty, with its levels
        let same = have.registered.as_ref().is_some_and(|h| h.schema_path == self.schema_path && h.alias == r.alias
            && h.soft_delete == self.soft_delete && h.rls == self.rls && h.endpoint.clone().unwrap_or_default() == endpoint);
        if !same {
            sql.push_str(&format!("INSERT INTO labac.tables (schema_name, table_name, column_name, kind, soft_delete, rls, endpoint, levels) \
                VALUES ('{}', '{}', '{}', '{}', {}, {}, '{}', {}) ON CONFLICT (schema_name, table_name) DO UPDATE SET \
                column_name = EXCLUDED.column_name, kind = EXCLUDED.kind, soft_delete = EXCLUDED.soft_delete, rls = EXCLUDED.rls, \
                endpoint = EXCLUDED.endpoint, levels = EXCLUDED.levels;\n",
                lit(schema), lit(table), lit(&self.dml().join("|")), lit(&r.alias),
                self.soft_delete.as_ref().map(|s| format!("'{}'", lit(s))).unwrap_or_else(|| "NULL".into()), self.rls, lit(&endpoint),
                levels_text(self.levels()).map(|l| format!("'{}'", lit(&l))).unwrap_or_else(|| "NULL".into())));
        }
        for role in &have.enter_missing {
            sql.push_str(&format!("GRANT EXECUTE ON FUNCTION labac.enter(text), labac.enter_session(text) TO {role};\n"));
        }
        let t = format!("{schema}.{table}");
        if !self.rls {
            // [review 2026-11-21] not rls (the lsr mode): labac's policies go, and row-level security
            // with them unless the table has policies of its own
            for name in POLICIES.iter().map(|(_, n)| *n).filter(|n| have.policies.contains_key(*n)) {
                sql.push_str(&format!("DROP POLICY IF EXISTS {name} ON {t};\n"));
            }
            if !have.policies.is_empty() && have.forced {
                sql.push_str(&format!("ALTER TABLE {t} NO FORCE ROW LEVEL SECURITY;\n"));
            }
            if !have.policies.is_empty() && have.enabled && have.others == 0 {
                sql.push_str(&format!("ALTER TABLE {t} DISABLE ROW LEVEL SECURITY;\n"));
            }
            return with_strings_on(sql);
        }
        if !have.enabled {
            sql.push_str(&format!("ALTER TABLE {t} ENABLE ROW LEVEL SECURITY;\n"));
        }
        if !have.forced {
            sql.push_str(&format!("ALTER TABLE {t} FORCE ROW LEVEL SECURITY;\n"));
        }
        sql.push_str(&policies_sql(schema, table, r, self.soft_delete.as_deref(), soft_bool, Some(&have.policies)));
        with_strings_on(sql)
    }

    /// [security] `data` rows with FORCE lifted for them: the table's owner puts its own rows
    pub fn around_data(&self, schema: &str, table: &str, data: String) -> String {
        if !self.rls {
            return data;
        }
        force_lifted(schema, table, data)
    }
}

/// [security] `data` rows of a table whose row-level security is forced - by this deploy, or
/// ([review 2026-11-22]) by the one before, when this one takes it off after the rows - with FORCE
/// lifted for them
pub fn force_lifted(schema: &str, table: &str, data: String) -> String {
    if data.is_empty() {
        return data;
    }
    format!("ALTER TABLE {schema}.{table} NO FORCE ROW LEVEL SECURITY;\n{data}ALTER TABLE {schema}.{table} FORCE ROW LEVEL SECURITY;\n")
}

/// [security] A registry row read back: `to_jsonb` of a `labac.tables` row. `None` for one that does
/// not read - a kind that is a path, as a schema_guard before the alias wrote for a kind read per row
pub fn from_registry(row: &serde_json::Value) -> Option<Registered> {
    let s = |k: &str| row.get(k).and_then(|v| v.as_str()).map(str::to_string);
    let alias = s("kind")?;
    if alias.contains('.') || alias.contains('|') {
        return None;
    }
    // [review 2026-11-22] a row's names go into DDL (a defining table's alias carried to it): one
    // that is not a plain name is not read
    let (schema, table, soft_delete) = (s("schema_name")?, s("table_name")?, s("soft_delete"));
    if !name_ok(&schema) || !name_ok(&table) || soft_delete.as_deref().is_some_and(|x| !name_ok(x)) {
        return None;
    }
    // the registry keeps the dml path in the column a schema_guard before the levels wrote
    // (`column_name`), each level's in `levels`: here they are the one map of named paths again
    let dml: Vec<String> = s("column_name")?.split('|').map(|x| x.trim().to_string()).filter(|x| !x.is_empty()).collect();
    // [levels] the levels: a map of name to path; one that does not read - a name or a step that is
    // no plain name - makes the row unread, as the rest of it would
    let mut schema_path: BTreeMap<String, Vec<String>> = match s("levels").filter(|l| !l.trim().is_empty()) {
        None => BTreeMap::new(),
        Some(text) => serde_json::from_str::<BTreeMap<String, String>>(&text).ok()?.into_iter()
            .map(|(n, p)| (n, p.split('|').map(|x| x.trim().to_string()).filter(|x| !x.is_empty()).collect::<Vec<_>>())).collect(),
    };
    if schema_path.iter().any(|(n, p)| !name_ok(n) || n.eq_ignore_ascii_case(DML) || steps(p).is_err()) {
        return None;
    }
    // [review 2026-11-22] an alias or a path that is empty names nothing - but for a table of levels
    // alone, which has neither
    let has_dml = !alias.trim().is_empty() && !dml.is_empty();
    if !has_dml && (schema_path.is_empty() || !alias.trim().is_empty() || !dml.is_empty()) {
        return None;
    }
    if has_dml {
        schema_path.insert(DML.to_string(), dml);
    }
    Some(Registered { schema, table, schema_path, alias: alias.trim().to_string(),
        soft_delete, rls: row.get("rls").and_then(|v| v.as_bool()).unwrap_or(false), endpoint: s("endpoint").filter(|e| !e.is_empty()) })
}

/// [levels] the levels as the registry holds them: a JSON map, name -> the path's steps joined by |; none, `None`
fn levels_text<'a>(levels: impl Iterator<Item = (&'a String, &'a Vec<String>)>) -> Option<String> {
    let levels: BTreeMap<String, String> = levels.map(|(n, p)| (n.clone(), p.join("|"))).collect();
    if levels.is_empty() {
        return None;
    }
    serde_json::to_string(&levels).ok()
}

/// [review 2026-11-21] `sql` with standard_conforming_strings on for its transaction: its literals
/// double their quotes and take a backslash as it is
fn with_strings_on(sql: String) -> String {
    if sql.is_empty() {
        return sql;
    }
    format!("SET LOCAL standard_conforming_strings = on;\n{sql}")
}

/// a literal's text with its quotes doubled
fn lit(s: &str) -> String {
    s.replace('\'', "''")
}

// ---- the file: one YAML's declarations, linked and checked ----------------------------------------

/// the declaration of each table of a file that has one, by (schema, table): templates left out
pub type Declared = BTreeMap<(String, String), Security>;

/// [security] The declarations of a parsed file as a deploy of `mode` takes them
pub fn declared(schemas: &OrderedHashMap<Schema>, mode: SecurityMode) -> Declared {
    let mut out = Declared::new();
    for s in &schemas.list {
        for t in s.tables.list.iter().filter(|t| !t.is_template()) {
            if let Some(d) = t.security.as_ref().and_then(|d| d.under(mode)) {
                out.insert((s.schema_name.clone(), t.table_name.clone()), d);
            }
        }
    }
    out
}

/// a column by its schema, table and name: a path's step, an entity
type Column = (String, String, String);

/// a table of the file: its columns, its primary key, the tables its columns' foreign keys name
struct Entity<'a> {
    table: &'a Table,
    schema: &'a str,
}

/// [security] One file's declarations linked - a table whose path reaches an entity the file defines
/// takes that entity's alias - and, with `strict` (the deploy's mode is `Lsr(true)`:
/// [`SecurityMode::traversal`]), checked: every step a column of a table of the
/// file, each hop's table keyed by one column, a declared foreign key of a hop pointing at the next
/// step's table, and the aliases unique - an alias one entity, an entity one alias.
pub fn link(schemas: &mut OrderedHashMap<Schema>, strict: bool) -> Result<(), String> {
    let mut found: Vec<((String, String), String)> = Vec::new();
    {
        let mut entities: BTreeMap<(String, String), Entity> = BTreeMap::new();
        for s in &schemas.list {
            for t in s.tables.list.iter().filter(|t| !t.is_template()) {
                entities.insert((s.schema_name.clone(), t.table_name.clone()), Entity { table: t, schema: &s.schema_name });
            }
        }
        let templates: BTreeSet<(String, String)> = schemas.list.iter().flat_map(|s| s.tables.list.iter().filter(|t| t.is_template())
            .map(|t| (s.schema_name.clone(), t.table_name.clone()))).collect();
        // each declaration's path, and its endpoint
        let mut paths: BTreeMap<(String, String), Vec<Column>> = BTreeMap::new();
        for (key, e) in &entities {
            let Some(d) = e.table.security.as_ref() else { continue };
            // [levels] a level's path is checked as the dml one; it has no entity and no alias
            if strict {
                for (name, path) in d.level_paths(e.schema, &e.table.table_name)? {
                    check_path(key, &format!("{} (level {name})", d.schema_path[&name].join(", ")), &path, &entities, &templates)?;
                }
            }
            if !d.has_dml() {
                continue;
            }
            let path = d.path_of(e.schema, &e.table.table_name)?;
            if strict {
                check_path(key, &d.dml().join(", "), &path, &entities, &templates)?;
            }
            paths.insert(key.clone(), path);
        }
        let endpoint = |k: &(String, String)| paths.get(k).and_then(|p| p.last()).cloned();
        // the entities the file defines: a table's own column, and the alias it gives it
        let mut defined: BTreeMap<(String, String, String), String> = BTreeMap::new();
        for (key, e) in &entities {
            if let (Some(d), Some(ep)) = (e.table.security.as_ref().filter(|d| d.defining()), endpoint(key)) {
                defined.insert(ep, d.alias_or_column());
            }
        }
        // an entity nobody defines: the alias the paths that reach it say, when they say one
        let mut said: BTreeMap<(String, String, String), BTreeSet<String>> = BTreeMap::new();
        for (key, e) in &entities {
            if let (Some(d), Some(ep)) = (e.table.security.as_ref(), endpoint(key))
                && let Some(a) = d.alias.as_ref().filter(|_| !defined.contains_key(&ep)) {
                said.entry(ep).or_default().insert(a.clone());
            }
        }
        for (key, e) in &entities {
            let (Some(d), Some(ep)) = (e.table.security.as_ref(), endpoint(key)) else { continue };
            let at = format!("{}.{}.{}", ep.0, ep.1, ep.2);
            let entity_alias = defined.get(&ep).cloned().or_else(|| said.get(&ep).filter(|a| a.len() == 1).and_then(|a| a.iter().next().cloned()));
            if strict {
                if let (Some(mine), Some(theirs)) = (d.alias.as_ref(), entity_alias.as_ref())
                    && mine != theirs {
                    return Err(format!("security alias of {}.{}: {at} is {theirs} in this file, not {mine} - an entity has one alias, said once", key.0, key.1));
                }
                if said.get(&ep).is_some_and(|a| a.len() > 1) {
                    return Err(format!("security: {at} is given the aliases {} in this file - an entity has one alias",
                        said[&ep].iter().cloned().collect::<Vec<_>>().join(", ")));
                }
            }
            if d.alias.is_none() && !d.defining()
                && let Some(a) = entity_alias {
                found.push((key.clone(), a));
            }
        }
        if strict {
            // an alias names one entity: the children of one template, each with its own copy of the
            // template's column, count as one
            let mut by_alias: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
            for (key, e) in &entities {
                let (Some(d), Some(ep)) = (e.table.security.as_ref(), endpoint(key)) else { continue };
                let alias = d.alias.clone().or_else(|| defined.get(&ep).cloned())
                    .or_else(|| said.get(&ep).and_then(|a| a.iter().next().cloned())).unwrap_or_else(|| d.alias_or_column());
                let inherited = e.table.use_templates.iter().find(|name| template_declares(schemas, e.schema, name, &e.table.table_name, d));
                let entity = match inherited {
                    Some(t) if d.defining() => format!("the children of the template {t}"),
                    _ => format!("{}.{}.{}", ep.0, ep.1, ep.2),
                };
                by_alias.entry(alias).or_default().insert(entity);
            }
            if let Some((alias, at)) = by_alias.iter().find(|(_, at)| at.len() > 1) {
                return Err(format!("security alias {alias} names {} in this file - an alias is unique: one entity defines it, the other tables' paths lead to it (or they say another alias)",
                    at.iter().cloned().collect::<Vec<_>>().join(" and ")));
            }
        }
    }
    for ((schema, table), alias) in found {
        if let Some(t) = schemas.list.iter_mut().find(|s| s.schema_name == schema).and_then(|s| s.tables.list.iter_mut().find(|t| t.table_name == table))
            && let Some(d) = t.security.as_mut() {
            d.alias = Some(alias);
        }
    }
    Ok(())
}

/// whether the template `name` (of `schema`, or `schema.table`) declares exactly `d`: a child's
/// declaration is then its template's copy
fn template_declares(schemas: &OrderedHashMap<Schema>, schema: &str, name: &str, child: &str, d: &Security) -> bool {
    let (s, t) = name.split_once('.').unwrap_or((schema, name));
    schemas.list.iter().filter(|x| x.schema_name == s).flat_map(|x| x.tables.list.iter())
        .any(|x| x.table_name == t && x.is_template() && x.security.as_ref().map(|l| l.of_child(&x.table_name, child)).as_ref() == Some(d))
}

/// [security] one path's steps against the file's tables
fn check_path(key: &(String, String), text: &str, path: &[Column], entities: &BTreeMap<(String, String), Entity>,
    templates: &BTreeSet<(String, String)>) -> Result<(), String> {
    let me = format!("{}.{}", key.0, key.1);
    for (i, (s, t, c)) in path.iter().enumerate() {
        let Some(e) = entities.get(&(s.clone(), t.clone())) else {
            // [review 2026-11-22] a template is in the file and is no table: said as it is
            if templates.contains(&(s.clone(), t.clone())) {
                return Err(format!("security path [{text}] of {me}: {s}.{t} is a template, not a table - a path leads to a table that uses it"));
            }
            return Err(format!("security path [{text}] of {me}: {s}.{t} is not a table of this file (SecurityMode::Lsr(false) lets a path leave the file)"));
        };
        if !e.table.columns.list.iter().any(|x| &x.name == c) {
            return Err(format!("security path [{text}] of {me}: {c} is not a column of {s}.{t}"));
        }
        if i > 0 && e.table.get_primary_key_columns().len() != 1 {
            return Err(format!("security path [{text}] of {me}: {s}.{t} needs a primary key of one column to be joined on"));
        }
        // a foreign key the YAML declares on this hop's column says where it leads
        if let Some((ns, nt, _)) = path.get(i + 1) {
            let fk = e.table.columns.list.iter().find(|x| &x.name == c).and_then(|x| x.constraint.as_ref()).and_then(|k| k.foreign_key.as_ref());
            if let Some(fk) = fk {
                let r = fk.references.trim();
                let (fs, ft) = r.split_once('.').unwrap_or((s.as_str(), r));
                if (fs, ft) != (ns.as_str(), nt.as_str()) {
                    return Err(format!("security path [{text}] of {me}: {s}.{t}.{c} references {fs}.{ft}, not {ns}.{nt}"));
                }
            }
        }
    }
    Ok(())
}

// ---- the deploy ---------------------------------------------------------------------------------

/// [security] What the database has of `schema.table`'s labac; `roles` the table's grantees,
/// those that may not run labac.enter yet named
#[cfg(feature = "postgres")]
pub async fn have(db: &mut tokio_postgres::Transaction<'_>, schema: &str, table: &str, roles: &[String])
    -> Result<Have, tokio_postgres::Error> {
    let mut h = Have::default();
    let r = db.query_one("SELECT to_regprocedure('labac.ids(text,text)') IS NOT NULL AND to_regclass('labac.tables') IS NOT NULL \
        AND to_regprocedure('labac.enter(text)') IS NOT NULL AND to_regprocedure('labac.enter_session(text)') IS NOT NULL \
        AND obj_description(to_regprocedure('labac.checked(text)'), 'pg_proc') IS NOT DISTINCT FROM $1", &[&SQL_VERSION]).await?;
    h.installed = r.get(0);
    let name = format!("{schema}.{table}");
    if h.installed {
        if let Some(r) = db.query_opt("SELECT to_jsonb(t)::text FROM labac.tables t WHERE schema_name = $1 AND table_name = $2",
            &[&schema, &table]).await? {
            let text: String = r.get(0);
            h.registered = serde_json::from_str::<serde_json::Value>(&text).ok().as_ref().and_then(from_registry);
        }
        let rows = db.query("SELECT r FROM unnest($1::text[]) r WHERE EXISTS (SELECT 1 FROM pg_roles WHERE rolname = r) \
            AND NOT (has_function_privilege(r, 'labac.enter(text)', 'EXECUTE') AND has_function_privilege(r, 'labac.enter_session(text)', 'EXECUTE'))",
            &[&roles]).await?;
        h.enter_missing = rows.iter().map(|r| r.get(0)).collect();
    } else {
        let rows = db.query("SELECT r FROM unnest($1::text[]) r WHERE EXISTS (SELECT 1 FROM pg_roles WHERE rolname = r)", &[&roles]).await?;
        h.enter_missing = rows.iter().map(|r| r.get(0)).collect();
    }
    if let Some(r) = db.query_opt("SELECT relrowsecurity, relforcerowsecurity FROM pg_class WHERE oid = to_regclass($1)", &[&name]).await? {
        h.enabled = r.get(0);
        h.forced = r.get(1);
        let rows = db.query("SELECT polname::text, coalesce(obj_description(oid, 'pg_policy'), '') FROM pg_policy \
            WHERE polrelid = to_regclass($1) AND polname LIKE 'labac\\_%'", &[&name]).await?;
        for r in rows {
            h.policies.insert(r.get(0), r.get(1));
        }
        let others: i64 = db.query_one("SELECT count(*) FROM pg_policy WHERE polrelid = to_regclass($1) AND polname NOT LIKE 'labac\\_%'",
            &[&name]).await?.get(0);
        h.others = others as usize;
    }
    Ok(h)
}

/// [security] every row of the registry, where the database has one that reads
#[cfg(feature = "postgres")]
pub async fn registry(db: &mut tokio_postgres::Transaction<'_>) -> Result<Vec<Registered>, tokio_postgres::Error> {
    let has: bool = db.query_one("SELECT to_regclass('labac.tables') IS NOT NULL", &[]).await?.get(0);
    if !has {
        return Ok(Vec::new());
    }
    let rows = db.query("SELECT to_jsonb(t)::text FROM labac.tables t", &[]).await?;
    Ok(rows.iter().filter_map(|r| serde_json::from_str::<serde_json::Value>(r.get::<_, &str>(0)).ok().as_ref().and_then(from_registry)).collect())
}

/// the tables a path reads beside its own: its hops, as `schema.table`
fn reads(schema: &str, table: &str, d: &Security) -> Vec<String> {
    let mut out: Vec<String> = d.path_of(schema, table).ok().filter(|_| d.has_dml()).unwrap_or_default().into_iter().skip(1)
        .map(|(s, t, _)| format!("{s}.{t}")).collect();
    // [levels] a level's path is read by the policy too
    for path in d.level_paths(schema, table).unwrap_or_default().into_values() {
        out.extend(path.into_iter().skip(1).map(|(s, t, _)| format!("{s}.{t}")));
    }
    out
}

/// [security] [review 2026-11-22] Whether `schema.table`'s policy would come back to the table through
/// the policies of the tables its path reads: itself, or another rls table whose own path reads it.
/// `file` this run's declarations, `rows` the registry's (a table of the file is judged by the file).
pub fn recurses(schema: &str, table: &str, d: &Security, file: &Declared, rows: &[Registered]) -> Option<String> {
    let me = format!("{schema}.{table}");
    let policy_reads = |name: &str| -> Vec<String> {
        let (s, t) = name.split_once('.').unwrap_or(("", name));
        match file.get(&(s.to_string(), t.to_string())) {
            Some(x) => if x.rls { reads(s, t, x) } else { Vec::new() },
            None => rows.iter().filter(|r| r.schema == s && r.table == t && r.rls).flat_map(|r| reads(s, t, &r.security())).collect(),
        }
    };
    let mut seen: BTreeSet<String> = BTreeSet::new();
    let mut todo: Vec<(String, String)> = reads(schema, table, d).into_iter().map(|t| (t.clone(), t)).collect();
    while let Some((t, via)) = todo.pop() {
        if t == me {
            return Some(via);
        }
        if seen.insert(t.clone()) {
            todo.extend(policy_reads(&t).into_iter().map(|n| (n, via.clone())));
        }
    }
    None
}

/// [levels] whether a column of the catalog's `schema.table` holds whole numbers; `None` not known
fn is_integer(dbc: &crate::loader::InfoSchemaType, schema: &str, table: &str, column: &str) -> Option<bool> {
    let c = dbc.get(schema)?.get(table)?.columns.get(column)?;
    let ty = c.column_type.to_lowercase();
    let ty = ty.split('(').next().unwrap_or("").trim();
    Some(matches!(ty, "int" | "int2" | "int4" | "int8" | "integer" | "smallint" | "bigint" | "serial" | "serial2" | "serial4" | "serial8"
        | "smallserial" | "bigserial"))
}

/// whether a column of the catalog's `schema.table` is a boolean
fn is_bool(dbc: &crate::loader::InfoSchemaType, schema: &str, table: &str, column: &str) -> bool {
    dbc.get(schema).and_then(|ts| ts.get(table)).and_then(|t| t.columns.get(column)).is_some_and(|c| c.column_type.to_lowercase().starts_with("bool"))
}

/// [security] One table's declaration deployed, after every table of the run exists: its path
/// resolved by the catalog, its alias by the file (already linked) and the registry, and the SQL
/// that makes it so - with, for a defining table whose alias the registered tables that reach it do
/// not have, their rows and policies carried to it. Empty: nothing to do.
#[cfg(feature = "postgres")]
pub async fn deploy(t: &Table, schema: &str, d: &Security, file: &Declared, dbc: &crate::loader::InfoSchemaType,
    db: &mut tokio_postgres::Transaction<'_>) -> Result<String, String> {
    let table = t.table_name.as_str();
    let pg = |e: tokio_postgres::Error| format!("security of {schema}.{table}: {}", crate::pg_error(&e));
    let steps = d.resolve(schema, table, dbc)?;
    // [levels] each level's path, and its value a whole number: said by name before any SQL is made
    let levels = d.resolve_levels(schema, table, dbc)?;
    for (name, path) in &levels {
        if let Some(l) = path.last().filter(|l| is_integer(dbc, &l.schema, &l.table, &l.column) == Some(false)) {
            return Err(format!("security level {name} of {schema}.{table}: {}.{}.{} must be an integer column", l.schema, l.table, l.column));
        }
    }
    let mut roles: Vec<String> = Vec::new();
    for g in &t.grant {
        for r in [&g.all, &g.select, &g.insert, &g.update, &g.delete] {
            if !r.is_empty() && !roles.contains(r) {
                roles.push(r.clone());
            }
        }
    }
    let have = have(db, schema, table, &roles).await.map_err(pg)?;
    // [review 2026-11-22] the registry is read whenever the database has one - under an older SQL
    // part too: its rows say the aliases, whatever the functions' version
    let rows = registry(db).await.map_err(pg)?;
    let last = steps.last().cloned().unwrap_or_default();
    // [levels] a declaration of levels alone has no entity and no alias
    let endpoint = if d.has_dml() { format!("{}.{}.{}", last.schema, last.table, last.column) } else { String::new() };
    // the other registered tables that reach the same entity: those of this file are deployed by
    // this run, with the file's aliases
    let others: Vec<&Registered> = rows.iter().filter(|r| d.has_dml() && r.entity().as_deref() == Some(endpoint.as_str()))
        .filter(|r| !(r.schema == schema && r.table == table) && !file.contains_key(&(r.schema.clone(), r.table.clone()))).collect();
    let alias = if !d.has_dml() {
        String::new()
    } else if d.defining() {
        d.alias_or_column()
    } else {
        // the entity's alias: this deploy's own defining table says it - under every mode, the file's
        // checks or not - else the database's registry
        let in_file = file.get(&(last.schema.clone(), last.table.clone()))
            .filter(|x| x.defining() && x.dml().first().map(|c| column_of(c)) == Some(last.column.as_str()))
            .map(|x| (x.alias_or_column(), format!("{}.{}", last.schema, last.table), "file"));
        let definer = others.iter().find(|r| r.defining() && r.schema == last.schema && r.table == last.table);
        let registered = match (in_file, definer) {
            (Some(x), _) => Some(x),
            (None, Some(r)) => Some((r.alias.clone(), format!("{}.{}", r.schema, r.table), "database")),
            (None, None) => {
                let said: BTreeSet<&String> = others.iter().map(|r| &r.alias).collect();
                if said.len() > 1 {
                    return Err(format!("security of {schema}.{table}: {endpoint} is registered under the aliases {} - an entity has one alias",
                        said.iter().map(|s| s.as_str()).collect::<Vec<_>>().join(", ")));
                }
                others.first().map(|r| (r.alias.clone(), format!("{}.{}", r.schema, r.table), "database"))
            }
        };
        match (&d.alias, registered) {
            (Some(mine), Some((theirs, who, place))) if *mine != theirs =>
                return Err(format!("security alias of {schema}.{table}: {endpoint} is {theirs} in this {place} ({who}), not {mine} - an entity has one alias, said by the table that defines it")),
            (Some(mine), _) => mine.clone(),
            (None, Some((theirs, _, _))) => theirs,
            (None, None) => d.alias_or_column(),
        }
    };
    if d.rls
        && let Some(via) = recurses(schema, table, d, file, &rows) {
        return Err(format!("security path of {schema}.{table} comes back to it through {via}: under SecurityMode::Rls its policy would recurse - such a schema deploys under lsr"));
    }
    let soft_bool = d.soft_delete.as_deref().is_some_and(|sd| is_bool(dbc, schema, table, sd)
        || t.columns.list.iter().any(|c| c.name == sd && c.column_type.to_lowercase().starts_with("bool")));
    let mut paths = levels;
    paths.extend(dml_alone(steps));
    let resolved = Resolved { paths, alias: alias.clone() };
    let mut sql = d.sql(schema, table, &resolved, &have, soft_bool);
    // a defining table says the alias: the registered tables that reach it follow - their rows and,
    // with rls, their policies
    if d.defining() {
        let mut carried = String::new();
        for r in others.iter().filter(|r| r.alias != alias) {
            // a row of a table the database no longer has: nothing to carry to
            if dbc.get(&r.schema).and_then(|ts| ts.get(&r.table)).is_none() {
                crate::log_info(&format!("security: {}.{} is registered and not a table of this database - alias {alias} not carried to it", r.schema, r.table));
                continue;
            }
            // [review 2026-11-22] the row and the policies change together: a path that no longer
            // resolves stops the deploy, it does not leave the two apart
            let policies = if r.rls {
                let steps = r.security().resolve(&r.schema, &r.table, dbc)
                    .map_err(|e| format!("security alias {alias} of {schema}.{table} cannot be carried to {}.{}, which reaches it: {e}", r.schema, r.table))?;
                // the mark its policy reads: a column the table still has
                if let Some(sd) = r.soft_delete.as_deref().filter(|sd| dbc.get(&r.schema).and_then(|ts| ts.get(&r.table)).is_none_or(|t| !t.columns.contains_key(*sd))) {
                    return Err(format!("security alias {alias} of {schema}.{table} cannot be carried to {}.{}, which reaches it: its softDelete column {sd} is not a column of it",
                        r.schema, r.table));
                }
                let soft = r.soft_delete.as_deref().is_some_and(|sd| is_bool(dbc, &r.schema, &r.table, sd));
                // [levels] its policies are made again whole: its levels with them
                let mut levels = r.security().resolve_levels(&r.schema, &r.table, dbc)
                    .map_err(|e| format!("security alias {alias} of {schema}.{table} cannot be carried to {}.{}, which reaches it: {e}", r.schema, r.table))?;
                levels.extend(dml_alone(steps));
                policies_sql(&r.schema, &r.table, &Resolved { paths: levels, alias: alias.clone() }, r.soft_delete.as_deref(), soft, None)
            } else {
                String::new()
            };
            carried.push_str(&format!("UPDATE labac.tables SET kind = '{}', endpoint = '{}' WHERE schema_name = '{}' AND table_name = '{}';\n",
                lit(&alias), lit(&endpoint), lit(&r.schema), lit(&r.table)));
            carried.push_str(&policies);
        }
        if !carried.is_empty() {
            sql = if sql.is_empty() { with_strings_on(carried) } else { format!("{sql}{carried}") };
        }
    }
    // another entity under the same alias in the database: a file's check cannot see it
    if let Some(r) = rows.iter().filter(|_| d.has_dml()).find(|r| r.alias == alias && r.entity().is_some_and(|e| e != endpoint)
        && !file.contains_key(&(r.schema.clone(), r.table.clone()))) {
        crate::log_info(&format!("security: alias {alias} of {schema}.{table} ({endpoint}) also names {} ({}.{}) in this database",
            r.entity().unwrap_or_default(), r.schema, r.table));
    }
    Ok(sql)
}

/// [security] The declarations of a file's `schemas` deployed, once every table of the file exists:
/// the defining ones first, so a table that reaches an entity finds its alias said. The count of the
/// tables it changed; a dry run hands `callback` each one's SQL.
/// [review 2026-11-22] `migrate_plan` runs it after its table pass. A caller that steps through the
/// tables itself (`Schema::deploy_all_tables_plan_pg`, `Table::deploy_plan_pg` - as sge does) runs
/// [`link`] on the parsed file before and this after: a table's deploy alone leaves its `security`
/// undeployed.
#[cfg(feature = "postgres")]
pub async fn deploy_all(schemas: &OrderedHashMap<Schema>, dbc: &crate::loader::InfoSchemaType, db: &mut tokio_postgres::Transaction<'_>,
    callback: Option<&crate::changes::DryRun<'_>>, opt: &crate::MigrationOptions, file_name: &str) -> Result<usize, String> {
    let declared = declared(schemas, opt.security);
    let mut cnt = 0;
    for defining in [true, false] {
        for s in &schemas.list {
            for t in s.tables.list.iter().filter(|t| !t.is_template()) {
                let Some(d) = declared.get(&(s.schema_name.clone(), t.table_name.clone())).filter(|d| d.defining() == defining) else { continue };
                let sql = deploy(t, &s.schema_name, d, &declared, dbc, db).await
                    .map_err(|e| if file_name.is_empty() { e } else { format!("{e} ({file_name})") })?;
                if sql.is_empty() {
                    continue;
                }
                cnt += 1;
                let name = format!("{}.{}", s.schema_name, t.table_name);
                match callback {
                    Some(callback) => {
                        let changes = crate::changes::Changes { grants: vec!["security".to_string()], ..Default::default() };
                        callback(vec![sql], Some(&name), Some(&changes))?;
                    }
                    None => {
                        db.batch_execute(sql.as_str()).await.map_err(|err| crate::error::DeployError::Pg {
                            step: crate::error::DeployStep::Grants, sql, file: file_name.to_string(), err }.to_string())?;
                    }
                }
            }
        }
    }
    Ok(cnt)
}

#[cfg(test)]
#[path = "labac_test.rs"]
mod tests;
