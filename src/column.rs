use serde::Serialize;
use yaml_rust::Yaml;

use crate::loader::PgColumnDfn;
use crate::utils::Named;

impl Named for Column {
    fn get_name(&self) -> String {
        self.name.clone()
    }
}

impl Named for Trig {
    fn get_name(&self) -> String {
        self.name.clone()
    }
}

impl Default for Column {
    fn default() -> Self {
        Column {
            name: "".to_string(),
            column_type: "".to_string(),
            default_value: None,
            constraint: None,
            description: "".to_string(),
            sql: "".to_string(),
            index: None,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct Column {
    pub name: String,
    #[serde(rename = "type")]
    pub column_type: String,
    #[serde(rename = "defaultValue", skip_serializing_if = "Option::is_none")]
    pub default_value: Option<String>,
    //
    #[serde(skip_serializing_if = "Option::is_none")]
    pub constraint: Option<Constr>,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub description: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub sql: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub index: Option<Index>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct Index {
    #[serde(skip_serializing_if = "String::is_empty")]
    pub name: String,
    /// UNIQUE index
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unique: Option<bool>,
    /// CREATE INDEX CONCURRENTLY
    #[serde(skip_serializing_if = "Option::is_none")]
    pub concurrently: Option<bool>,
    /// Index method: btree, hash, gist, spgist, gin, brin
    #[serde(skip_serializing_if = "String::is_empty")]
    pub using: String,
    /// ASC or DESC
    #[serde(skip_serializing_if = "String::is_empty")]
    pub order: String,
    /// NULLS FIRST or NULLS LAST
    #[serde(skip_serializing_if = "String::is_empty")]
    pub nulls: String,
    /// COLLATE collation
    #[serde(skip_serializing_if = "String::is_empty")]
    pub collate: String,
    /// Additional SQL (deprecated, use specific fields instead)
    #[serde(skip_serializing_if = "String::is_empty")]
    pub sql: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct Constr {
    #[serde(rename = "primaryKey", skip_serializing_if = "Option::is_none")]
    pub primary_key: Option<bool>,
    pub nullable: bool,
    #[serde(rename = "foreignKey", skip_serializing_if = "Option::is_none")]
    pub foreign_key: Option<ForeignKey>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ForeignKey {
    pub references: String,
    //fk_table
    #[serde(skip_serializing_if = "String::is_empty")]
    pub sql: String, //-- some SQL suffix on new FK create- on delete no action on update no action
}

#[derive(Debug, Clone, Serialize)]
pub struct Trig {
    pub name: String,
    //uniq_name_of_trigger
    pub event: String,
    // before update
    pub when: String,
    //  for each row
    pub proc: String,  //  -- p()
}

impl Column {
    pub(crate) fn new(input: &Yaml) -> Self {
        let constraint = &input["constraint"];
        let foreign_key = &constraint["foreignKey"];
        let references = crate::utils::as_str_esc(foreign_key, "references");
        let (foreign_key, fk_set) = if references.len() == 0 {
            (None, false)
        } else {
            (
                Some(ForeignKey {
                    references,
                    sql: crate::utils::as_str_esc(foreign_key, "sql"),
                }),
                true,
            )
        };
        let primary_key = crate::utils::as_bool(constraint, "primaryKey", false);
        let nullable = crate::utils::as_bool(constraint, "nullable", true);
        let constraint = if primary_key || !nullable || fk_set {
            Some(Constr {
                primary_key: if primary_key { Some(true) } else { None },
                nullable,
                foreign_key,
            })
        } else {
            None
        };
        let index = &input["index"];
        // Parse index: can be boolean (true = default index) or object (full config)
        // - index: true → create index with auto-generated name
        // - index: { name: "+" } or { name: "my_idx" } → create index
        // - index: false, index: {}, or not present → no index
        let index = if index.is_null() {
            None
        } else if let Some(b) = index.as_bool() {
            if b {
                Some(Index::default()) // index: true creates index with auto-generated name
            } else {
                None // index: false means no index
            }
        } else {
            Index::new(index) // index: { ... } returns Some only if name is set
        };

        Column {
            name: crate::utils::safe_sql_name(crate::utils::as_str_esc(input, "name")),
            column_type: crate::utils::as_str_esc(input, "type"),
            default_value: input["defaultValue"].as_str().map(|s| crate::utils::as_esc(s)),
            description: crate::utils::as_str_esc(input, "description"),
            sql: crate::utils::as_str_esc(input, "sql"),
            constraint,
            index,
        }
    }

    // #[cfg(test)]
    pub fn newt(name: &str, ctype: &str, primary_key: bool, nullable: bool) -> Self {
        let constraint = if primary_key || !nullable {
            Some(Constr {
                primary_key: if primary_key { Some(true) } else { None },
                nullable,
                foreign_key: None,
            })
        } else {
            None
        };
        Column {
            name: name.to_string(),
            column_type: ctype.to_string(),
            default_value: None,
            description: "".to_string(),
            sql: "".to_string(),
            constraint,
            index: None,
        }
    }

    pub fn is_pk(&self) -> bool {
        match &self.constraint {
            None => false,
            Some(c) => c.primary_key.unwrap_or(false),
        }
    }

    #[inline]
    pub(crate) fn column_def(
        &self,
        schema: &String,
        table_name: &String,
        file: &str,
    ) -> Result<PgColumnDfn, String> {
        if self.column_type.len() == 0 {
            Err(format!(
                "Error column definition on table: {}.{} column {} file {}",
                schema, table_name, self.name, file
            ))
        } else {
            let c = self.constraint.clone();
            Ok(PgColumnDfn {
                column_name: self.name.clone(),
                column_type: self.column_type.clone(),
                column_default: self.default_value.clone(),
                sql: Some(self.sql.trim().into()),
                pk: c.as_ref().map_or(false, |c| c.primary_key.unwrap_or(false)),
                nullable: c.as_ref().map_or(true, |c| c.nullable),
                fk: c.map_or(None, |cs| cs.foreign_key
                    .map_or(None, |fk| Some((fk.references.trim().into(), fk.sql.trim().into())))),
                sort_order: 0,
                column_comment: None,
            })
        }
    }
}

impl Trig {
    pub(crate) fn new(input: &Yaml) -> Self {
        Trig {
            name: crate::utils::safe_sql_name(crate::utils::as_str_esc(input, "name")),
            event: crate::utils::as_str_esc(input, "event"),
            when: crate::utils::as_str_esc(input, "when"),
            proc: crate::utils::as_str_esc(input, "proc"),
        }
    }

    /// column defenition to SQL string
    #[inline]
    pub(crate) fn trig_def(&self, schema: &String, table_name: &String) -> Option<String> {
        // let proc = as_str_esc(input, "proc");
        if self.proc.len() > 0 {
            Some(format!(
                "CREATE TRIGGER {} {} ON {}.{} {} EXECUTE PROCEDURE {};",
                self.name, self.event, schema, table_name, self.when, self.proc
            ))
        } else {
            None
        }
    }

    /// Convert to PgTrigger for storage in dbc
    pub(crate) fn to_pg_trigger(&self) -> crate::loader::PgTrigger {
        crate::loader::PgTrigger {
            trigger_name: self.name.clone(),
            event: self.event.to_uppercase(),
            orientation: self.when.to_uppercase(),
            proc: self.proc.clone(),
        }
    }

    /// Check if this trigger matches an existing PgTrigger from the database
    pub(crate) fn matches_pg_trigger(&self, existing: &crate::loader::PgTrigger) -> bool {
        // Normalize for comparison
        let self_event = self.event.to_uppercase();
        let self_when = self.when.to_uppercase();

        // Compare event (BEFORE INSERT, AFTER UPDATE, etc.)
        if self_event != existing.event {
            return false;
        }

        // Compare orientation (FOR EACH ROW, FOR EACH STATEMENT)
        if self_when != existing.orientation {
            return false;
        }

        // Compare procedure - existing includes schema, self might not
        // Handle both "schema.func()" and "func()" formats
        let self_proc = self.proc.trim();
        let existing_proc = existing.proc.trim();

        // If self_proc doesn't have schema prefix, check if existing ends with it
        if self_proc.contains('.') {
            self_proc == existing_proc
        } else {
            // self_proc is just "func()", existing is "schema.func()"
            existing_proc.ends_with(self_proc) || existing_proc == self_proc
        }
    }
}

impl Default for Index {
    fn default() -> Self {
        Index {
            name: "+".to_string(), // "+" triggers auto-generated index name
            unique: None,
            concurrently: None,
            using: String::new(),
            order: String::new(),
            nulls: String::new(),
            collate: String::new(),
            sql: String::new(),
        }
    }
}

impl Index {
    pub(crate) fn new(input: &Yaml) -> Option<Self> {
        let name = crate::utils::as_str_esc(input, "name");
        // If name is empty, no index is created (must explicitly set name or use index: true)
        if name.is_empty() {
            return None;
        }
        let unique_val = crate::utils::as_bool(input, "unique", false);
        let concurrently_val = crate::utils::as_bool(input, "concurrently", false);
        Some(Index {
            name,
            unique: if unique_val { Some(true) } else { None },
            concurrently: if concurrently_val { Some(true) } else { None },
            using: crate::utils::as_str_esc(input, "using").to_lowercase(),
            order: crate::utils::as_str_esc(input, "order").to_uppercase(),
            nulls: crate::utils::as_str_esc(input, "nulls").to_uppercase(),
            collate: crate::utils::as_str_esc(input, "collate"),
            sql: crate::utils::as_str_esc(input, "sql"),
        })
    }
}