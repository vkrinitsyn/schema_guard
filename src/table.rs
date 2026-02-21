use std::collections::{HashMap, HashSet};
use std::fmt::Write;

use serde::Serialize;
use tokio_postgres::error::DbError;
use yaml_rust::Yaml;
use yaml_rust::yaml::Array;

use crate::column::{Column, Trig};
use crate::loader::{FKTable, InfoSchemaType, PgTable};
use crate::MigrationOptions;
#[cfg(feature = "slog")]
use crate::log_debug;
use crate::schema::Schema;
use crate::table::CreateST::{SchemaAndTable, TableOnly};
use crate::utils::{Named, OrderedHashMap};

#[derive(Debug, Clone, Serialize)]
pub struct Table {
    #[serde(rename = "tableName")]
    pub table_name: String,
    /// comments
    #[serde(skip_serializing_if = "String::is_empty")]
    pub description: String,
    /// transaction: -- single (default) OR table OR column OR retry (wrap to psql)
    #[serde(skip_serializing_if = "String::is_empty")]
    pub transaction: String,
    /// suffix on table create
    #[serde(skip_serializing_if = "String::is_empty")]
    pub sql: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub constraint: String,
    #[serde(with = "ycolumns")]
    pub columns: OrderedHashMap<Column>,
    #[serde(skip_serializing_if = "OrderedHashMap::is_empty")]
    pub triggers: OrderedHashMap<Trig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data_file: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub data: Vec<Vec<String>>,

    #[serde(skip_serializing_if = "String::is_empty")]
    pub owner: String,

    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub grant: Vec<YGrant>,

    /// Template configuration:
    /// - None: regular table, no template inheritance
    /// - Some(true): this table IS a template (won't be created in DB)
    /// - Some(false): regular table (same as None)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_template: Option<bool>,

    /// List of template names to inherit from (format: "schema.table" or just "table" for same schema)
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub use_templates: Vec<String>,
}


/// grant data
#[derive(Debug, Clone, Serialize)]
pub struct YGrant {
    #[serde(skip_serializing_if = "String::is_empty")]
    pub all: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub select: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub insert: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub update: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub delete: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub truncate: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub references: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub trigger: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub create: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub connect: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub temporary: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub execute: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub usage: String,
    pub with_grant_option: bool,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub by: String,
}

/// wrapper for table to get a format as on lb
#[derive(Serialize)]
struct YtVO<'a> {
    /// createTable
    table: &'a Table,
}

/// wrapper for columns to get a format as on lb
#[derive(Serialize)]
struct YcVO<'a> {
    column: &'a Column,
}

mod ycolumns {
    use serde::{Deserializer, Serializer};
    use serde::ser::SerializeSeq;

    use crate::column::Column;
    use crate::table::YcVO;
    use crate::utils::OrderedHashMap;

    pub fn serialize<S>(columns: &OrderedHashMap<Column>, serializer: S) -> Result<<S as Serializer>::Ok, <S as Serializer>::Error> where
        S: Serializer
    {
        let mut seq = serializer.serialize_seq(Some(columns.len()))?;
        for c in &columns.list {
            let ref vo = YcVO { column: c };
            seq.serialize_element(vo)?;
        }
        seq.end()
    }

    #[allow(dead_code)]
    pub fn deserialize<'de, D>(_deserializer: D) -> Result<OrderedHashMap<Column>, D::Error> where D: Deserializer<'de> { unimplemented!() }
}

pub(crate) mod ytables {
    use serde::{Deserializer, Serializer};
    use serde::ser::SerializeSeq;

    use crate::column::Column;
    use crate::table::{Table, YtVO};
    use crate::utils::OrderedHashMap;

    pub fn serialize<S>(tables: &OrderedHashMap<Table>, serializer: S) -> Result<<S as Serializer>::Ok, <S as Serializer>::Error> where
        S: Serializer
    {
        let mut seq = serializer.serialize_seq(Some(tables.len()))?;
        for t in &tables.list {
            let ref vo = YtVO { table: t };
            seq.serialize_element(vo)?;
        }
        seq.end()
    }

    #[allow(dead_code)]
    pub fn deserialize<'de, D>(_deserializer: D) -> Result<OrderedHashMap<Column>, D::Error> where D: Deserializer<'de> { unimplemented!() }
}

impl Named for Table {
    fn get_name(&self) -> String {
        self.table_name.clone()
    }
}

impl Default for Table {
    fn default() -> Self {
        Table {
            table_name: "".to_string(),
            description: "".to_string(),
            transaction: "".to_string(),
            sql: "".to_string(),
            constraint: "".to_string(),
            columns: OrderedHashMap::new(),
            triggers: OrderedHashMap::new(),
            data_file: None,
            data: vec![],
            owner: "".to_string(),
            grant: vec![],
            is_template: None,
            use_templates: vec![],
        }
    }
}

impl Table {
    pub fn new(
        input: &Yaml,
        table_name: &str,
        file: Option<&String>,
        // log: Option<&Logger>,
    ) -> Result<Self, String> {
        let mut columns = OrderedHashMap::new();
        if let Some(cls) = input["columns"].as_vec() {
            let mut i = 1;
            for cl in cls {
                let c = &cl["column"];
                if !c.is_null() {
                    if let Some(_name) = c["name"].as_str() {
                        let yc = Column::new(c);
                        if let Err(e) = columns.append(yc) {
                            return Err(format!(
                                "{} (column name) {}/{} on table: {}{}{}",
                                e,
                                i,
                                cls.len(),
                                table_name,
                                match file {
                                    None => "",
                                    Some(_) => ", found in file: ",
                                },
                                match file {
                                    None => "",
                                    Some(f) => f.as_str(),
                                },
                            ));
                        };
                    }
                    i += 1;
                }
            }
        }
        let mut triggers = OrderedHashMap::new();
        if let Some(trs) = input["triggers"].as_vec() {
            for tr in trs {
                let t = &tr["trigger"];
                if !t.is_null() {
                    if let Some(name) = t["name"].as_str() {
                        if name.len() > 0 {
                            if let Err(_) = triggers.append(Trig::new(t)) {
                                return Err(format!(
                                    "Duplicate trigger name: {} on table: {}{}{}",
                                    name,
                                    table_name,
                                    match file {
                                        None => "",
                                        Some(_) => ", found in file: ",
                                    },
                                    match file {
                                        None => "",
                                        Some(f) => f.as_str(),
                                    },
                                ));
                            };
                        }
                    }
                }
            }
        }
        // Validate partition_by: only one column allowed, must be RANGE/LIST/HASH
        let mut partition_count = 0usize;
        for col in &mut columns.list {
            if let Some(ref pv) = col.partition_by {
                match pv.as_str() {
                    "RANGE" | "LIST" | "HASH" => {}
                    _ => {
                        return Err(format!(
                            "Invalid partition_by value '{}' on column '{}' in table '{}'. Expected one of: RANGE, LIST, HASH{}{}",
                            pv, col.name, table_name,
                            match file { None => "", Some(_) => ", found in file: " },
                            match file { None => "", Some(f) => f.as_str() },
                        ));
                    }
                }
                partition_count += 1;
                if partition_count > 1 {
                    return Err(format!(
                        "Only one column can have partition_by on table '{}'. Found multiple columns with partition_by{}{}",
                        table_name,
                        match file { None => "", Some(_) => ", found in file: " },
                        match file { None => "", Some(f) => f.as_str() },
                    ));
                }
                // Auto-create index if none declared
                if col.index.is_none() {
                    col.index = Some(crate::column::Index::default());
                }
            }
        }

        let etl = &input["data_file"];
        // Parse template field - can be boolean or array of strings
        let template_field = &input["template"];
        let (is_template, use_templates) = if template_field.is_null() {
            (None, vec![])
        } else if let Some(b) = template_field.as_bool() {
            (Some(b), vec![])
        } else if let Some(arr) = template_field.as_vec() {
            let templates: Vec<String> = arr
                .iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect();
            (None, templates)
        } else {
            (None, vec![])
        };

        Ok(Table {
            table_name: table_name.into(),
            description: crate::utils::as_str(input, "description", ""),
            transaction: crate::utils::as_str(input, "transaction", ""),
            sql: crate::utils::as_str_esc(input, "sql"),
            constraint: crate::utils::as_str_esc(input, "constraint"),
            columns,
            triggers,
            data_file: if etl.is_null() {
                None
            } else {
                etl.as_str().map(|s| s.to_string())
            },
            data: crate::utils::as_vec(input, "data"),
            owner: crate::utils::as_str(input, "owner", ""),
            grant: YGrant::new(input["grant"].as_vec()),
            is_template,
            use_templates,
        })
    }


    /// build a create or alter sql
    #[allow(unused_mut)]
    pub async fn deploy(
        &self,
        dbc: &mut InfoSchemaType,
        db: &mut tokio_postgres::Transaction<'_>,
        schema: &String, // this
        file: &str,
        dry_run: Option<&(dyn Fn(Vec<String>) -> Result<(), String> + Send + Sync)>,
        opt: &MigrationOptions,
    ) -> Result<bool, String> {
        let mut sql = String::new();
        let mut comments = String::new();
        let mut exec = false;
        let do_create = match dbc.get_mut(schema) {
            None => SchemaAndTable,
            Some(mut ss) => match ss.get_mut(&self.table_name) {
                None => TableOnly,
                Some(mut ts) => {
                    let pks = ts.pks();
                    let mut skipped_columns = String::new();
                    for dc in &self.columns.list {
                        if !ts.columns.contains_key(&dc.name) {
                            let def = dc.column_def(schema, &self.table_name, file)?;
                            append(format!(
                                "ALTER TABLE {}.{} ADD COLUMN {}",
                                schema, self.table_name, def.def(pks.is_some())
                            ).as_str(), &mut sql, opt.with_ddl_retry);
                            let _ = ts.columns.insert(dc.get_name(), def);
                            exec = true;
                        } else {
                            // Column exists - check for type changes
                            let existing_col = ts.columns.get(&dc.name).unwrap();
                            let desired_def = dc.column_def(schema, &self.table_name, file)?;

                            // Normalize types for comparison
                            let existing_type = existing_col.column_type.to_lowercase();
                            let desired_type = desired_def.column_type.to_lowercase();

                            if existing_type != desired_type {
                                // Types differ - check if it's a size change
                                let type_change = analyze_type_change(&existing_type, &desired_type);

                                match type_change {
                                    TypeChange::SizeExtension | TypeChange::Compatible => {
                                        // Safe change - always apply
                                        let alter_sql = format!(
                                            "ALTER TABLE {}.{} ALTER COLUMN {} TYPE {}",
                                            schema, self.table_name, dc.name, desired_def.column_type
                                        );
                                        append(&alter_sql, &mut sql, opt.with_ddl_retry);
                                        let _ = ts.columns.insert(dc.get_name(), desired_def);
                                        exec = true;
                                    }
                                    TypeChange::SizeReduction => {
                                        // Potentially destructive - requires with_size_cut
                                        let alter_sql = format!(
                                            "ALTER TABLE {}.{} ALTER COLUMN {} TYPE {}",
                                            schema, self.table_name, dc.name, desired_def.column_type
                                        );
                                        if opt.with_size_cut {
                                            append(&alter_sql, &mut sql, opt.with_ddl_retry);
                                            let _ = ts.columns.insert(dc.get_name(), desired_def);
                                            exec = true;
                                        } else {
                                            if opt.without_failfast {
                                                let _ = writeln!(skipped_columns,
                                                    "-- SKIPPED (with_size_cut=false): {};\n-- Column {} type change from {} to {}",
                                                    alter_sql, dc.name, existing_type, desired_type);
                                            } else {
                                                return Err(format!(
                                                    "Column {} on {}.{} type change from {} to {} requires size reduction but with_size_cut is disabled. SQL: {};",
                                                    dc.name, schema, self.table_name, existing_type, desired_type, alter_sql
                                                ));
                                            }
                                        }
                                    }
                                    TypeChange::Incompatible => {
                                        // Incompatible type change - requires with_size_cut as it's destructive
                                        let alter_sql = format!(
                                            "ALTER TABLE {}.{} ALTER COLUMN {} TYPE {} USING {}::{}",
                                            schema, self.table_name, dc.name, desired_def.column_type, dc.name, desired_def.column_type
                                        );
                                        if opt.with_size_cut {
                                            append(&alter_sql, &mut sql, opt.with_ddl_retry);
                                            let _ = ts.columns.insert(dc.get_name(), desired_def);
                                            exec = true;
                                        } else {
                                            if opt.without_failfast {
                                                let _ = writeln!(skipped_columns,
                                                    "-- SKIPPED (with_size_cut=false): {};\n-- Column {} incompatible type change from {} to {}",
                                                    alter_sql, dc.name, existing_type, desired_type);
                                            } else {
                                                return Err(format!(
                                                    "Column {} on {}.{} has incompatible type change from {} to {} but with_size_cut is disabled. SQL: {};",
                                                    dc.name, schema, self.table_name, existing_type, desired_type, alter_sql
                                                ));
                                            }
                                        }
                                    }
                                    TypeChange::NoChange => {
                                        // No change needed
                                    }
                                }
                            }
                        }
                    }
                    // Log skipped column changes if any
                    if !skipped_columns.is_empty() {
                        #[cfg(not(feature = "slog"))]
                        eprintln!("Skipped column type changes:\n{}", skipped_columns);
                    }
                    if let Some(o) = &ts.owner {
                        if self.owner.len() > 0 && &self.owner != o {
                            append(format!("ALTER TABLE {}.{} OWNER TO {}",
                                           schema, self.table_name, self.owner
                            ).as_str(), &mut sql, opt.with_ddl_retry);
                        }
                    }
                    if !opt.exclude_triggers {
                        let mut skipped_triggers = String::new();
                        for dt in &self.triggers.list {
                            let desired_trigger = dt.to_pg_trigger();
                            let existing_trigger = ts.triggers.get(&dt.name);
                            let trigger_exists = existing_trigger.is_some();
                            let trigger_changed = existing_trigger.map_or(false, |ex| !dt.matches_pg_trigger(ex));

                            if !trigger_exists {
                                // New trigger - create it
                                if let Some(def) = dt.trig_def(schema, &self.table_name) {
                                    let _ = writeln!(sql, "{}\n", def);
                                    let _ = ts.triggers.insert(dt.get_name(), desired_trigger);
                                    exec = true;
                                }
                            } else if trigger_changed {
                                // Trigger changed
                                let drop_sql = format!("DROP TRIGGER IF EXISTS {} ON {}.{};", dt.name, schema, self.table_name);
                                let create_sql = dt.trig_def(schema, &self.table_name).unwrap_or_default();

                                if opt.with_trigger_drop {
                                    // Drop and recreate
                                    let _ = writeln!(sql, "{}", drop_sql);
                                    let _ = writeln!(sql, "{}\n", create_sql);
                                    let _ = ts.triggers.insert(dt.get_name(), desired_trigger);
                                    exec = true;
                                } else {
                                    if opt.without_failfast {
                                        // Show skipped SQL
                                        let _ = writeln!(skipped_triggers, "-- SKIPPED (with_trigger_drop=false):\n-- {}\n-- {}", drop_sql, create_sql);
                                    } else {
                                        return Err(format!(
                                            "Trigger {} on {}.{} has changed but without_failfast is enabled. SQL would be:\n{}\n{}",
                                            dt.name, schema, self.table_name, drop_sql, create_sql
                                        ));
                                    }
                                }
                            }
                        }
                        // Log skipped triggers if any
                        if !skipped_triggers.is_empty() {
                            #[cfg(not(feature = "slog"))]
                            eprintln!("Skipped trigger changes:\n{}", skipped_triggers);
                        }
                    }

                    // Check for primary key changes
                    let desired_pk = self.get_primary_key_columns();
                    let existing_pk = ts.primary_key.as_ref().map(|pk| pk.columns.clone()).unwrap_or_default();

                    if desired_pk != existing_pk {
                        let pk_constraint_name = ts.primary_key.as_ref()
                            .map(|pk| pk.constraint_name.clone())
                            .unwrap_or_else(|| format!("{}_pkey", self.table_name));

                        let drop_pk_sql = if ts.primary_key.is_some() {
                            format!("ALTER TABLE {}.{} DROP CONSTRAINT {};", schema, self.table_name, pk_constraint_name)
                        } else {
                            String::new()
                        };

                        let add_pk_sql = if !desired_pk.is_empty() {
                            format!("ALTER TABLE {}.{} ADD PRIMARY KEY ({});",
                                schema, self.table_name, desired_pk.join(", "))
                        } else {
                            String::new()
                        };

                        // PK change requires with_index_drop since PK is backed by a unique index
                        if opt.with_index_drop {
                            if !drop_pk_sql.is_empty() {
                                append(&drop_pk_sql, &mut sql, opt.with_ddl_retry);
                            }
                            if !add_pk_sql.is_empty() {
                                append(&add_pk_sql, &mut sql, opt.with_ddl_retry);
                            }
                            // Update dbc with new PK
                            ts.primary_key = if desired_pk.is_empty() {
                                None
                            } else {
                                Some(crate::loader::PgPrimaryKey {
                                    constraint_name: format!("{}_pkey", self.table_name),
                                    columns: desired_pk.clone(),
                                })
                            };
                            exec = true;
                        } else {
                            let mut skipped_pk = String::new();
                            if !drop_pk_sql.is_empty() {
                                let _ = writeln!(skipped_pk, "-- {}", drop_pk_sql);
                            }
                            if !add_pk_sql.is_empty() {
                                let _ = writeln!(skipped_pk, "-- {}", add_pk_sql);
                            }

                            if opt.without_failfast {
                                let _ = writeln!(skipped_pk,
                                    "-- SKIPPED (with_index_drop=false): Primary key change from {:?} to {:?}",
                                    existing_pk, desired_pk);
                                #[cfg(not(feature = "slog"))]
                                eprintln!("Skipped primary key changes:\n{}", skipped_pk);
                            } else {
                                return Err(format!(
                                    "Primary key on {}.{} has changed from {:?} to {:?} but with_index_drop is disabled. SQL would be:\n{}{}",
                                    schema, self.table_name, existing_pk, desired_pk, drop_pk_sql, add_pk_sql
                                ));
                            }
                        }
                    }

                    CreateST::None
                }
            },
        };
        if let SchemaAndTable = do_create { // no TableOnly
            dbc.insert(schema.clone(), HashMap::new());
        }
        if {
            match do_create {
                CreateST::None => false,
                _ => true
            }
        } {
            // let owner = "";
            let mut columns = String::new();

            // Get primary key columns from column-level definitions (constraint.primaryKey: true)
            let pk_columns = self.get_primary_key_columns();
            let has_composite_pk = pk_columns.len() > 1;

            let mut st = PgTable {
                table_name: self.table_name.clone(),
                columns: HashMap::new(),
                fks: Default::default(),
                triggers: HashMap::new(),
                indexes: HashMap::new(),
                grants: HashMap::new(),
                primary_key: if pk_columns.is_empty() {
                    None
                } else {
                    Some(crate::loader::PgPrimaryKey {
                        constraint_name: format!("{}_pkey", self.table_name),
                        columns: pk_columns.clone(),
                    })
                },
                sort_order: 0,
                table_comment: None,
                owner: if self.owner.len() > 0 { Some(self.owner.clone()) } else { None },
            };

            for dc in &self.columns.list {
                let cd = dc.column_def(schema, &self.table_name, file)?;
                let _ = st.columns.insert(dc.get_name(), cd);
                self.comments(&mut comments, schema, &dc.name, &dc.description);
            }

            // Build column definitions
            // If composite PK or table-level PK, don't include pk in column def (will add as separate constraint)
            let skip_column_pk = has_composite_pk;
            for dc in &self.columns.list {
                if let Some(cd) = st.columns.get(dc.name.as_str()) {
                    columns.push_str(cd.def(skip_column_pk).as_str());
                    columns.push_str(", ");
                }
            }

            // Add PRIMARY KEY constraint if composite or table-level PK
            if !pk_columns.is_empty() && has_composite_pk {
                columns.push_str(&format!("PRIMARY KEY ({})", pk_columns.join(", ")));
                columns.push_str(", ");
            } else if !pk_columns.is_empty() {
                // Single column PK from column definition - handled in column def
                // But we still need to add it if st.pks() returns something
                let pks = st.pks();
                if let Some(pks) = pks {
                    columns.push_str(pks.as_str());
                    columns.push_str(", ");
                }
            }
            if let SchemaAndTable = do_create {
                if schema.as_str() != "public" {
                    let _ = write!(sql, "CREATE SCHEMA IF NOT EXISTS {} ", schema);
                    if self.owner.len() > 0 {
                        let _ = write!(sql, "AUTHORIZATION {}", self.owner);
                    }
                    let _ = write!(sql, ";\n");
                }
            }
            if let Some(idx) = columns.rfind(",") {
                columns.remove(idx);
            }

            // Build partition clause from column-level partition_by
            let partition_clause = self.columns.list.iter()
                .find_map(|c| c.partition_by.as_ref().map(|pv| format!(" PARTITION BY {} ({})", pv, c.name)))
                .unwrap_or_default();

            let suffix = format!("{}{}", partition_clause, self.sql);
            let csql = format!("CREATE TABLE {}.{} ({}{}{}){}; \n",
                               schema,
                               self.table_name,
                               columns,
                               if self.constraint.len() > 0 { ", " } else { "" },
                               self.constraint,
                               suffix
            );

            sql.push_str(csql.as_str());

            if self.owner.len() > 0 {
                append(format!(
                    "ALTER TABLE {}.{} OWNER TO {}",
                    schema, self.table_name, self.owner
                ).as_str(), &mut sql, opt.with_ddl_retry);
            }
            // }
            if !opt.exclude_triggers {
                for dt in &self.triggers.list {
                    if let Some(td) = dt.trig_def(schema, &self.table_name) {
                        let _ = writeln!(sql, "{}\n", td);
                        st.triggers.insert(dt.get_name(), dt.to_pg_trigger());
                    }
                }
            }
            dbc.get_mut(schema)
                .unwrap()
                .insert(self.table_name.clone(), st);
            exec = true;
        }
        if exec {
            if self.description.len() > 0 {
                let _ = writeln!(
                    comments,
                    "COMMENT ON TABLE {}.{} IS '{}'; \n",
                    schema, self.table_name, self.description
                );
            }

        }

        // Generate CREATE INDEX statements for indexes defined in YAML
        let index_builder = crate::index::IndexBuilder::new(&self.columns);
        let indexes_sql = index_builder.generate_sql(schema, &self.table_name, dbc, opt)?;

        // Generate GRANT/REVOKE statements for grants defined in YAML
        let grant_builder = crate::grant::GrantBuilder::new(&self.grant, &self.table_name);
        let grants_sql = grant_builder.generate_sql(schema, dbc, opt)?;

        let mut data = String::new();
        for row in &self.data {
            self.insert(&mut data, row, schema);
        }

        let exec = exec || !indexes_sql.is_empty() || !grants_sql.is_empty();

        match dry_run {
            Some(store) => {
                store(vec![sql, comments, indexes_sql, grants_sql, data]).map(|_| false)
            }
            None => {
                if exec {
                    if !sql.is_empty() {
                        #[cfg(feature = "slog")] log_debug("deploy SQL", &sql, file, schema);
                        let _ = db.batch_execute(sql.as_str()).await
                            .map_err(|e| Self::format_it("DB execute", sql, e, file))?;
                    }
                    if !comments.is_empty() {
                        #[cfg(feature = "slog")] log_debug("deploy SQL", &comments, file, schema);
                        let _ = db.batch_execute(comments.as_str()).await
                            .map_err(|e| Self::format_it("DB execute comments", comments, e, file))?;
                    }
                    if !indexes_sql.is_empty() {
                        #[cfg(feature = "slog")] log_debug("deploy SQL", &indexes_sql, file, schema);
                        let _ = db.batch_execute(indexes_sql.as_str()).await
                            .map_err(|e| Self::format_it("DB execute indexes", indexes_sql, e, file))?;
                    }
                    if !grants_sql.is_empty() {
                        #[cfg(feature = "slog")] log_debug("deploy SQL", &grants_sql, file, schema);
                        let _ = db.batch_execute(grants_sql.as_str()).await
                            .map_err(|e| Self::format_it("DB execute grants", grants_sql, e, file))?;
                    }
                    if !data.is_empty() {
                        #[cfg(feature = "slog")] log_debug("deploy SQL", &data, file, schema);
                        let _ = db.batch_execute(data.as_str()).await
                            .map_err(|e| Self::format_it("DB execute data upserts", data, e, file))?;
                    }
                }
                Ok(exec)
            }
        }
    }

    fn format_it(msg: &str, sql: String, e: tokio_postgres::Error, file: &str) -> String {
        format!("{} [{}] {} {} \nThe error is: {}", msg, sql,
                if file.len() > 0 { ", source: " } else { "" },
                file,
                match e.as_db_error() {
                    None => e.to_string(),
                    Some(e) => e.to_string()
                }
        )
    }

    fn insert(&self, data: &mut String, row: &Vec<String>, schema: &String) {
        let mut names = String::new();
        let mut vals = String::new();
        let mut pks = String::new();
        for i in 0..row.len() {
            let c = self.columns.list.get(i).unwrap();
            if c.is_pk() {
                if pks.len() > 0 {
                    pks.push_str(", ");
                }
                pks.push_str(c.name.as_str());
            }
            if i > 0 {
                names.push_str(", ");
                vals.push_str(", ");
            }
            names.push_str(c.name.as_str());
            vals.push_str("'");
            vals.push_str(row[i].as_str());
            vals.push_str("'");
        }
        let _ = writeln!(data, " insert into {}.{} ({}) values ({}) ON CONFLICT ({}) DO NOTHING;", schema, self.table_name, names, vals, pks);
    }
    //YTable

    #[inline]
    fn _is_pk(&self, input: &Yaml) -> bool {
        let mut yes = false;
        let constraint = &input["constraint"];
        if !constraint.is_null() {
            if let Some(pk) = constraint["primaryKey"].as_bool() {
                yes = pk;
            }
        }
        yes
    }

    /// column defenition to SQL string
    #[inline]
    fn comments(&self, sql: &mut String, schema: &String, column_name: &String, t: &String) {
        // if let Some(t) = input["description"].as_str() {
        if t.len() > 0 {
            let _ = writeln!(
                sql,
                "COMMENT ON COLUMN {}.{}.{} IS '{}';",
                schema, self.table_name, column_name, t
            );
        }
    }

    /// build a create or alter sql
    #[allow(unused, unused_mut)]
    pub async fn deploy_fk(
        &self,
        // target: &FileVersion,
        schemas: &OrderedHashMap<Schema>, //FilesMap,
        dbc: &mut InfoSchemaType,
        db: &mut tokio_postgres::Transaction<'_>,
        schema: &String,
        is_retry: bool,
        file: &str,
        dry_run: Option<&(dyn Fn(Vec<String>) -> Result<(), String> + Send + Sync)>,
    ) -> Result<bool, String> {
        let mut sql = String::new();
        let mut fk_list = HashMap::new();
        if let Some(ss) = dbc.get(schema) {
            if let Some(ts) = ss.get(&self.table_name) {
                for dc in &self.columns.list {
                    if let Some(constraint) = &dc.constraint {
                        if let Some(fk) = &constraint.foreign_key {
                            let fk_table = &fk.references;
                            let (fk_schema, fk_table) = match fk_table.find(".") {
                                None => (schema.clone(), fk_table.to_string()),
                                Some(i) => {
                                    (fk_table[0..i].to_string(), fk_table[i + 1..].to_string())
                                }
                            };
                            // check for FK already in DB
                            if let Some(a) = dbc.get(schema) {
                                if let Some(b) = a.get(&self.table_name) {
                                    if let Some(_) = b.fks.get(&dc.name) {
                                        continue; // FK found as already created in DB, check the next column
                                    }
                                }
                            }
                            let fk_columns = pks(&fk_schema, &fk_table, schemas);
                            let key = format!("{}.{}", fk_schema, fk_table);
                            if fk_columns.len() > 0 {
                                fk_list.insert(key, FKTable {
                                    column: fk_columns,
                                    name: dc.get_name(),
                                    schema: fk_schema,
                                    table: fk_table,
                                    sql: fk.sql.clone(),
                                });
                            }
                        }
                    }
                }
            }
        };
        let exec = fk_list.len() > 0;
        for ff in fk_list.values() {
            if let Some(mut ss) = dbc.get_mut(schema) {
                if let Some(mut ts) = ss.get_mut(&self.table_name) {
                    ts.fks.insert(ff.name.clone(), ff.clone());
                }
            }
            append(format!(
                "ALTER TABLE {}.{} ADD CONSTRAINT fk_{}_{}_{}_{} FOREIGN KEY ({}) REFERENCES {}.{} ({}) {}",
                schema, self.table_name, schema, self.table_name, ff.table,
                ff.name, ff.name, ff.schema, &ff.table, ff.columns(), ff.sql
            ).as_str(), &mut sql, is_retry);
        }

        match dry_run {
            Some(store) => {
                store(vec![sql]).map(|_| false)
            }
            None => {
                if exec {
                    if let Err(e) = db.batch_execute(sql.as_str()).await {
                        return Err(format!("DB FK execute [{}]: {} source: {}", sql, e, file));
                    }
                    Ok(exec)
                } else {
                    Ok(true)
                }
            }
        }
    }
    /*
        /// calc pseudo weight by constraints count
        // TODO replace to weight calculation: level from a top dictionary table
        pub fn pseudo_weight(&self) -> u8 {
            let mut w = 0;
            for c in &self.columns.list {
                if let Some(x) = &c.constraint {
                    if x.foreign_key.is_some() {
                        w += 1;
                    }
                }
            }
            w
        }
    */
    pub fn is_table_transaction(&self) -> bool {
        self.transaction.as_str() == "table"
            || self.transaction.as_str() == "retry"
    }

    /// Check if this table is a template (should not be created in DB)
    pub fn is_template(&self) -> bool {
        self.is_template.unwrap_or(false)
    }

    /// Merge another table's definition into this table (for template inheritance)
    /// Template columns, triggers, and grants are added first, then this table's definitions override
    pub fn merge_from(&mut self, template: &Table) {
        // Merge columns - template columns come first, table columns override if same name
        let mut merged_columns = OrderedHashMap::new();
        for col in &template.columns.list {
            let _ = merged_columns.append(col.clone());
        }
        for col in &self.columns.list {
            if merged_columns.map.contains_key(&col.name) {
                // Override existing column from template
                if let Some(existing) = merged_columns.get_mut(&col.name) {
                    *existing = col.clone();
                }
            } else {
                let _ = merged_columns.append(col.clone());
            }
        }
        self.columns = merged_columns;

        // Merge triggers - template triggers come first, table triggers override if same name
        let mut merged_triggers = OrderedHashMap::new();
        for trig in &template.triggers.list {
            let _ = merged_triggers.append(trig.clone());
        }
        for trig in &self.triggers.list {
            if merged_triggers.map.contains_key(&trig.name) {
                // Override existing trigger from template
                if let Some(existing) = merged_triggers.get_mut(&trig.name) {
                    *existing = trig.clone();
                }
            } else {
                let _ = merged_triggers.append(trig.clone());
            }
        }
        self.triggers = merged_triggers;

        // Merge grants - template grants come first, then table grants are added
        let mut merged_grants = template.grant.clone();
        merged_grants.extend(self.grant.clone());
        self.grant = merged_grants;

        // Inherit owner if not set
        if self.owner.is_empty() && !template.owner.is_empty() {
            self.owner = template.owner.clone();
        }

        // Inherit description if not set
        if self.description.is_empty() && !template.description.is_empty() {
            self.description = template.description.clone();
        }

        // Inherit sql suffix if not set
        if self.sql.is_empty() && !template.sql.is_empty() {
            self.sql = template.sql.clone();
        }

        // Inherit constraint if not set
        if self.constraint.is_empty() && !template.constraint.is_empty() {
            self.constraint = template.constraint.clone();
        }
        // Note: primary key columns are inherited through merged columns with constraint.primaryKey
    }

    /// Get the primary key columns from columns with constraint.primaryKey = true
    /// Returns columns in order (preserves column order from the columns list)
    pub fn get_primary_key_columns(&self) -> Vec<String> {
        self.columns.list.iter()
            .filter(|c| c.is_pk())
            .map(|c| c.name.clone())
            .collect()
    }
}

impl YGrant {
    fn new(input: Option<&Array>) -> Vec<Self> {
        let mut data = Vec::new();
        if let Some(vv) = input {
            for v in vv {
                data.push(YGrant {
                    all: crate::utils::safe_sql_name(crate::utils::as_str_esc(v, "all")),
                    select: crate::utils::safe_sql_name(crate::utils::as_str_esc(v, "select")),
                    insert: crate::utils::safe_sql_name(crate::utils::as_str_esc(v, "insert")),
                    update: crate::utils::safe_sql_name(crate::utils::as_str_esc(v, "update")),
                    delete: crate::utils::safe_sql_name(crate::utils::as_str_esc(v, "delete")),
                    truncate: crate::utils::safe_sql_name(crate::utils::as_str_esc(v, "truncate")),
                    references: crate::utils::safe_sql_name(crate::utils::as_str_esc(v, "references")),
                    trigger: crate::utils::safe_sql_name(crate::utils::as_str_esc(v, "trigger")),
                    create: crate::utils::safe_sql_name(crate::utils::as_str_esc(v, "create")),
                    connect: crate::utils::safe_sql_name(crate::utils::as_str_esc(v, "connect")),
                    temporary: crate::utils::safe_sql_name(crate::utils::as_str_esc(v, "temporary")),
                    execute: crate::utils::safe_sql_name(crate::utils::as_str_esc(v, "execute")),
                    usage: crate::utils::safe_sql_name(crate::utils::as_str_esc(v, "usage")),
                    with_grant_option: crate::utils::as_bool(v, "with_grant_option", false),
                    by: crate::utils::safe_sql_name(crate::utils::as_str_esc(v, "by")),
                });
            }
        }
        data
    }
}

#[derive(Debug)]
enum CreateST {
    None,
    SchemaAndTable,
    TableOnly,
}

fn append(sql: &str, buff: &mut String, retry: bool) {
    if retry {
        buff.push_str(RPT1);
        buff.push_str(sql);
        buff.push_str(RPT2);
    } else {
        buff.push_str(sql);
        buff.push_str(";\n");
    }
}

const RPT1: &str = r#"DO
$do$
DECLARE
   lock_timeout CONSTANT text := '1000ms';
   max_attempts CONSTANT INT := 100;
   ddl_completed BOOLEAN := FALSE;
BEGIN

   PERFORM set_config('lock_timeout', lock_timeout, FALSE);

   FOR i IN 1..max_attempts LOOP
      BEGIN
         EXECUTE '"#;

const RPT2: &str = r#"';
         ddl_completed := TRUE;
         EXIT;
      EXCEPTION
         WHEN lock_not_available THEN
           NULL;
      END;
   END LOOP;

   IF ddl_completed THEN
      RAISE INFO 'DDL has been successfully completed';
   ELSE
      RAISE EXCEPTION 'Failed to perform DDL';
   END IF;
END
$do$;
"#;


/// Type of column type change
#[derive(Debug, PartialEq)]
enum TypeChange {
    /// No change needed
    NoChange,
    /// Size extension (safe) - e.g., varchar(50) -> varchar(100)
    SizeExtension,
    /// Size reduction (potentially destructive) - e.g., varchar(100) -> varchar(50)
    SizeReduction,
    /// Compatible type change (safe) - e.g., int4 -> int8
    Compatible,
    /// Incompatible type change (destructive) - e.g., int -> varchar
    Incompatible,
}

/// Parse size from type string, returns (base_type, size, scale)
/// Examples:
/// - varchar(100) -> ("varchar", Some(100), None)
/// - NUMERIC(10,2) -> ("numeric", Some(10), Some(2))
/// - int4 -> ("int4", None, None)
fn parse_type_size(type_str: &str) -> (String, Option<i32>, Option<i32>) {
    let type_lower = type_str.to_lowercase().trim().to_string();

    if let Some(paren_pos) = type_lower.find('(') {
        let base_type = type_lower[..paren_pos].trim().to_string();
        let params = &type_lower[paren_pos + 1..type_lower.len() - 1];

        if params.contains(',') {
            // NUMERIC(precision, scale)
            let parts: Vec<&str> = params.split(',').collect();
            let precision = parts.get(0).and_then(|s| s.trim().parse::<i32>().ok());
            let scale = parts.get(1).and_then(|s| s.trim().parse::<i32>().ok());
            (base_type, precision, scale)
        } else {
            // varchar(length)
            let size = params.trim().parse::<i32>().ok();
            (base_type, size, None)
        }
    } else {
        (type_lower, None, None)
    }
}

/// Analyze the type change between existing and desired column types
fn analyze_type_change(existing: &str, desired: &str) -> TypeChange {
    let (existing_base, existing_size, existing_scale) = parse_type_size(existing);
    let (desired_base, desired_size, desired_scale) = parse_type_size(desired);

    // Normalize base types (int4 == integer == int == serial, etc.)
    // serial/bigserial/smallserial are pseudo-types that create int4/int8/int2 with sequences
    let normalize_base = |t: &str| -> String {
        match t {
            "int" | "int4" | "integer" | "serial" => "int4".to_string(),
            "int8" | "bigint" | "bigserial" | "serial8" => "int8".to_string(),
            "int2" | "smallint" | "smallserial" | "serial2" => "int2".to_string(),
            "float4" | "real" => "float4".to_string(),
            "float" | "float8" | "double precision" | "double" => "float8".to_string(),
            "bool" | "boolean" => "bool".to_string(),
            "varchar" | "character varying" => "varchar".to_string(),
            "char" | "character" | "bpchar" => "char".to_string(),
            "text" => "text".to_string(),
            "numeric" | "decimal" => "numeric".to_string(),
            "timestamptz" | "timestamp with time zone" => "timestamptz".to_string(),
            "timestamp" | "timestamp without time zone" => "timestamp".to_string(),
            "timetz" | "time with time zone" => "timetz".to_string(),
            "time" | "time without time zone" => "time".to_string(),
            "json" | "jsonb" => t.to_string(), // keep distinct, jsonb is different from json
            other => other.to_string(),
        }
    };

    let norm_existing = normalize_base(&existing_base);
    let norm_desired = normalize_base(&desired_base);

    // Same base type - check size changes
    if norm_existing == norm_desired {
        match (&existing_size, &desired_size) {
            (Some(e), Some(d)) => {
                if e == d {
                    // Check scale for NUMERIC
                    match (&existing_scale, &desired_scale) {
                        (Some(es), Some(ds)) if es == ds => TypeChange::NoChange,
                        (Some(es), Some(ds)) if ds > es => TypeChange::SizeExtension,
                        (Some(_), Some(_)) => TypeChange::SizeReduction,
                        (None, None) => TypeChange::NoChange,
                        _ => TypeChange::SizeExtension, // Scale added or removed
                    }
                } else if d > e {
                    TypeChange::SizeExtension
                } else {
                    TypeChange::SizeReduction
                }
            }
            (None, Some(_)) => TypeChange::SizeExtension, // Adding size constraint
            (Some(_), None) => TypeChange::SizeExtension, // Removing size constraint (e.g., varchar(100) -> text)
            (None, None) => TypeChange::NoChange,
        }
    } else {
        // Different base types - check compatibility
        // Safe promotions: int2 -> int4 -> int8, float4 -> float8
        let is_safe_promotion = matches!(
            (norm_existing.as_str(), norm_desired.as_str()),
            ("int2", "int4") | ("int2", "int8") | ("int4", "int8") |
            ("float4", "float8") |
            ("varchar", "text") | ("char", "text") | ("char", "varchar")
        );

        if is_safe_promotion {
            TypeChange::Compatible
        } else {
            // Check if converting to varchar/text with sufficient size
            // These conversions are safe if varchar is large enough to hold the string representation
            let min_varchar_size = match norm_existing.as_str() {
                "int2" => Some(6),      // -32768 to 32767
                "int4" => Some(11),     // -2147483648 to 2147483647
                "int8" => Some(20),     // -9223372036854775808 to 9223372036854775807
                "float4" => Some(15),   // ~7 decimal digits + sign + decimal point + exponent
                "float8" => Some(25),   // ~15 decimal digits + sign + decimal point + exponent
                "numeric" => Some(existing_size.map(|s| s + 2).unwrap_or(40)), // precision + sign + decimal
                "bool" => Some(5),      // "false"
                "timestamp" | "timestamptz" => Some(32), // '2024-01-22 12:34:56.123456+00'
                "time" | "timetz" => Some(18),           // '12:34:56.123456+00'
                "date" => Some(10),     // '2024-01-22'
                "uuid" => Some(36),     // 'xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx'
                "json" | "jsonb" => None, // variable, can't determine safe size
                _ => None,
            };

            // Check if target is varchar/text with sufficient size
            let is_safe_to_varchar = match (norm_desired.as_str(), desired_size) {
                ("text", _) => min_varchar_size.is_some(), // text has no limit
                ("varchar", Some(size)) => min_varchar_size.map_or(false, |min| size >= min),
                ("varchar", None) => min_varchar_size.is_some(), // varchar without size = unlimited
                _ => false,
            };

            if is_safe_to_varchar {
                TypeChange::Compatible
            } else {
                TypeChange::Incompatible
            }
        }
    }
}

#[inline]
fn pks(schema: &String, table: &String, sks: &OrderedHashMap<Schema>) -> HashSet<String> {
    let mut pk = HashSet::new();
    if let Some(fkst) = sks.get(schema) {
        if let Some(tt) = fkst.tables.get(table) {
            for cc in &tt.columns.list {
                if cc.is_pk() {
                    pk.insert(cc.get_name());
                }
            }
        }
    }
    pk
}
