use std::collections::HashMap;

use crate::column::Column;
use crate::loader::{InfoSchemaType, PgIndex, PgIndexColumn};
use crate::utils::OrderedHashMap;
use crate::MigrationOptions;

/// Represents a desired index configuration collected from YAML
#[derive(Debug, Clone)]
pub struct DesiredIndex {
    pub name: String,
    pub columns: Vec<DesiredIndexColumn>,
    pub is_unique: bool,
    pub concurrently: bool,
    pub using: String,
}

/// Represents a column in a desired index
#[derive(Debug, Clone)]
pub struct DesiredIndexColumn {
    pub column_name: String,
    pub order: String,
    pub nulls: String,
    pub collate: String,
}

/// Collects and generates CREATE INDEX statements for table columns
pub struct IndexBuilder {
    /// index_name -> DesiredIndex
    index_groups: HashMap<String, DesiredIndex>,
}

impl IndexBuilder {
    /// Create a new IndexBuilder by collecting indexes from columns
    pub fn new(columns: &OrderedHashMap<Column>) -> Self {
        let mut index_groups: HashMap<String, DesiredIndex> = HashMap::new();

        for col in &columns.list {
            if let Some(idx) = &col.index {
                if idx.name.is_empty() {
                    continue;
                }

                let col_info = DesiredIndexColumn {
                    column_name: col.name.clone(),
                    order: idx.order.clone(),
                    nulls: idx.nulls.clone(),
                    collate: idx.collate.clone(),
                };

                match index_groups.get_mut(&idx.name) {
                    Some(existing) => {
                        existing.columns.push(col_info);
                    }
                    None => {
                        index_groups.insert(
                            idx.name.clone(),
                            DesiredIndex {
                                name: idx.name.clone(),
                                columns: vec![col_info],
                                is_unique: idx.unique.unwrap_or(false)
                                    || idx.sql.to_uppercase().contains("UNIQUE"),
                                concurrently: idx.concurrently.unwrap_or(false),
                                using: idx.using.clone(),
                            },
                        );
                    }
                }
            }
        }

        IndexBuilder { index_groups }
    }

    /// Generate CREATE/DROP INDEX SQL statements and update dbc with created indexes
    pub fn generate_sql(
        &self,
        schema: &str,
        table_name: &str,
        dbc: &mut InfoSchemaType,
        opt: &MigrationOptions,
    ) -> Result<String, String> {
        let mut indexes_sql = String::new();
        let mut skipped_sql = String::new();

        // Get existing indexes from dbc
        let existing_indexes: HashMap<String, PgIndex> = if let Some(ss) = dbc.get(schema) {
            if let Some(ts) = ss.get(table_name) {
                ts.indexes.clone()
            } else {
                HashMap::new()
            }
        } else {
            HashMap::new()
        };

        for (idx_name, desired) in &self.index_groups {
            // Generate actual index name - "+" means auto-generate
            let actual_index_name = if idx_name == "+" || idx_name.is_empty() {
                let col_names: Vec<&str> = desired.columns.iter().map(|c| c.column_name.as_str()).collect();
                format!("idx_{}_{}", table_name, col_names.join("_"))
            } else {
                idx_name.clone()
            };

            // Check if index exists and if it needs to be updated
            if let Some(existing) = existing_indexes.get(&actual_index_name) {
                if self.index_matches(desired, existing) {
                    // Index exists and matches - skip
                    continue;
                }
                let drop_sql = format!("DROP INDEX IF EXISTS {}.{};", schema, actual_index_name);
                // Index exists but differs
                if opt.with_index_drop {
                    // Drop it first
                    indexes_sql.push_str(&format!(
                        "DROP INDEX IF EXISTS {}.{};\n",
                        schema, actual_index_name
                    ));
                } else {
                    let create_sql = self.build_create_index_sql(schema, table_name, &actual_index_name, desired); 
                    if opt.without_failfast {
                        // Show skipped SQL
                        skipped_sql.push_str(&format!(
                            "-- SKIPPED (with_index_drop=false):\n-- {}\n-- {}\n",
                            drop_sql, create_sql.trim()
                        ));
                    } else {
                        return Err(format!(
                            "Index {} on {}.{} has changed but without_failfast is enabled. SQL would be:\n{}\n{}",
                            actual_index_name, schema, table_name, drop_sql, create_sql.trim()
                        ));
                    }
                }
            }

            // Build the CREATE INDEX statement
            let create_idx = self.build_create_index_sql(
                schema,
                table_name,
                &actual_index_name,
                desired,
            );
            indexes_sql.push_str(&create_idx);

            // Update dbc with the new index
            self.update_dbc(schema, table_name, &actual_index_name, desired, dbc);
        }

        // Log skipped SQL if any
        if !skipped_sql.is_empty() {
            #[cfg(not(feature = "slog"))]
            eprintln!("Skipped index changes:\n{}", skipped_sql);
        }

        Ok(indexes_sql)
    }

    /// Check if the existing index matches the desired configuration
    fn index_matches(&self, desired: &DesiredIndex, existing: &PgIndex) -> bool {
        // Check unique flag
        if desired.is_unique != existing.is_unique {
            return false;
        }

        // Check index method (btree is default)
        let desired_method = if desired.using.is_empty() { "btree" } else { &desired.using };
        if desired_method != existing.index_method {
            return false;
        }

        // Check column count
        if desired.columns.len() != existing.columns.len() {
            return false;
        }

        // Check each column
        for (i, desired_col) in desired.columns.iter().enumerate() {
            let existing_col = &existing.columns[i];

            if desired_col.column_name != existing_col.column_name {
                return false;
            }

            // Check order (default is ASC)
            let desired_order = if desired_col.order.is_empty() { "ASC" } else { &desired_col.order };
            if desired_order != existing_col.order {
                return false;
            }

            // Check nulls order (default depends on sort order: ASC -> LAST, DESC -> FIRST)
            let default_nulls = if desired_order == "DESC" { "FIRST" } else { "LAST" };
            let desired_nulls = if desired_col.nulls.is_empty() { default_nulls } else { &desired_col.nulls };
            if desired_nulls != existing_col.nulls {
                return false;
            }

            // Check collation
            if desired_col.collate != existing_col.collation {
                return false;
            }
        }

        true
    }

    /// Build CREATE INDEX SQL statement
    fn build_create_index_sql(
        &self,
        schema: &str,
        table_name: &str,
        index_name: &str,
        desired: &DesiredIndex,
    ) -> String {
        let mut sql = String::from("CREATE ");

        // UNIQUE
        if desired.is_unique {
            sql.push_str("UNIQUE ");
        }

        sql.push_str("INDEX ");

        // CONCURRENTLY
        if desired.concurrently {
            sql.push_str("CONCURRENTLY ");
        }

        // IF NOT EXISTS and index name
        sql.push_str("IF NOT EXISTS ");
        sql.push_str(index_name);
        sql.push_str(" ON ");
        sql.push_str(schema);
        sql.push('.');
        sql.push_str(table_name);

        // USING method
        if !desired.using.is_empty() {
            sql.push_str(" USING ");
            sql.push_str(&desired.using);
        }

        // Columns
        sql.push_str(" (");
        let col_defs: Vec<String> = desired
            .columns
            .iter()
            .map(|c| {
                let mut col_sql = c.column_name.clone();

                // COLLATE
                if !c.collate.is_empty() {
                    col_sql.push_str(" COLLATE ");
                    col_sql.push_str(&c.collate);
                }

                // ORDER (ASC/DESC)
                if !c.order.is_empty() {
                    col_sql.push(' ');
                    col_sql.push_str(&c.order);
                }

                // NULLS FIRST/LAST
                if !c.nulls.is_empty() {
                    col_sql.push_str(" NULLS ");
                    col_sql.push_str(&c.nulls);
                }

                col_sql
            })
            .collect();
        sql.push_str(&col_defs.join(", "));
        sql.push_str(");\n");

        sql
    }

    /// Update dbc with the new index information
    fn update_dbc(
        &self,
        schema: &str,
        table_name: &str,
        index_name: &str,
        desired: &DesiredIndex,
        dbc: &mut InfoSchemaType,
    ) {
        if let Some(ss) = dbc.get_mut(schema) {
            if let Some(ts) = ss.get_mut(table_name) {
                let columns: Vec<PgIndexColumn> = desired
                    .columns
                    .iter()
                    .map(|c| {
                        let order = if c.order.is_empty() { "ASC".to_string() } else { c.order.clone() };
                        let default_nulls = if order == "DESC" { "FIRST" } else { "LAST" };
                        PgIndexColumn {
                            column_name: c.column_name.clone(),
                            order,
                            nulls: if c.nulls.is_empty() { default_nulls.to_string() } else { c.nulls.clone() },
                            collation: c.collate.clone(),
                        }
                    })
                    .collect();

                ts.indexes.insert(
                    index_name.to_string(),
                    PgIndex {
                        index_name: index_name.to_string(),
                        columns,
                        is_unique: desired.is_unique,
                        index_method: if desired.using.is_empty() {
                            "btree".to_string()
                        } else {
                            desired.using.clone()
                        },
                    },
                );
            }
        }
    }
}
