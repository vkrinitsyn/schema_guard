// use postgres::Transaction;
use serde::Serialize;
use yaml_rust::Yaml;

use crate::loader::InfoSchemaType;
use crate::table::Table;
use crate::utils::{Named, OrderedHashMap};
use crate::MigrationOptions;

#[derive(Debug, Clone, Serialize)]
pub struct Schema {
    #[serde(rename = "schemaName")]
    pub schema_name: String,
    pub owner: String,
    /// tableName: table(with name)
    #[serde(with = "crate::table::ytables")]
    pub tables: OrderedHashMap<Table>,
    /// the table definition loaded from file
    #[serde(skip)]
    pub file: String,
}

impl Named for Schema {
    fn get_name(&self) -> String {
        self.schema_name.clone()
    }
}

impl Default for Schema {
    fn default() -> Self {
        Schema {
            schema_name: "".to_string(),
            owner: "".to_string(),
            tables: OrderedHashMap::new(),
            file: "".to_string(),
        }
    }
}

impl Schema {
    #[inline]
    pub fn schema_name(input: &Yaml) -> String {
        match input["schemaName"].as_str() {
            None => "public".into(),
            Some(s) => s.into(),
        }
    }

    #[inline]
    pub fn new(input: &Yaml, file: &str) -> Self {
        Schema {
            schema_name: Schema::schema_name(input),
            owner: crate::utils::as_str(input, "schemaName", ""),
            tables: OrderedHashMap::new(),
            file: file.to_string(),
        }
    }

    #[inline]
    pub fn append(&mut self, input: &Yaml) -> Result<(), String> {
        if let Some(tbls) = input["tables"].as_vec() {
            for t in tbls {
                let t = &t["table"];
                match t["tableName"].as_str() {
                    None => {
                        return Err(format!("no table name set in file: {}", self.file));
                    }
                    Some(tn) => {
                        if self.tables.map.contains_key(tn) {
                            return Err(format!(
                                "duplicate table definition: {} found in file: {}",
                                tn, self.file
                            ));
                        } else {
                            let _ = self.tables.append(Table::new(
                                t,
                                tn,
                                Some(&self.file),
                            )?);
                        }
                    }
                }
            }
        }
        Ok(())
    }

    #[inline]
    /// return statements to execute
    pub async fn deploy_all_tables(&self, schema: &mut InfoSchemaType,
                                   db: &mut tokio_postgres::Transaction<'_>,
                                   dry_run: Option<&(dyn Fn(Vec<String>) -> Result<(), String> + Send + Sync)>,
                                   opt: &MigrationOptions) -> Result<usize, String> {
        let mut cnt = 0;
        for t in &self.tables.list {
            // Skip template tables - they are not created in DB
            if t.is_template() {
                continue;
            }
            if t.deploy(schema, db, &self.schema_name, self.file.as_str(), dry_run, opt).await? {
                cnt += 1;
            }
        }
        Ok(cnt)
    }

    #[inline]
    /// return statements to execute
    pub async fn deploy_all_fk(&self, schemas: &OrderedHashMap<Schema>,
                               schema: &mut InfoSchemaType,
                               db: &mut tokio_postgres::Transaction<'_>,
                               retry: bool,
                               dry_run: Option<&(dyn Fn(Vec<String>) -> Result<(), String> + Send + Sync)>) -> Result<usize, String> {
        let mut cnt = 0;
        for t in &self.tables.list {
            // Skip template tables - they are not created in DB
            if t.is_template() {
                continue;
            }
            if t.deploy_fk(schemas, schema, db, &self.schema_name, retry, self.file.as_str(), dry_run).await? {
                cnt += 1;
            }
        }
        Ok(cnt)
    }

    /// Resolve templates for all tables in this schema
    /// Templates are referenced by name: "schema.table" or just "table" for same schema
    pub fn resolve_templates(&mut self, all_schemas: &OrderedHashMap<Schema>) -> Result<(), String> {
        // Collect tables that need template resolution
        let table_names: Vec<String> = self.tables.list.iter()
            .filter(|t| !t.use_templates.is_empty())
            .map(|t| t.table_name.clone())
            .collect();

        for table_name in table_names {
            let use_templates = if let Some(t) = self.tables.get(&table_name) {
                t.use_templates.clone()
            } else {
                continue;
            };

            // Collect all templates to merge
            let mut templates_to_merge: Vec<Table> = Vec::new();
            for template_ref in &use_templates {
                let (template_schema, template_table) = if template_ref.contains('.') {
                    let parts: Vec<&str> = template_ref.splitn(2, '.').collect();
                    (parts[0].to_string(), parts[1].to_string())
                } else {
                    (self.schema_name.clone(), template_ref.clone())
                };

                // Find the template table
                let template = if template_schema == self.schema_name {
                    self.tables.get(&template_table).cloned()
                } else {
                    all_schemas.get(&template_schema)
                        .and_then(|s| s.tables.get(&template_table).cloned())
                };

                match template {
                    Some(t) => {
                        if !t.is_template() {
                            return Err(format!(
                                "Table {}.{} references '{}' as template, but it is not marked as template: true",
                                self.schema_name, table_name, template_ref
                            ));
                        }
                        templates_to_merge.push(t);
                    }
                    None => {
                        return Err(format!(
                            "Table {}.{} references template '{}' which does not exist",
                            self.schema_name, table_name, template_ref
                        ));
                    }
                }
            }

            // Apply templates in order
            if let Some(table) = self.tables.get_mut(&table_name) {
                for template in templates_to_merge {
                    table.merge_from(&template);
                }
            }
        }

        Ok(())
    }
}
