// use postgres::Transaction;
use serde::Serialize;
use yaml_rust::Yaml;

use crate::loader::InfoSchemaType;
use crate::table::Table;
use crate::utils::{Named, OrderedHashMap};

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
    pub async fn deploy_all_tables(&self, schema: &mut InfoSchemaType, db: &mut tokio_postgres::Transaction<'_>, retry: bool, dry_run: Option<&dyn Fn(Vec<String>) -> Result<(), String>>) -> Result<usize, String> {
        let mut cnt = 0;
        for t in &self.tables.list {
            if t.deploy(schema, db, &self.schema_name, retry, self.file.as_str(), dry_run).await? {
                cnt += 1;
            }
        }
        Ok(cnt)
    }

    #[inline]
    /// return statements to execute
    pub async fn deploy_all_fk(&self, schemas: &OrderedHashMap<Schema>, schema: &mut InfoSchemaType, db: &mut tokio_postgres::Transaction<'_>, retry: bool, dry_run: Option<&dyn Fn(Vec<String>) -> Result<(), String>>) -> Result<usize, String> {
        let mut cnt = 0;
        for t in &self.tables.list {
            if t.deploy_fk(schemas, schema, db, &self.schema_name, retry, self.file.as_str(), dry_run).await? {
                cnt += 1;
            }
        }
        Ok(cnt)
    }

}
