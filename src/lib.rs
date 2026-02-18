#[cfg(feature = "slog")]
#[macro_use] extern crate slog;
extern crate yaml_rust;

use std::convert::TryFrom;
use std::fs;
use std::result::Result;
use std::str::FromStr;
use std::sync::{Arc, RwLock};

use lazy_static::lazy_static;


use slog::Logger;
use yaml_rust::YamlLoader;
use yaml_validator::Validate;

use schema::Schema;

use crate::loader::load_info_schema;
use crate::utils::OrderedHashMap;

use self::yaml_rust::Yaml;

pub mod loader;
pub mod table;
pub mod column;
pub mod index;
pub mod grant;
pub mod schema;
pub mod utils;

static SCHEMA_YAML: &'static str = include_str!("schema.yaml");

lazy_static! {
    pub(crate) static ref LOG: Arc<RwLock<Option<Logger>>> = Arc::new (RwLock::new(None));

    pub(crate) static ref SCHEMA_YAMLS: Vec<Yaml> = YamlLoader::load_from_str(SCHEMA_YAML).expect("wrong schema");
}

#[cfg(feature = "slog")]
pub fn set_logger(logger: Logger) {
    let mut log = LOG.write().unwrap();
    *log = Some(logger);
}

#[cfg(feature = "slog")]
pub(crate) fn log_debug(prefix: &str, msg: &String, file: &str, schema: &String) {
    let log = LOG.read().unwrap();
    if let Some(l) = &*log {
        debug!(l, "--{} {}[{}] {}", prefix, file, schema, msg);
    } else {
        println!("{} {}[{}] {}", prefix, file, schema, msg);
    }
}

#[derive(Debug, Clone, Default)]
/// Migration option for column types, indexes, triggers and grant/revoke access.
/// By default, all false - return exception on potentially destructive changes.
/// <li> Use without_failfast=true, to log those SQL without execution
///
pub struct MigrationOptions {
    /// if column type identified less than perform alter, size extend will perform anyway, otherwise ignore changes, also see failfast flag
    /// <li> default: false
    pub with_size_cut: bool,
    /// if index change detected, then perform drop before update, otherwise ignore changes, also see failfast flag
    /// <li> default: false
    pub with_index_drop: bool,
    /// if trigger change detected, then perform drop before update, otherwise ignore changes, also see failfast flag
    /// <li> default: false
    pub with_trigger_drop: bool,
    /// if access(grant) change detected, then perform revoke, otherwise ignore changes, also see failfast flag
    /// <li> default: false
    pub with_revoke: bool,
    /// if changes detected AND not set to 'true' for apply, then ignore changes and show generated skipped SQL,
    /// otherwise return exception (default)
    /// <li> default: false - return exception if no "with_xxx" and changes detected
    pub without_failfast: bool,
}

pub fn get_schema() -> Vec<Yaml> {
    SCHEMA_YAMLS.clone()
}


/// Migrate one yaml schema file with options
pub async fn migrate1(schema: Yaml, db_url: &str) -> Result<usize, String> {
    let config = tokio_postgres::config::Config::from_str(db_url)
        .map_err(|e| format!("parse db_url: {e}"))?;
    let (mut client, conn) = config.connect(tokio_postgres::NoTls)
        .await.map_err(|e| format!("connect: {e}"))?;
    tokio::spawn(async move { let _ = conn.await; });
    let mut tx = client.transaction()
        .await.map_err(|e| format!("transaction: {e}"))?;
    let cnt = migrate(schema, &mut tx, true, None, "", &MigrationOptions::default()).await?;
    tx.commit().await.map_err(|e| format!("commit: {e}"))?;
    Ok(cnt)
}

/// Migrate one yaml schema file with options
pub async fn migrate_opt(schema: Yaml, db_url: &str, opt: &MigrationOptions) -> Result<usize, String> {
    let config = tokio_postgres::config::Config::from_str(db_url)
        .map_err(|e| format!("parse db_url: {e}"))?;
    let (mut client, conn) = config.connect(tokio_postgres::NoTls)
        .await.map_err(|e| format!("connect: {e}"))?;
    tokio::spawn(async move { let _ = conn.await; });
    let mut tx = client.transaction()
        .await.map_err(|e| format!("transaction: {e}"))?;
    let cnt = migrate(schema, &mut tx, true, None, "", &opt).await?;
    tx.commit().await.map_err(|e| format!("commit: {e}"))?;
    Ok(cnt)
}

/// main entry point to apply schema from yaml to the database
/// return statements to execute
///
pub async fn migrate(schema: Yaml, db: &mut tokio_postgres::Transaction<'_>, retry: bool,
               dry_run: Option<&(dyn Fn(Vec<String>) -> Result<(), String> + Sync + Send)>, file_name: &str,
               opt: &MigrationOptions
) -> Result<usize, String> {
    // let mut db = dbc.transaction().map_err(|e| format!("{}", e))?;
    let mut cnt = 0;
    // check db connection
    let db_name: String = db.query("select current_database()", &[])
        .await.map_err(|e| format!("DB connection error: {}", e))?[0].get(0);
    // load schema
    let mut info = load_info_schema(db_name.as_str(), db).await?;
    let schemas = parse_yaml_schema(schema, file_name)?;
    for s in &schemas.list {
        cnt += s.deploy_all_tables(&mut info, db, retry, dry_run, opt).await?;
    }

    for s in &schemas.list {
        cnt += s.deploy_all_fk(&schemas, &mut info, db, retry, dry_run).await?;
    }

    // let _ = db.commit().map_err(|e| format!("committing error: {}", e))?;
    Ok(cnt)
}


pub fn load_schema_from_file(filename_yaml: &str) -> Result<Yaml, String> {
    match fs::read_to_string(filename_yaml) {
        Ok(data) => load_schema_from_src(data),
        Err(e) => Err(format!("load error [{}]: {}", filename_yaml, e))
    }
}


pub fn load_schema_from_src(data: String) -> Result<Yaml, String> {
     match YamlLoader::load_from_str(data.as_str()) {
        Ok(mut y) => {
            let doc = y.remove(0);
            let context = yaml_validator::Context::try_from(&crate::SCHEMA_YAMLS[..])
                .map_err(|e| format!("correct schema.yaml: context {}", e))?;

            let yts = match context.get_schema("database") {
                None => { return Err("correct schema.yaml: database".to_string()); }
                Some(y) => y,
            };
            let _ = yts.validate(&context, &doc)
                .map_err(|err| {format!("Schema validation error: {}", err)})?;
            Ok(doc)
        },
        Err(e) => Err(format!("parsing error: {} ", e)),
    }
}

/// filename is for logging reference only
pub fn parse_yaml_schema(yaml: Yaml, file_name: &str) -> Result<OrderedHashMap<Schema>, String> {
    match yaml["database"].as_vec() {
        None => Err("empty file".to_string()),
        Some(schemas) => {
            let mut schema_schemas = OrderedHashMap::new();

            for s in schemas {
                match schema_schemas.get_mut(&Schema::schema_name(s)) {
                    None => {
                        let mut ss = Schema::new(&s, file_name);
                        let _ = ss.append(s)?;
                        let _ = schema_schemas.append(ss);
                    }
                    Some(ss) => ss.append(s)?
                }
            }

            // Resolve templates for all schemas
            // First pass: collect all schemas for cross-schema template references
            let schemas_snapshot = schema_schemas.clone();
            for schema in &mut schema_schemas.list {
                schema.resolve_templates(&schemas_snapshot)?;
            }

            Ok(schema_schemas)
        }
    }
}



#[cfg(test)]
mod tests {
    use crate::{load_schema_from_file, parse_yaml_schema, MigrationOptions};
    #[tokio::test]
    async fn test_schema() {

        let r = parse_yaml_schema(load_schema_from_file("tests/example.yaml").unwrap(), "").unwrap();
        assert_eq!(r.len(), 1);
        let t = r.list.get(0).unwrap().tables.list.get(0).unwrap();
        assert_eq!(t.grant.len(), 1);
        assert_eq!(t.grant.get(0).unwrap().all.as_str(), "postgres");
        assert_eq!(t.grant.get(0).unwrap().by.as_str(), "");
        assert_eq!(t.data.len(), 2);
        assert!(t.data_file.is_some());
        assert_eq!(t.data_file.as_ref().unwrap_or(&"".to_string()).as_str(), "data.cvs");
        let row = t.data.get(0).unwrap();
        assert_eq!(row.len(), 2);
        assert_eq!(row[0].as_str(), "1");
        assert_eq!(row[1].as_str(), "test1");
        let row = t.data.get(1).unwrap();
        assert_eq!(row.len(), 2);
        assert_eq!(row[0].as_str(), "2");
        assert_eq!(row[1].as_str(), "test2");
        let i = t.columns.list.get(0).unwrap().index.as_ref();
        assert!(i.is_some());
        assert_eq!(i.unwrap().name.as_str(), "pk");
        assert_eq!(i.unwrap().sql.as_str(), "");

    }

    #[test]
    fn test_schema_opt() {
        let m = MigrationOptions::default();
        assert!(!m.without_failfast);
        assert!(!m.with_index_drop);
        assert!(!m.with_revoke);
        assert!(!m.with_trigger_drop);
    }
}
