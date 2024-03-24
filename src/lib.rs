#[cfg(feature = "slog")]
#[macro_use] extern crate slog;
extern crate yaml_rust;

use std::convert::TryFrom;
use std::fs;
use std::result::Result;
use std::sync::{Arc, RwLock};

use lazy_static::lazy_static;
use postgres::Client;


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
pub(crate) fn log_debug(msg: String) {
    let log = LOG.read().unwrap();
    if let Some(l) = &*log {
        debug!(l, "{}", msg);
    } else {
        println!("{}", msg);
    }
}

pub fn get_schema() -> Vec<Yaml> {
    SCHEMA_YAMLS.clone()
}


/// simplified migrate
pub fn migrate1(schema: Yaml, db: &mut Client) -> Result<usize, String> {
    migrate(schema, db, false, None::<&dyn Fn(Vec<String>) -> Result<(), String>>, "")
}

/// main entry point to apply schema from yaml to the database
/// return statements to execute
///
pub fn migrate(schema: Yaml, dbc: &mut Client, retry: bool,
               dry_run: Option<&dyn Fn(Vec<String>) -> Result<(), String>>, file_name: &str
) -> Result<usize, String> {
    let mut db = dbc.transaction().map_err(|e| format!("{}", e))?;
    let mut cnt = 0;
    // check db connection
    let db_name: String = db.query("select current_database()", &[])
        .map_err(|e| format!("DB connection error: {}", e))?[0].get(0);
    // load schema
    let mut info = load_info_schema(db_name.as_str(), &mut db)?;
    let schemas = parse_yaml_schema(schema, file_name)?;
    for s in &schemas.list {
        cnt += s.deploy_all_tables(&mut info, &mut db, retry, dry_run)?;
    }

    for s in &schemas.list {
        cnt += s.deploy_all_fk(&schemas, &mut info, &mut db, retry, dry_run)?;
    }

    let _ = db.commit().map_err(|e| format!("committing error: {}", e))?;
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
            Ok(schema_schemas)
        }
    }
}



#[cfg(test)]
mod tests {
    use crate::{load_schema_from_file, parse_yaml_schema};

    #[test]
    fn test_schema() {

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

}
