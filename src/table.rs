use std::collections::{HashMap, HashSet};
use std::fmt::Write;

use serde::Serialize;
use yaml_rust::Yaml;
use yaml_rust::yaml::Array;

use crate::column::{Column, Trig};
use crate::loader::{FKTable, InfoSchemaType, PgTable};
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
                        /*
                        match file {
                            None => {
                                // println!("{}:\n{:?}\n{:?}", _name, yc, c);
                            }
                            Some(_f) => {
                                if let Some(log) = log {
                                    trace!(log, "+\n{:?}\n{:?}", yc, c);
                                }
                            }
                        }
                         */
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
        let etl = &input["data_file"];
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
                // match etl.as_str() {
                //     Some(s) => Some(s.to_string()),
                //     None => None,
                // }
                etl.as_str().map(|s| s.to_string())
            },
            data: crate::utils::as_vec(input, "data"),
            owner: crate::utils::as_str(input, "owner", ""),
            grant: YGrant::new(input["grant"].as_vec()),
        })
    }


    /// build a create or alter sql
    #[allow(unused_mut)]
    pub async fn deploy(
        &self,
        dbc: &mut InfoSchemaType,
        db: &mut tokio_postgres::Transaction<'_>,
        schema: &String, // this
        is_retry: bool,
        file: &str,
        dry_run: Option<&dyn Fn(Vec<String>) -> Result<(), String>>,
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
                    for dc in &self.columns.list {
                        if !ts.columns.contains_key(&dc.name) {
                            let def = dc.column_def(schema, &self.table_name, file)?;
                            append(format!(
                                "ALTER TABLE {}.{} ADD COLUMN {}",
                                schema, self.table_name, def.def(pks.is_some())
                            ).as_str(), &mut sql, is_retry);
                            let _ = ts.columns.insert(dc.get_name(), def);
                            exec = true;
                        }
                    }
                    if let Some(o) = &ts.owner {
                        if self.owner.len() > 0 && &self.owner != o {
                            append(format!("ALTER TABLE {}.{} OWNER TO {}",
                                           schema, self.table_name, self.owner
                            ).as_str(), &mut sql, is_retry);
                        }
                    }
                    for dt in &self.triggers.list {
                        if !ts.triggers.contains_key(&dt.name) {
                            if let Some(def) = dt.trig_def(schema, &self.table_name) {
                                let _ = writeln!(sql, "{}\n", def);
                                let _ = ts.triggers.insert(dt.get_name(), def);
                                exec = true;
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
            let mut st = PgTable {
                table_name: self.table_name.clone(),
                columns: HashMap::new(),
                fks: Default::default(),
                triggers: HashMap::new(),
                sort_order: 0,
                table_comment: None,
                owner: if self.owner.len() > 0 { Some(self.owner.clone()) } else { None },
            };

            for dc in &self.columns.list {
                let cd = dc.column_def(schema, &self.table_name, file)?;
                // let _ = write!(columns, "{}, ", cd.def(true));
                let _ = st.columns.insert(dc.get_name(), cd);
                self.comments(&mut comments, schema, &dc.name, &dc.description);
            }

            // if columns.len() > 0 {
            let pks = st.pks();
            for dc in &self.columns.list {
                if let Some(cd) = st.columns.get(dc.name.as_str()) {
                    columns.push_str(cd.def(pks.is_some()).as_str());
                    columns.push_str(", ");
                }
            }
            if let Some(pks) = pks {
                columns.push_str(pks.as_str());
                columns.push_str(", ");
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
            let csql = format!("CREATE TABLE {}.{} ({}{}{}){}; \n",
                               schema,
                               self.table_name,
                               columns,
                               if self.constraint.len() > 0 { ", " } else { "" },
                               self.constraint,
                               self.sql
            );

            sql.push_str(csql.as_str());

            if self.owner.len() > 0 {
                append(format!(
                    "ALTER TABLE {}.{} OWNER TO {}",
                    schema, self.table_name, self.owner
                ).as_str(), &mut sql, is_retry);
            }
            // }
            for dt in &self.triggers.list {
                if let Some(td) = dt.trig_def(schema, &self.table_name) {
                    let _ = writeln!(sql, "{}\n", td);
                    st.triggers.insert(dt.get_name(), td);
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
        let mut data = String::new();
        for row in &self.data {
            self.insert(&mut data, row, schema);
        }

        match dry_run {
            Some(store) => {
                store(vec![sql, comments, data]).map(|_| false)
            }
            None => {
                #[cfg(feature = "slog")] log_debug(format!("deploy SQL {:?}[{}:{}]> {}", exec, file, schema, sql));
                if exec {
                    let source = if file.len() > 0 { format!(", source: {}", file)} else {"".to_string()};
                    let _ = db.batch_execute(sql.as_str()).await
                        .map_err(|e| format!("DB execute [{}]: {} {}", sql, e, source))?;
                    let _ = db.batch_execute(comments.as_str()).await
                        .map_err(|e| format!("DB execute [{}]: {} {}", comments, e, source))?;
                    let _ = db.batch_execute(data.as_str()).await
                        .map_err(|e| format!("DB execute [{}]: {} {}", data, e, source))?;
                }
                Ok(exec)
            }
        }
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
        dry_run: Option<&dyn Fn(Vec<String>) -> Result<(), String>>,
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
                "ALTER TABLE {}.{} ADD CONSTRAINT fk_{}_{}_{} FOREIGN KEY ({}) REFERENCES {}.{} ({}) {}",
                schema, self.table_name, schema, self.table_name, ff.table,
                ff.name, ff.schema, &ff.table, ff.columns(), ff.sql
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
