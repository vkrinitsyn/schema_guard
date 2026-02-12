// extern crate postgres;

use std::collections::{BTreeMap, HashMap, HashSet};
use std::result::Result;

// use postgres::Transaction;
use serde::Serialize;

use crate::utils::{Named, OrderedHashMap};

/// information schema types: schema, table, column
// BTreeMap use to keep dump schema order
pub type InfoSchemaType = BTreeMap<String, HashMap<String, PgTable>>;
pub type InfoSchemaTypeS = OrderedHashMap<OrderedHashMap<PgTable>>;

/// information schema types: schema, (owner, table: name: owner)
pub type InfoSchemaOwnerType = HashMap<String, (String, HashMap<String, String>)>;

#[derive(Debug, Clone, Serialize)]
/// information schema data
pub struct PgSchema {
    pub schema_name: String,
    pub tables: OrderedHashMap<PgTable>,
}

impl Named for PgSchema {
    fn get_name(&self) -> String {
        self.schema_name.clone()
    }
}

impl Named for PgTable {
    fn get_name(&self) -> String {
        self.table_name.clone()
    }
}

#[derive(Debug, Clone, Serialize)]
/// information schema data
pub struct PgTable {
    pub table_name: String,
    /// column name,
    pub columns: HashMap<String, PgColumnDfn>,
    /// column name, foreign schema, table, column, fk name
    pub fks: HashMap<String, FKTable>,
    /// trigger name -> PgTrigger
    pub triggers: HashMap<String, PgTrigger>,
    /// index name -> PgIndex
    pub indexes: HashMap<String, PgIndex>,
    /// grantee -> PgGrant
    pub grants: HashMap<String, PgGrant>,
    /// Primary key columns in order, with constraint name
    pub primary_key: Option<PgPrimaryKey>,
    pub sort_order: usize,
    pub table_comment: Option<String>,
    pub owner: Option<String>,
}

/// Primary key information loaded from DB
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct PgPrimaryKey {
    pub constraint_name: String,
    /// Column names in order
    pub columns: Vec<String>,
}

const _PRIVILEGES: [&str; 14] = [
    "SELECT",
    "INSERT",
    "UPDATE",
    "DELETE",
    "TRUNCATE",
    "REFERENCES",
    "TRIGGER",
    "CREATE",
    "CONNECT",
    "TEMPORARY",
    "EXECUTE",
    "USAGE",
    "SET",
    "ALTER SYSTEM"
];

/// information schema column data
#[derive(Debug, Clone, Serialize)]
pub struct PgColumnDfn {
    pub column_name: String,
    pub column_type: String,
    pub column_default: Option<String>,
    pub sql: Option<String>,
    pub fk: Option<(String, String)>,
    pub pk: bool,
    pub nullable: bool,
    pub sort_order: usize,
    pub column_comment: Option<String>,
    // pub grant: list<String>,
}

/// FK information loaded from DB
#[derive(Debug, Clone, Serialize)]
pub struct FKTable {
    pub schema: String,
    pub table: String,
    pub column: HashSet<String>,
    pub name: String,
    pub sql: String,
}

/// Index column information loaded from DB
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct PgIndexColumn {
    pub column_name: String,
    pub order: String,        // ASC or DESC
    pub nulls: String,        // FIRST or LAST
    pub collation: String,    // collation name or empty
}

/// Index information loaded from DB
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct PgIndex {
    pub index_name: String,
    pub columns: Vec<PgIndexColumn>,
    pub is_unique: bool,
    pub index_method: String,  // btree, hash, gist, etc.
}

/// Trigger information loaded from DB
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct PgTrigger {
    pub trigger_name: String,
    pub event: String,         // BEFORE INSERT, AFTER UPDATE, etc.
    pub orientation: String,   // FOR EACH ROW, FOR EACH STATEMENT
    pub proc: String,          // function name with schema
}

/// Grant information loaded from DB
#[derive(Debug, Clone, Serialize, PartialEq, Default)]
pub struct PgGrant {
    pub grantee: String,
    pub privileges: HashSet<String>,  // SELECT, INSERT, UPDATE, DELETE, etc.
    pub with_grant_option: bool,
}

impl FKTable {
    pub(crate) fn columns(&self) -> String {
        let mut cs = String::new();
        for c in &self.column {
            if cs.len() > 0 {
                cs.push_str(", ");
            }
            cs.push_str(c.as_str())
        }
        cs
    }
}

impl PgColumnDfn {
    fn new(column_name: &str, column_type: String, column_def: &str, nullable: bool, sort_order: usize) -> Self {
        PgColumnDfn {
            column_name: column_name.into(),
            column_type,
            column_default: if column_def.len() > 0 { Some(column_def.into()) } else { None },
            sql: None,
            fk: None,
            pk: false,
            nullable,
            sort_order,
            column_comment: None,
        }
    }

    pub(crate) fn def(&self, ignore_pk: bool) -> String {
        let mut sql = format!("{} {}", self.column_name, self.column_type);
        if self.pk && !ignore_pk {
            sql.push_str(" primary key");
        }
        if !self.nullable {
            sql.push_str(" not null");
        }
        if let Some(def) = &self.column_default {
            if def.len() > 0 {
                sql.push_str(" default ");
                sql.push_str(def.as_str());
            }
        }
        if let Some(ssql) = &self.sql {
            if ssql.len() > 0 {
                sql.push_str(" ");
                sql.push_str(ssql.as_str());
            }
        }
        sql
    }
}


pub fn load_info_schema(db_name: &str, db: &mut Transaction) -> Result<InfoSchemaType, String> {
    let mut data = load_info_cc(db_name, db)?;
    let _ = load_info_fk(db_name, db, &mut data)?;
    let _ = load_info_tg(db_name, db, &mut data)?;
    Ok(data)
}


// SELECT table_catalog, table_schema, table_name, column_name, column_default, is_nullable, data_type, udt_name, character_maximum_length, numeric_precision, numeric_scale, ordinal_position from information_schema.columns where table_schema not in ('pg_catalog', 'information_schema') and table_name = table_catalog = $1
#[inline]
async fn load_info_cc(db_name: &str, db: &mut tokio_postgres::Transaction<'_>) -> Result<InfoSchemaType, String> {
    let mut data: InfoSchemaType = Default::default();
    let result = db.query("SELECT table_catalog, table_schema, table_name, column_name, column_default, is_nullable, \
    data_type, udt_name, character_maximum_length, numeric_precision, numeric_scale, ordinal_position \
     from information_schema.columns where table_schema not in ('pg_catalog', 'information_schema') and table_catalog = $1 \
      order by 1,2,3, ordinal_position", &[&db_name])
        .await
        .map_err(|e| format!("on loading information_schema [{}]: {}", db_name, e))?;
    let mut sort_order = 0;
    for r in result {
        sort_order += 1;
        let _table_catalog: &str = r.get(0);
        let table_schema: &str = r.get(1);
        let table_name: &str = r.get(2);
        let column_name: &str = r.get(3);
        let column_default: Option<&str> = r.get(4);
        let nullable: &str = r.get(5);
        let data_type: &str = r.get(6);
        let udt_name: &str = r.get(7);
        let character_maximum_length: Option<i32> = r.get(8);
        let numeric_precision: Option<i32> = r.get(9);
        let numeric_scale: Option<i32> = r.get(10);
        let mut data_type = if udt_name.len() == 0 { data_type.to_string() } else { udt_name.to_string() };
        if data_type.to_lowercase().as_str() == "varchar" {
            if let Some(varchar_len) = character_maximum_length {
                data_type.push_str(format!("({})", varchar_len).as_str());
            }
        } else {
            if let Some(numeric) = numeric_precision {
                if let Some(scale) = numeric_scale {
                    if scale > 0 {
                        data_type = format!("NUMERIC({}, {})", numeric, scale);
                    }
                }
            }
        }
        #[cfg(debug_assertions)]
        {
            if column_name == "id" {
                // println!("{}.id= {}", table_name, column_default.unwrap_or("NA"));
            }
        }
        let column_data = PgColumnDfn::new(column_name, data_type,
                                           column_default.unwrap_or(""), nullable.to_lowercase() == "yes", sort_order);
        match data.get_mut(table_schema) {
            None => {
                let mut hd = HashMap::new();
                hd.insert(table_name.into(), PgTable::new(table_name, column_name, column_data, sort_order));
                data.insert(table_schema.into(), hd);
            }
            Some(s) => {
                match s.get_mut(table_name) {
                    None => {
                        s.insert(table_name.into(), PgTable::new(table_name, column_name, column_data, sort_order));
                    }
                    Some(hd) => {
                        hd.columns.insert(column_name.into(), column_data);
                    }
                }
            }
        }
    }

    for (schema, tbls) in &mut data {
        let mut query = String::new();
        let mut tables = String::new();
        for tn in tbls.keys() {
            if !query.is_empty() {
                query.push(',');
                tables.push(',');
            }
            query.push_str(format!("'{}.{}'::regclass", schema, tn).as_str());
            tables.push_str(format!("'{}.{}'", schema, tn).as_str());
        }
        if !query.is_empty() {
            let result = db.query(format!("SELECT * from
(SELECT tabs.table_schema, tabs.table_name,
    pg_catalog.obj_description(tabs.table_name::regclass::oid) as table_comment
    FROM information_schema.tables tabs
    WHERE tabs.table_schema not in ('pg_catalog', 'information_schema') AND tabs.table_catalog = $1
     and tabs.table_name in ({})
    ) as ist WHERE ist.table_comment is not null order by 1,2", tables).as_str(), &[&db_name]).await
                .map_err(|e| format!("on loading table_comment from information_schema [{}]: {}", db_name, e))?;
            for r in result {
                // let table_schema: &str = r.get(0);
                let table_name: &str = r.get(1);
                let table_comment: &str = r.get(2);
                if let Some(t) = tbls.get_mut(table_name) {
                    t.table_comment = Some(table_comment.to_string());
                }
            }

            let result = db.query("SELECT schemaname, tablename, tableowner from pg_tables where schemaname = $1 ",
                                  &[&schema]).await
                .map_err(|e| format!("on loading table_owner from information_schema [{}]: {}", db_name, e))?;
            for r in result {
                // let table_schema: &str = r.get(0);
                let table_name: &str = r.get(1);
                let table_owner: &str = r.get(2);
                if let Some(t) = tbls.get_mut(table_name) {
                    t.owner = Some(table_owner.to_string());
                }
            }

            let result = db.query(format!("select * from
(SELECT cols.table_schema, cols.table_name, cols.column_name, pg_catalog.col_description(cols.table_name::regclass::oid, cols.ordinal_position::int) as column_comment
FROM information_schema.columns cols
WHERE cols.table_schema not in ('pg_catalog', 'information_schema')  AND cols.table_catalog = $1
 AND cols.table_name in ({})
) as iss where iss.column_comment is not null", tables).as_str(), &[&db_name]).await
                .map_err(|e| format!("on loading table_comment from information_schema [{}]: {}", db_name, e))?;
            for r in result {
                // let table_schema: &str = r.get(0);
                let table_name: &str = r.get(1);
                let column_name: &str = r.get(2);
                let column_comment: &str = r.get(3);
                if let Some(t) = tbls.get_mut(table_name) {
                    if let Some(c) = t.columns.get_mut(column_name) {
                        c.column_comment = Some(column_comment.to_string());
                    }
                }
            }

            let result = db.query(format!("SELECT relname, a.attname, indisprimary, indisunique
                    FROM pg_index i
                    JOIN pg_class pc on pc.oid = i.indrelid
                    JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                    WHERE (i.indisprimary or i.indisunique) and i.indrelid in ({})", query).as_str(), &[]).await
                .map_err(|e| format!("on loading information_schema pk/uniq: {}", e))?;
            for r in result {
                let table_name: &str = r.get(0);
                let col_name: &str = r.get(1);
                let indisprimary: bool = r.get(2);
                let indisunique: bool = r.get(3);
                if let Some(st) = tbls.get_mut(table_name) {
                    if let Some(ct) = st.columns.get_mut(col_name) {
                        if indisprimary {
                            ct.pk = true;
                            if let Some(cd) = &ct.column_default {
                                let seq = format!("{}_id_seq'::regclass)", table_name);
                                if !ct.nullable && ct.column_type.starts_with("int")
                                    && cd.starts_with("nextval('")
                                    && cd.ends_with(seq.as_str()) {
                                    ct.column_default = None;
                                    ct.column_type =
                                        if ct.column_type.as_str() == "int4" {
                                            "serial"
                                        } else {
                                            "bigserial"
                                        }.to_string();
                                }
                            }
                        }

                        if indisunique {
                            ct.sql = Some("UNIQUE".into());
                        }
                    }
                }
            }
        }
    }


    Ok(data)
}

#[inline]
async fn load_info_tg(_db_name: &str, db: &mut tokio_postgres::Transaction<'_>, data: &mut InfoSchemaType) -> Result<(), String> {
    // Query triggers with full details from pg_trigger
    let result = db.query(
        "SELECT
            n.nspname as schema_name,
            c.relname as table_name,
            t.tgname as trigger_name,
            CASE
                WHEN t.tgtype & 2 = 2 THEN 'BEFORE'
                WHEN t.tgtype & 64 = 64 THEN 'INSTEAD OF'
                ELSE 'AFTER'
            END ||
            CASE WHEN t.tgtype & 4 = 4 THEN ' INSERT' ELSE '' END ||
            CASE WHEN t.tgtype & 8 = 8 THEN ' DELETE' ELSE '' END ||
            CASE WHEN t.tgtype & 16 = 16 THEN ' UPDATE' ELSE '' END ||
            CASE WHEN t.tgtype & 32 = 32 THEN ' TRUNCATE' ELSE '' END as event,
            CASE WHEN t.tgtype & 1 = 1 THEN 'FOR EACH ROW' ELSE 'FOR EACH STATEMENT' END as orientation,
            pn.nspname || '.' || p.proname || '()' as proc_name
         FROM pg_trigger t
         JOIN pg_class c ON c.oid = t.tgrelid
         JOIN pg_namespace n ON n.oid = c.relnamespace
         JOIN pg_proc p ON p.oid = t.tgfoid
         JOIN pg_namespace pn ON pn.oid = p.pronamespace
         WHERE NOT t.tgisinternal
           AND n.nspname NOT IN ('pg_catalog', 'information_schema')
         ORDER BY n.nspname, c.relname, t.tgname",
        &[]
    ).await.map_err(|e| format!("on loading pg_trigger: {}", e))?;

    for r in result {
        let schema_name: &str = r.get(0);
        let table_name: &str = r.get(1);
        let trigger_name: &str = r.get(2);
        let event: &str = r.get(3);
        let orientation: &str = r.get(4);
        let proc_name: &str = r.get(5);

        let trigger = PgTrigger {
            trigger_name: trigger_name.to_string(),
            event: event.trim().to_string(),
            orientation: orientation.to_string(),
            proc: proc_name.to_string(),
        };

        if let Some(schema_tables) = data.get_mut(schema_name) {
            if let Some(table) = schema_tables.get_mut(table_name) {
                table.triggers.insert(trigger_name.to_string(), trigger);
            }
        }
    }
    Ok(())
}

const NO_ACTION: &str = "NO ACTION";

#[inline]
// db: &mut Transaction,
// db: &mut Client
async fn load_info_fk(db_name: &str, db: &mut tokio_postgres::Transaction<'_>, data: &mut InfoSchemaType) -> Result<(), String> {
    match db.query("SELECT tc.table_schema,  tc.table_name, kcu.column_name,
 ccu.table_schema AS foreign_schema_name, ccu.table_name AS foreign_table_name, ccu.column_name AS foreign_column_name, tc.constraint_name,
 rc.match_option, rc.update_rule, rc.delete_rule
 FROM information_schema.table_constraints AS tc
 JOIN information_schema.key_column_usage AS kcu ON tc.constraint_name = kcu.constraint_name
 JOIN information_schema.constraint_column_usage AS ccu ON ccu.constraint_name = tc.constraint_name
 join information_schema.referential_constraints as rc on tc.constraint_name = rc.constraint_name
 WHERE constraint_type = 'FOREIGN KEY' and tc.table_catalog = $1", &[&db_name]).await {
        Err(e) => Err(format!("on loading information_schema.fk: {}", e)),
        Ok(result) => {
            for r in result {
                let table_schema: &str = r.get(0);
                let table_name: &str = r.get(1);
                let column_name: &str = r.get(2);
                let foreign_schema_name: &str = r.get(3);
                let foreign_table_name: &str = r.get(4);
                let foreign_column_name: &str = r.get(5);
                let constraint_name: &str = r.get(6);
                let _match_option: &str = r.get(7);
                let update_rule: &str = r.get(8);
                let delete_rule: &str = r.get(9);
                let sql = if update_rule == NO_ACTION && delete_rule == NO_ACTION {
                    "".to_string()
                } else {
                    format!("ON UPDATE {} ON DELETE {}", update_rule, delete_rule)
                };
                if let Some(s) = data.get_mut(table_schema) {
                    if let Some(hd) = s.get_mut(table_name) {
                        if let Some(column) = hd.columns.get_mut(column_name) {
                            column.fk = Some((format!("{}.{}", foreign_schema_name, foreign_table_name),
                                              sql.clone()
                            ));
                        }
                        let constraint_name = constraint_name.to_string();

                        match hd.fks.get_mut(&constraint_name) {
                            None => {
                                let mut column = HashSet::new();
                                column.insert(foreign_column_name.to_string());
                                hd.fks.insert(column_name.into(), FKTable {
                                    schema: foreign_schema_name.to_string(),
                                    table: foreign_table_name.to_string(),
                                    column,
                                    name: constraint_name.to_string(),
                                    sql,
                                });
                            }
                            Some(fks) => {
                                fks.column.insert(foreign_column_name.to_string());
                            }
                        }
                    }
                }
            }
            Ok(())
        }
    }
}

#[inline]
async fn load_info_idx(_db_name: &str, db: &mut tokio_postgres::Transaction<'_>, data: &mut InfoSchemaType) -> Result<(), String> {
    // Query indexes with detailed column information from pg_index
    let result = db.query(
        "SELECT
            n.nspname as schema_name,
            t.relname as table_name,
            i.relname as index_name,
            ix.indisunique as is_unique,
            ix.indisprimary as is_primary,
            am.amname as index_method,
            a.attname as column_name,
            CASE WHEN ix.indoption[array_position(ix.indkey, a.attnum) - 1] & 1 = 1 THEN 'DESC' ELSE 'ASC' END as sort_order,
            CASE
                WHEN ix.indoption[array_position(ix.indkey, a.attnum) - 1] & 2 = 2 THEN 'FIRST'
                ELSE 'LAST'
            END as nulls_order,
            COALESCE(coll.collname, '') as collation,
            array_position(ix.indkey, a.attnum) as col_position
         FROM pg_index ix
         JOIN pg_class i ON i.oid = ix.indexrelid
         JOIN pg_class t ON t.oid = ix.indrelid
         JOIN pg_namespace n ON n.oid = t.relnamespace
         JOIN pg_am am ON am.oid = i.relam
         JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey)
         LEFT JOIN pg_collation coll ON coll.oid = ANY(ix.indcollation)
            AND array_position(ix.indcollation, coll.oid) = array_position(ix.indkey, a.attnum)
         WHERE n.nspname NOT IN ('pg_catalog', 'information_schema')
         ORDER BY n.nspname, t.relname, i.relname, array_position(ix.indkey, a.attnum)",
        &[]
    ).await.map_err(|e| format!("on loading pg_indexes: {}", e))?;

    // Group results by index
    let mut current_index: Option<(String, String, String, PgIndex)> = None;

    for r in result {
        let schema_name: &str = r.get(0);
        let table_name: &str = r.get(1);
        let index_name: &str = r.get(2);
        let is_unique: bool = r.get(3);
        let is_primary: bool = r.get(4);
        let index_method: &str = r.get(5);
        let column_name: &str = r.get(6);
        let sort_order: &str = r.get(7);
        let nulls_order: &str = r.get(8);
        let collation: &str = r.get(9);

        // Skip primary key indexes as they are handled separately
        if is_primary {
            continue;
        }

        let col_info = PgIndexColumn {
            column_name: column_name.to_string(),
            order: sort_order.to_string(),
            nulls: nulls_order.to_string(),
            collation: collation.to_string(),
        };

        match &mut current_index {
            Some((cur_schema, cur_table, cur_idx_name, idx))
                if cur_schema == schema_name && cur_table == table_name && cur_idx_name == index_name =>
            {
                // Same index, add column
                idx.columns.push(col_info);
            }
            _ => {
                // Save previous index if exists
                if let Some((prev_schema, prev_table, _, prev_idx)) = current_index.take() {
                    if let Some(schema_tables) = data.get_mut(&prev_schema) {
                        if let Some(table) = schema_tables.get_mut(&prev_table) {
                            table.indexes.insert(prev_idx.index_name.clone(), prev_idx);
                        }
                    }
                }
                // Start new index
                current_index = Some((
                    schema_name.to_string(),
                    table_name.to_string(),
                    index_name.to_string(),
                    PgIndex {
                        index_name: index_name.to_string(),
                        columns: vec![col_info],
                        is_unique,
                        index_method: index_method.to_string(),
                    },
                ));
            }
        }
    }

    // Save last index
    if let Some((prev_schema, prev_table, _, prev_idx)) = current_index {
        if let Some(schema_tables) = data.get_mut(&prev_schema) {
            if let Some(table) = schema_tables.get_mut(&prev_table) {
                table.indexes.insert(prev_idx.index_name.clone(), prev_idx);
            }
        }
    }

    Ok(())
}

#[inline]
async fn load_info_pk(db: &mut tokio_postgres::Transaction<'_>, data: &mut InfoSchemaType) -> Result<(), String> {
    // Query primary keys with column order from pg_constraint
    let result = db.query(
        "SELECT
            n.nspname as schema_name,
            t.relname as table_name,
            c.conname as constraint_name,
            a.attname as column_name,
            array_position(c.conkey, a.attnum) as col_position
         FROM pg_constraint c
         JOIN pg_class t ON t.oid = c.conrelid
         JOIN pg_namespace n ON n.oid = t.relnamespace
         JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(c.conkey)
         WHERE c.contype = 'p'
           AND n.nspname NOT IN ('pg_catalog', 'information_schema')
         ORDER BY n.nspname, t.relname, array_position(c.conkey, a.attnum)",
        &[]
    ).await.map_err(|e| format!("on loading primary keys: {}", e))?;

    // Group results by table
    let mut current_pk: Option<(String, String, String, Vec<String>)> = None; // schema, table, constraint_name, columns

    for r in result {
        let schema_name: &str = r.get(0);
        let table_name: &str = r.get(1);
        let constraint_name: &str = r.get(2);
        let column_name: &str = r.get(3);

        match &mut current_pk {
            Some((cur_schema, cur_table, _, columns))
                if cur_schema == schema_name && cur_table == table_name =>
            {
                // Same table, add column
                columns.push(column_name.to_string());
            }
            _ => {
                // Save previous PK if exists
                if let Some((prev_schema, prev_table, prev_constraint, prev_columns)) = current_pk.take() {
                    if let Some(schema_tables) = data.get_mut(&prev_schema) {
                        if let Some(table) = schema_tables.get_mut(&prev_table) {
                            table.primary_key = Some(PgPrimaryKey {
                                constraint_name: prev_constraint,
                                columns: prev_columns,
                            });
                        }
                    }
                }
                // Start new PK
                current_pk = Some((
                    schema_name.to_string(),
                    table_name.to_string(),
                    constraint_name.to_string(),
                    vec![column_name.to_string()],
                ));
            }
        }
    }

    // Save last PK
    if let Some((prev_schema, prev_table, prev_constraint, prev_columns)) = current_pk {
        if let Some(schema_tables) = data.get_mut(&prev_schema) {
            if let Some(table) = schema_tables.get_mut(&prev_table) {
                table.primary_key = Some(PgPrimaryKey {
                    constraint_name: prev_constraint,
                    columns: prev_columns,
                });
            }
        }
    }

    Ok(())
}

#[inline]
async fn load_info_grant(_db_name: &str, db: &mut tokio_postgres::Transaction<'_>, data: &mut InfoSchemaType) -> Result<(), String> {
    // Query table grants from information_schema
    let result = db.query(
        "SELECT
            table_schema,
            table_name,
            grantee,
            privilege_type,
            is_grantable
         FROM information_schema.table_privileges
         WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
           AND grantor != grantee
         ORDER BY table_schema, table_name, grantee",
        &[]
    ).await.map_err(|e| format!("on loading table_privileges: {}", e))?;

    for r in result {
        let schema_name: &str = r.get(0);
        let table_name: &str = r.get(1);
        let grantee: &str = r.get(2);
        let privilege_type: &str = r.get(3);
        let is_grantable: &str = r.get(4);

        if let Some(schema_tables) = data.get_mut(schema_name) {
            if let Some(table) = schema_tables.get_mut(table_name) {
                let grant = table.grants.entry(grantee.to_string()).or_insert_with(|| PgGrant {
                    grantee: grantee.to_string(),
                    privileges: HashSet::new(),
                    with_grant_option: false,
                });
                grant.privileges.insert(privilege_type.to_string());
                if is_grantable == "YES" {
                    grant.with_grant_option = true;
                }
            }
        }
    }
    Ok(())
}

#[inline]
pub async fn load_info_schema_owner(db_name: &str, db: &mut tokio_postgres::Transaction<'_>) -> Result<InfoSchemaOwnerType, String> {
    let mut res = HashMap::new();
    match db.query("select schema_name, schema_owner from information_schema.schemata where schema_name not in ('information_schema', 'pg_catalog')",
                   &[]).await {
        Ok(schemas) => {
            for schema in schemas {
                let schema_name: &str = schema.get(0);
                let schema_owner: &str = schema.get(1);
                let mut tables = HashMap::new();
                match db.query("select t.table_name, u.usename
from information_schema.tables t
join pg_catalog.pg_class c on (t.table_name = c.relname)
join pg_catalog.pg_user u on (c.relowner = u.usesysid)
where t.table_schema = $1 and t.table_catalog = $2 ", &[&schema_name, &db_name]).await {
                    Err(e) => { return Err(format!("on loading information_schema.owner: {}", e)); }
                    Ok(result) => {
                        for r in result {
                            let table_name: &str = r.get(0);
                            let table_owner: &str = r.get(1);
                            tables.insert(table_name.into(), table_owner.into());
                        }
                    }
                }
                res.insert(schema_name.into(), (schema_owner.into(), tables));
            }
            Ok(res)
        }
        Err(e) => Err(format!("on loading information schema owners: {}", e)),
    }
}

impl Default for PgTable {
    fn default() -> Self {
        PgTable {
            table_name: "".to_string(),
            columns: Default::default(),
            fks: Default::default(),
            triggers: Default::default(),
            indexes: Default::default(),
            grants: Default::default(),
            primary_key: None,
            sort_order: 0,
            table_comment: None,
            owner: None,
        }
    }
}

impl PgTable {
    /// pre loaded on information_schema.columns
    #[inline]
    fn new(table: &str, column: &str, column_data: PgColumnDfn, sort_order: usize) -> Self {
        let mut cls = HashMap::new();
        cls.insert(column.into(), column_data);
        PgTable {
            table_name: table.into(),
            columns: cls,
            sort_order,
            ..PgTable::default()
        }
    }

    /// should not be called - kept for backward compatibility
    #[inline]
    #[allow(dead_code)]
    fn newt(table: &str, trig_name: &str, event: &str, orientation: &str, proc: &str, sort_order: usize) -> Self {
        let mut tgs = HashMap::new();
        tgs.insert(trig_name.into(), PgTrigger {
            trigger_name: trig_name.to_string(),
            event: event.to_string(),
            orientation: orientation.to_string(),
            proc: proc.to_string(),
        });
        PgTable {
            table_name: table.into(),
            triggers: tgs,
            sort_order,
            ..PgTable::default()
        }
    }

    /// проверяет колонки на РК, если несколько, то выдает готовый кусок SQL,
    /// если одна колонка, то будет написано в def() колонке
    /// возвращает только, если 2 колонки
    pub(crate) fn pks(&self) -> Option<String> {
        let mut pks = ", PRIMARY KEY (".to_string();
        let mut cnt = 0;
        for c in self.columns.values() {
            if c.pk {
                cnt += 1;
                if cnt > 1 {
                    pks.push_str(", ");
                }
                pks.push_str(c.column_name.as_str());
            }
        }
        if cnt > 1 {
            pks.push_str(") ");
            Some(pks)
        } else {
            None
        }
    }
}
