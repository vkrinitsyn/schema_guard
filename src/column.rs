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

#[derive(Debug, Clone, Serialize)]
pub struct Index {
    #[serde(skip_serializing_if = "String::is_empty")]
    pub name: String,
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

        Column {
            name: crate::utils::safe_sql_name(crate::utils::as_str_esc(input, "name")),
            column_type: crate::utils::as_str_esc(input, "type"),
            default_value: input["defaultValue"].as_str().map(|s| crate::utils::as_esc(s)),
            description: crate::utils::as_str_esc(input, "description"),
            sql: crate::utils::as_str_esc(input, "sql"),
            constraint,
            index: if index.is_null() {
                None
            } else {
                Some(Index::new(index))
            },
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
}

impl Index {
    pub(crate) fn new(input: &Yaml) -> Self {
        Index {
            name: crate::utils::as_str_esc(input, "name"),
            sql: crate::utils::as_str_esc(input, "sql"),
        }
    }
}