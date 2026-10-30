//! [dry-run] What a dry run hands its callback: the SQL a deploy would run, its
//! schema.table, and the changes it would make there - as schema_guard decides them,
//! so a caller's digest reads no SQL back

/// the _plan functions' callback: the SQL, the schema.table it is of, and the changes
/// to make there - none when nothing changes
pub type DryRun<'a> = dyn Fn(Vec<String>, Option<&str>, Option<&Changes>) -> Result<(), String> + Send + Sync + 'a;

/// [dry-run] a dry run of the SQL alone - the functions without _plan take one - as a
/// plan's callback
pub(crate) fn sql_only<'a>(f: &'a (dyn Fn(Vec<String>) -> Result<(), String> + Send + Sync))
    -> impl Fn(Vec<String>, Option<&str>, Option<&Changes>) -> Result<(), String> + Send + Sync + 'a {
    move |sql: Vec<String>, _: Option<&str>, _: Option<&Changes>| f(sql)
}

/// one table's changes to make, from one deploy step: the table's, or its foreign keys'
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Changes {
    /// the table is created: columns are all of its own
    pub create: bool,
    /// columns created with the table, or added to it
    pub columns: Vec<String>,
    /// columns whose type or NOT NULL changes
    pub altered: Vec<String>,
    /// indexes created - or dropped and created again
    pub indexes: Vec<String>,
    /// grantees granted to or revoked from
    pub grants: Vec<String>,
    /// triggers created - or dropped and created again
    pub triggers: Vec<String>,
    /// foreign keys added, by their column
    pub foreign_keys: Vec<String>,
    /// the primary key changes
    pub primary_key: bool,
    /// the owner changes
    pub owner: bool,
    /// the table's comment is set
    pub comment: bool,
    /// the rows of its data inserted
    pub rows: usize,
}

impl Changes {
    /// nothing changes
    pub fn is_empty(&self) -> bool {
        *self == Changes::default()
    }

    /// these changes, or none when there are none
    pub fn some(&self) -> Option<&Changes> {
        if self.is_empty() { None } else { Some(self) }
    }

    pub(crate) fn alter(&mut self, column: &str) {
        if !self.altered.iter().any(|c| c == column) {
            self.altered.push(column.into());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// [dry-run] no changes are none; a column altered twice is altered once
    #[test]
    fn changes_are_some_only_when_there_are_any() {
        let mut c = Changes::default();
        assert!(c.is_empty() && c.some().is_none());
        c.alter("total");
        c.alter("total");
        assert_eq!(c.altered, vec!["total".to_string()]);
        assert_eq!(c.some(), Some(&c));
    }
}
