use std::collections::{HashMap, HashSet};
use std::fmt::Write;

use crate::loader::{InfoSchemaType, PgGrant};
use crate::table::YGrant;
use crate::MigrationOptions;

/// Collects and generates GRANT/REVOKE statements for table grants
pub struct GrantBuilder<'a> {
    grants: &'a Vec<YGrant>,
    table_name: String,
}

impl<'a> GrantBuilder<'a> {
    pub fn new(grants: &'a Vec<YGrant>, table_name: &str) -> Self {
        GrantBuilder {
            grants,
            table_name: table_name.to_string(),
        }
    }

    /// Generate GRANT/REVOKE SQL statements and update dbc with grants
    pub fn generate_sql(
        &self,
        schema: &str,
        dbc: &mut InfoSchemaType,
        opt: &MigrationOptions,
    ) -> Result<String, String> {
        let mut grants_sql = String::new();
        let mut skipped_sql = String::new();

        // Get existing grants from dbc
        let existing_grants: HashMap<String, PgGrant> = if let Some(ss) = dbc.get(schema) {
            if let Some(ts) = ss.get(&self.table_name) {
                ts.grants.clone()
            } else {
                HashMap::new()
            }
        } else {
            HashMap::new()
        };

        // Build desired grants from YAML
        let mut desired_grants: HashMap<String, HashSet<String>> = HashMap::new();
        let mut grant_options: HashMap<String, bool> = HashMap::new();

        for yg in self.grants {
            // Process each privilege type
            let privileges = [
                ("all", &yg.all),
                ("SELECT", &yg.select),
                ("INSERT", &yg.insert),
                ("UPDATE", &yg.update),
                ("DELETE", &yg.delete),
                ("TRUNCATE", &yg.truncate),
                ("REFERENCES", &yg.references),
                ("TRIGGER", &yg.trigger),
            ];

            for (priv_name, grantee) in privileges {
                if !grantee.is_empty() {
                    let entry = desired_grants.entry(grantee.clone()).or_insert_with(HashSet::new);
                    if priv_name == "all" {
                        // ALL expands to all table privileges
                        entry.insert("SELECT".to_string());
                        entry.insert("INSERT".to_string());
                        entry.insert("UPDATE".to_string());
                        entry.insert("DELETE".to_string());
                        entry.insert("TRUNCATE".to_string());
                        entry.insert("REFERENCES".to_string());
                        entry.insert("TRIGGER".to_string());
                    } else {
                        entry.insert(priv_name.to_string());
                    }
                    if yg.with_grant_option {
                        grant_options.insert(grantee.clone(), true);
                    }
                }
            }
        }

        // Compare and generate REVOKE/GRANT statements
        for (grantee, desired_privs) in &desired_grants {
            let existing = existing_grants.get(grantee);

            // Determine what needs to be granted (new privileges)
            let privs_to_grant: Vec<&String> = match existing {
                None => desired_privs.iter().collect(),
                Some(ex) => desired_privs.difference(&ex.privileges).collect(),
            };

            // Determine what needs to be revoked (removed privileges)
            let privs_to_revoke: Vec<&String> = match existing {
                None => vec![],
                Some(ex) => ex.privileges.difference(desired_privs).collect(),
            };

            // Generate REVOKE statements for changed privileges
            if !privs_to_revoke.is_empty() {
                let privs_str: Vec<&str> = privs_to_revoke.iter().map(|s| s.as_str()).collect();
                let revoke_stmt = format!(
                    "REVOKE {} ON {}.{} FROM {};\n",
                    privs_str.join(", "),
                    schema,
                    self.table_name,
                    grantee
                );

                if opt.with_revoke {
                    grants_sql.push_str(&revoke_stmt);
                } else {
                    if opt.without_failfast {
                        // Show skipped SQL
                        let _ = writeln!(skipped_sql, "-- SKIPPED (with_revoke=false): {}", revoke_stmt.trim());
                    } else {
                        return Err(format!(
                            "Grant changes detected for {} on {}.{} but without_failfast is enabled. SQL: {}",
                            grantee, schema, self.table_name, revoke_stmt.trim()
                        ));
                    }
                }
            }

            // Generate GRANT statements for new privileges
            if !privs_to_grant.is_empty() {
                let privs_str: Vec<&str> = privs_to_grant.iter().map(|s| s.as_str()).collect();
                let with_grant = if grant_options.get(grantee).unwrap_or(&false) == &true {
                    " WITH GRANT OPTION"
                } else {
                    ""
                };
                let _ = writeln!(
                    grants_sql,
                    "GRANT {} ON {}.{} TO {}{};",
                    privs_str.join(", "),
                    schema,
                    self.table_name,
                    grantee,
                    with_grant
                );
            }

            // Update dbc with new grants (only if we're applying changes)
            if opt.with_revoke || privs_to_revoke.is_empty() {
                if let Some(ss) = dbc.get_mut(schema) {
                    if let Some(ts) = ss.get_mut(&self.table_name) {
                        ts.grants.insert(grantee.clone(), PgGrant {
                            grantee: grantee.clone(),
                            privileges: desired_privs.clone(),
                            with_grant_option: *grant_options.get(grantee).unwrap_or(&false),
                        });
                    }
                }
            }
        }

        // Handle grantees that exist in DB but not in YAML (revoke all)
        for (grantee, existing) in &existing_grants {
            if !desired_grants.contains_key(grantee) {
                let privs_str: Vec<&str> = existing.privileges.iter().map(|s| s.as_str()).collect();
                if !privs_str.is_empty() {
                    let revoke_stmt = format!(
                        "REVOKE {} ON {}.{} FROM {};\n",
                        privs_str.join(", "),
                        schema,
                        self.table_name,
                        grantee
                    );
                    
                    if opt.with_revoke {
                        grants_sql.push_str(&revoke_stmt);
                        // Remove from dbc
                        if let Some(ss) = dbc.get_mut(schema) {
                            if let Some(ts) = ss.get_mut(&self.table_name) {
                                ts.grants.remove(grantee);
                            }
                        }
                    } else {
                        if opt.without_failfast {
                            // Show skipped SQL
                            let _ = writeln!(skipped_sql, "-- SKIPPED (with_revoke=false): {}", revoke_stmt.trim());
                        } else {
                            return Err(format!(
                                "Grant removal detected for {} on {}.{} but without_failfast is enabled. SQL: {}",
                                grantee, schema, self.table_name, revoke_stmt.trim()
                            ));
                        }
                    }
                }
            }
        }

        // Log skipped SQL if any
        if !skipped_sql.is_empty() {
            #[cfg(not(feature = "slog"))]
            eprintln!("Skipped grant changes:\n{}", skipped_sql);
        }

        Ok(grants_sql)
    }
}
