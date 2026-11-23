//! [security] the declaration read and checked, the file linked and checked, the paths resolved, the
//! SQL made of it against what a database has
use super::*;
use yaml_rust2::YamlLoader;

fn decl(rls: bool) -> Security {
    Security { alias: Some("dept".into()), soft_delete: Some("deleted_at".into()), rls, ..Security::of_dml(vec!["dept".into()]) }
}

/// named paths of the dml one alone
fn dml(path: Vec<String>) -> BTreeMap<String, Vec<String>> {
    [(DML.to_string(), path)].into_iter().collect()
}

/// named paths: each name and its steps
fn named(paths: &[(&str, &[&str])]) -> BTreeMap<String, Vec<String>> {
    paths.iter().map(|(n, p)| (n.to_string(), p.iter().map(|s| s.to_string()).collect())).collect()
}

/// a declaration with its path resolved by `dbc` and the alias it says (else its last column's name)
fn resolved(d: &Security, schema: &str, table: &str, dbc: &crate::loader::InfoSchemaType) -> Resolved {
    Resolved { paths: d.resolve_paths(schema, table, dbc).unwrap(), alias: if d.has_dml() { d.alias_or_column() } else { String::new() } }
}

/// `decl` on app.orders, resolved
fn orders(d: &Security) -> Resolved {
    resolved(d, "app", "orders", &Default::default())
}

fn path(steps: &[&str]) -> Security {
    Security::of_dml(steps.iter().map(|s| s.to_string()).collect())
}

#[test]
fn a_declaration_is_read_and_checked() {
    let y = YamlLoader::load_from_str("security: {schemaPath: [dept], alias: dept, softDelete: deleted_at}").unwrap().remove(0);
    assert_eq!(Security::read(&y["security"]), Ok(Some(decl(false))), "a table does not say rls: the deploy's mode does");
    // `rls` was a table's key: refused by that name, as `kind` is - not dropped, which would leave a table
    // that says `rls: true` without its policies under another mode
    for was in ["rls: true", "rls: false"] {
        let y = YamlLoader::load_from_str(&format!("security: {{schemaPath: [dept], {was}}}")).unwrap().remove(0);
        assert!(Security::read(&y["security"]).unwrap_err().contains("`rls` is the deploy's mode now"), "{was}");
    }
    let y = YamlLoader::load_from_str("security: {schemaPath: dept}").unwrap().remove(0);
    assert_eq!(Security::read(&y["security"]), Ok(Some(Security { alias: None, soft_delete: None, rls: false, ..decl(false) })));
    assert_eq!(Security::read(&y["other"]), Ok(None));
    let cols = vec!["id".to_string(), "dept".to_string(), "deleted_at".to_string()];
    assert_eq!(decl(true).problem("orders", &cols), None);
    // [security] the alias is left out where it is the column's name
    assert_eq!(Security { alias: None, ..decl(true) }.alias_or_column(), "dept");
    assert_eq!(path(&["public.items.order_id", "public.orders.dept"]).alias_or_column(), "dept", "the last column's");
    assert!(Security { schema_path: dml(vec!["nope".into()]), ..decl(true) }.problem("orders", &cols).unwrap().contains("nope"));
    assert!(Security { soft_delete: Some("gone".into()), ..decl(true) }.problem("orders", &cols).is_some());
    assert!(Security { schema_path: dml(vec!["dept; drop table x".into()]), ..decl(true) }.problem("orders", &[]).is_some(), "a plain name only");
    assert!(Security { schema_path: dml(vec!["public.items.order_id".into(), "public.orders.dept".into()]), ..decl(true) }.problem("orders", &[]).unwrap()
        .contains("first step"), "a path starts at the table declared");
    assert_eq!(decl(true).problem("orders", &[]), None, "columns not known: not checked");
    assert!(path(&[]).problem("orders", &[]).unwrap().contains("schemaPath"));
    // `kind` was the alias's name before: refused by that name; an alias is no path
    let y = YamlLoader::load_from_str("security: {schemaPath: [dept], kind: dept}").unwrap().remove(0);
    assert!(Security::read(&y["security"]).unwrap_err().contains("alias"));
    let y = YamlLoader::load_from_str("security: {schemaPath: [res_id], alias: public.documents.folder_id|public.folders.kind}").unwrap().remove(0);
    assert!(Security::read(&y["security"]).unwrap_err().contains("not a path"));
}

#[test]
fn the_sql_is_what_the_database_lacks() {
    let d = decl(true);
    let fresh = d.sql("app", "orders", &orders(&d), &Have { enter_missing: vec!["sales".into()], ..Default::default() }, false);
    assert!(fresh.starts_with(&format!("SET LOCAL standard_conforming_strings = on;\n{SQL}")), "the SQL part first");
    for want in ["INSERT INTO labac.tables", "'app', 'orders', 'dept', 'dept', 'deleted_at', true, 'app.orders.dept'",
                 "GRANT EXECUTE ON FUNCTION labac.enter(text), labac.enter_session(text) TO sales;", "ALTER TABLE app.orders ENABLE ROW LEVEL SECURITY;",
                 "ALTER TABLE app.orders FORCE ROW LEVEL SECURITY;",
                 "CREATE POLICY labac_select ON app.orders FOR SELECT USING ((labac.any_id('dept', 'select') OR (orders.dept)::text = ANY (labac.ids('dept', 'select'))));",
                 "FOR INSERT WITH CHECK ((labac.any_id('dept', 'insert') OR (orders.dept)::text = ANY (labac.ids('dept', 'insert'))))",
                 "FOR UPDATE USING ((labac.any_id('dept', 'update') OR (orders.dept)::text = ANY (labac.ids('dept', 'update'))) AND (orders.deleted_at IS NULL OR (labac.any_id('dept', 'soft_delete') OR (orders.dept)::text = ANY (labac.ids('dept', 'soft_delete')))))",
                 "FOR DELETE USING ((labac.any_id('dept', 'delete') OR (orders.dept)::text = ANY (labac.ids('dept', 'delete'))))",
                 "COMMENT ON POLICY labac_delete ON app.orders IS 'labac:v6:app.orders.dept>:dept:deleted_at::';"] {
        assert!(fresh.contains(want), "{want}\n{fresh}");
    }
    let sig = "labac:v6:app.orders.dept>:dept:deleted_at::".to_string();
    let row = Registered { schema: "app".into(), table: "orders".into(), schema_path: dml(vec!["dept".into()]), alias: "dept".into(),
        soft_delete: Some("deleted_at".into()), rls: true, endpoint: Some("app.orders.dept".into()) };
    let all = Have { installed: true, registered: Some(row.clone()), enabled: true, forced: true,
        policies: ["labac_select", "labac_insert", "labac_update", "labac_delete"].iter().map(|n| (n.to_string(), sig.clone())).collect(),
        enter_missing: vec![], others: 0 };
    assert_eq!(d.sql("app", "orders", &orders(&d), &all, false), "", "nothing to make");
    // another alias: the registry's row and every policy again
    let moved = Resolved { alias: "unit".into(), ..orders(&d) };
    let s = d.sql("app", "orders", &moved, &all, false);
    assert!(!s.contains("CREATE SCHEMA") && s.contains("INSERT INTO labac.tables") && s.matches("CREATE POLICY").count() == 4, "{s}");
    // a row an older schema_guard wrote has no endpoint: written again
    let older = Have { registered: Some(Registered { endpoint: None, ..row }), ..all.clone() };
    assert!(d.sql("app", "orders", &orders(&d), &older, false).contains("INSERT INTO labac.tables"));
    let lsr = decl(false).sql("app", "orders", &orders(&decl(false)), &Have { installed: true, ..Default::default() }, false);
    assert!(lsr.contains("INSERT INTO labac.tables") && !lsr.contains("POLICY") && !lsr.contains("ROW LEVEL"), "{lsr}");
    let b = d.sql("app", "orders", &orders(&d), &Have::default(), true);
    assert!(b.contains("orders.deleted_at IS NOT TRUE OR"), "a boolean mark");
    let q = d.sql("app", "orders", &Resolved { alias: "o'k".into(), ..orders(&d) }, &Have::default(), false);
    assert!(q.contains("labac.ids('o''k', 'select')"), "a quote in the alias doubled");
}

#[test]
fn data_rows_go_in_with_force_lifted() {
    assert_eq!(decl(false).around_data("a", "t", "insert;\n".into()), "insert;\n");
    assert_eq!(decl(true).around_data("a", "t", String::new()), "");
    assert_eq!(decl(true).around_data("a", "t", "insert;\n".into()),
        "ALTER TABLE a.t NO FORCE ROW LEVEL SECURITY;\ninsert;\nALTER TABLE a.t FORCE ROW LEVEL SECURITY;\n");
}

/// [review 2026-11-21] the soft-delete mark's kind is in the signature; rls turned off takes labac's
/// policies away, and row-level security unless the table has its own; the SQL carries its version
#[test]
fn a_changed_mark_and_rls_off() {
    let d = decl(true);
    let r = orders(&d);
    let sig_ts = signature(&r, d.soft_delete.as_deref(), false);
    assert_ne!(sig_ts, signature(&r, d.soft_delete.as_deref(), true), "a boolean mark is another declaration");
    assert_eq!(signature(&r, None, true), signature(&r, None, false));
    assert_ne!(sig_ts, signature(&Resolved { alias: "unit".into(), ..r.clone() }, d.soft_delete.as_deref(), false), "the alias is in it");
    let row = Registered { schema: "app".into(), table: "orders".into(), schema_path: dml(vec!["dept".into()]), alias: "dept".into(),
        soft_delete: Some("deleted_at".into()), rls: true, endpoint: Some("app.orders.dept".into()) };
    let had = Have { installed: true, registered: Some(row), enabled: true, forced: true,
        policies: [("labac_select".to_string(), sig_ts.clone()), ("labac_update".to_string(), sig_ts)].into_iter().collect(),
        enter_missing: vec![], others: 0 };
    let off = decl(false).sql("app", "orders", &orders(&decl(false)), &had, false);
    for want in ["DROP POLICY IF EXISTS labac_select ON app.orders;", "DROP POLICY IF EXISTS labac_update ON app.orders;",
                 "ALTER TABLE app.orders NO FORCE ROW LEVEL SECURITY;", "ALTER TABLE app.orders DISABLE ROW LEVEL SECURITY;"] {
        assert!(off.contains(want), "{want}\n{off}");
    }
    let theirs = decl(false).sql("app", "orders", &orders(&decl(false)), &Have { others: 1, ..had.clone() }, false);
    assert!(!theirs.contains("DISABLE"), "the table's own policies keep it on");
    let lsr_row = Registered { schema: "app".into(), table: "orders".into(), schema_path: dml(vec!["dept".into()]), alias: "dept".into(),
        soft_delete: Some("deleted_at".into()), rls: false, endpoint: Some("app.orders.dept".into()) };
    let never = decl(false).sql("app", "orders", &orders(&decl(false)), &Have { installed: true, registered: Some(lsr_row), enabled: true, ..Default::default() }, false);
    assert_eq!(never, "", "no labac policy ever: nothing to take away");
    assert!(SQL.contains(&format!("IS '{SQL_VERSION}'")) && SQL.contains("session_user") && SQL.contains("endpoint text"));
}

/// a catalog with `public.orders`, `public.folders` and `public.items`, keyed by `id`
fn catalog() -> crate::loader::InfoSchemaType {
    let t = |name: &str| crate::loader::PgTable { table_name: name.into(),
        primary_key: Some(crate::loader::PgPrimaryKey { constraint_name: format!("{name}_pkey"), columns: vec!["id".into()] }),
        ..Default::default() };
    let mut public = std::collections::HashMap::new();
    public.insert("orders".to_string(), t("orders"));
    public.insert("folders".to_string(), t("folders"));
    public.insert("items".to_string(), t("items"));
    [("public".to_string(), public)].into_iter().collect()
}

/// [security] `schemaPath` is the path of join hops, each by the next table's
/// primary key; the policies join through EXISTS; the kind read per row is no more - the alias is a name
#[test]
fn a_path_of_joins_and_the_alias_a_name() {
    let s = Security { rls: true, ..path(&["public.order_items.order_id", "public.orders.dept"]) };
    assert_eq!(s.problem("order_items", &["id".into(), "order_id".into()]), None);
    let r = resolved(&s, "public", "order_items", &catalog());
    assert_eq!(r.dml()[1], Step { schema: "public".into(), table: "orders".into(), column: "dept".into(), key: "id".into() });
    assert_eq!((r.alias.as_str(), r.endpoint().as_str()), ("dept", "public.orders.dept"));
    let sql = s.sql("public", "order_items", &r, &Have::default(), false);
    // the path's rows are read whoever asks - the user of every department too: the choice is inside the EXISTS
    assert!(sql.contains("FOR SELECT USING (EXISTS (SELECT 1 FROM public.orders j1 WHERE j1.id = order_items.order_id \
        AND (labac.any_id('dept', 'select') OR (j1.dept)::text = ANY (labac.ids('dept', 'select')))));"), "{sql}");
    assert!(sql.contains("'public', 'order_items', 'public.order_items.order_id|public.orders.dept', 'dept', NULL, true, 'public.orders.dept'"),
        "registered: the path as written, the alias, the endpoint: {sql}");
    assert!(!sql.contains("id_path") && !sql.contains("kind_path"), "no path is stored resolved");
    // two hops; the first step may be the column alone
    let two = Security { rls: true, ..path(&["item_id", "public.items.order_id", "orders.dept"]) };
    assert_eq!(two.problem("notes", &[]), None);
    let sql = two.sql("public", "notes", &resolved(&two, "public", "notes", &catalog()), &Have::default(), false);
    assert!(sql.contains("EXISTS (SELECT 1 FROM public.items j1 JOIN public.orders j2 ON j2.id = j1.order_id WHERE j1.id = notes.item_id \
        AND (labac.any_id('dept', 'select') OR (j2.dept)::text = ANY (labac.ids('dept', 'select'))))"), "{sql}");
    // an alias other than the last column's name
    let named = Security { alias: Some("department".into()), ..s.clone() };
    assert!(named.sql("public", "order_items", &resolved(&named, "public", "order_items", &catalog()), &Have::default(), false)
        .contains("labac.ids('department', 'select')"));
    // the kind read per row is gone: an alias that is a path does not read, nor does `kind`
    let y = YamlLoader::load_from_str("security: {schemaPath: [res_id], alias: documents.res_kind}").unwrap().remove(0);
    assert!(Security::read(&y["security"]).is_err());
    assert!(Security { alias: Some("public.documents.folder_id|public.folders.kind".into()), ..path(&["res_id"]) }.problem("documents", &[]).is_some());
    // what is refused
    assert!(path(&["public.order_items.order_id", "nosuch.dept"]).resolve("public", "order_items", &catalog()).unwrap_err().contains("not known"));
    assert!(path(&["a.b.c.d"]).problem("a", &[]).is_some());
    assert!(path(&["orders.id", "dept"]).problem("orders", &[]).is_some(), "a later step names its table");
    assert!(path(&["x"]).problem("t", &["id".into()]).unwrap().contains("x"));
}

/// [review 2026-11-21] a tree: resolved for the rewrite, refused for rls; a first step of another schema refused
#[test]
fn a_tree_path_and_the_first_steps_schema() {
    let mut cat = catalog();
    cat.get_mut("public").unwrap().insert("nodes".into(), crate::loader::PgTable { table_name: "nodes".into(),
        primary_key: Some(crate::loader::PgPrimaryKey { constraint_name: "nodes_pkey".into(), columns: vec!["id".into()] }), ..Default::default() });
    let lsr = path(&["nodes.parent_id", "public.nodes.dept"]);
    assert_eq!(lsr.resolve("public", "nodes", &cat).unwrap()[1].table, "nodes");
    let rls = Security { rls: true, ..lsr.clone() };
    assert_eq!(recurses("public", "nodes", &rls, &Declared::new(), &[]).as_deref(), Some("public.nodes"), "its policy would read itself");
    assert!(path(&["other.nodes.parent_id", "public.nodes.dept"]).resolve("public", "nodes", &cat).unwrap_err().contains("first step"));
    assert!(path(&["public.order_items.order_id", "public.orders.dept"]).resolve("public", "order_items", &cat).is_ok());
}

/// [review 2026-11-22] two tables whose policies would read each other, by the file or by the registry
#[test]
fn policies_recurse_through_another_table() {
    let a = Security { rls: true, ..path(&["public.a.b_id", "public.b.code"]) };
    let b = Security { rls: true, ..path(&["public.b.a_id", "public.a.code"]) };
    let file: Declared = [(("public".to_string(), "a".to_string()), a.clone()), (("public".to_string(), "b".to_string()), b.clone())].into_iter().collect();
    assert_eq!(recurses("public", "a", &a, &file, &[]).as_deref(), Some("public.b"));
    let row = Registered { schema: "public".into(), table: "b".into(), schema_path: b.schema_path.clone(), alias: "code".into(), soft_delete: None,
        rls: true, endpoint: Some("public.a.code".into()) };
    assert_eq!(recurses("public", "a", &a, &Declared::new(), std::slice::from_ref(&row)).as_deref(), Some("public.b"), "the registry's row");
    assert_eq!(recurses("public", "a", &a, &Declared::new(), &[Registered { rls: false, ..row.clone() }]), None, "b has no policy");
    // the file's word over the registry's: b is rewritten by ytproxy now
    let lsr: Declared = [(("public".to_string(), "b".to_string()), Security { rls: false, ..b })].into_iter().collect();
    assert_eq!(recurses("public", "a", &a, &lsr, &[row]), None);
    let line = Security { rls: true, ..path(&["public.order_items.order_id", "public.orders.dept"]) };
    let orders: Declared = [(("public".to_string(), "orders".to_string()), Security { rls: true, ..path(&["dept"]) })].into_iter().collect();
    assert_eq!(recurses("public", "order_items", &line, &orders, &[]), None, "a parent that reads only itself");
}

/// [security] the deploy's mode decides for every declared table: off (the default) none of them, lsr
/// registered only - its traversal checked or not - rls with policies. A mode read from text: `lsr`,
/// `rls`, and anything else the default, off
#[test]
fn the_deploys_mode_over_the_declaration() {
    let d = decl(false);
    assert_eq!(SecurityMode::default(), SecurityMode::Off);
    assert_eq!(d.under(SecurityMode::Off), None);
    assert!(d.under(SecurityMode::Rls).unwrap().rls);
    for traversal in [true, false] {
        assert!(!decl(true).under(SecurityMode::Lsr(traversal)).unwrap().rls);
    }
    assert!(SecurityMode::Lsr(true).traversal(), "the file's checks: under Lsr(true) only");
    assert!(!SecurityMode::Lsr(false).traversal() && !SecurityMode::Rls.traversal() && !SecurityMode::Off.traversal());
    assert_eq!("RLS".parse::<SecurityMode>(), Ok(SecurityMode::Rls));
    assert_eq!(" lsr ".parse::<SecurityMode>(), Ok(SecurityMode::Lsr(true)));
    for other in ["off", "OFF", "", "both", "rsl", "lsr(false)", "on"] {
        assert_eq!(other.parse::<SecurityMode>(), Ok(SecurityMode::Off), "{other}: anything else is the default");
    }
}

/// a schema YAML validated and parsed as a deploy reads it
fn parsed(yaml: &str) -> crate::utils::OrderedHashMap<crate::schema::Schema> {
    let doc = crate::load_schema_from_src(yaml.to_string()).expect("the YAML validates");
    crate::parse_yaml_schema(doc, "security.yaml").expect("the YAML parses")
}

/// the catalog a deploy has of a parsed file: each table with its primary key
fn catalog_of(schemas: &crate::utils::OrderedHashMap<crate::schema::Schema>) -> crate::loader::InfoSchemaType {
    let mut dbc = crate::loader::InfoSchemaType::new();
    for s in &schemas.list {
        let tables = dbc.entry(s.schema_name.clone()).or_default();
        for t in s.tables.list.iter().filter(|t| !t.is_template()) {
            tables.insert(t.table_name.clone(), crate::loader::PgTable { table_name: t.table_name.clone(),
                primary_key: Some(crate::loader::PgPrimaryKey { constraint_name: format!("{}_pkey", t.table_name), columns: t.get_primary_key_columns() }),
                ..Default::default() });
        }
    }
    dbc
}

const SHOP: &str = r#"
database:
  - schemaName: public
    tables:
      - table:
          tableName: order_items
          security:
            schemaPath: [public.order_items.order_id, public.orders.dept]
          columns:
            - column: {name: item_no, type: int, constraint: {primaryKey: true}}
            - column: {name: order_id, type: int, constraint: {foreignKey: {references: public.orders}}}
      - table:
          tableName: orders
          security: {schemaPath: [dept], alias: department, softDelete: deleted_at}
          columns:
            - column: {name: id, type: int, constraint: {primaryKey: true}}
            - column: {name: dept, type: text}
            - column: {name: deleted_at, type: timestamptz}
      - table:
          tableName: notes
          security: {schemaPath: [item_id, public.order_items.order_id, orders.dept]}
          columns:
            - column: {name: id, type: int, constraint: {primaryKey: true}}
            - column: {name: item_id, type: int}
      - table:
          tableName: projects
          security: {schemaPath: code, alias: project}
          columns:
            - column: {name: id, type: int, constraint: {primaryKey: true}}
            - column: {name: code, type: text}
      - table:
          tableName: tasks
          security: {schemaPath: [tasks.project_id, projects.code]}
          columns:
            - column: {name: id, type: int, constraint: {primaryKey: true}}
            - column: {name: project_id, type: int}
"#;

/// [security] from a schema YAML - validated, parsed and linked by schema_guard
/// as a deploy does - to the paths resolved and the aliases: said once, by the table that defines the
/// entity, and taken by the tables whose paths reach it, whatever their order in the file
#[test]
fn a_schema_yaml_builds_the_proper_paths() {
    let mut schemas = parsed(SHOP);
    link(&mut schemas, true).expect("the file's checks pass");
    let dbc = catalog_of(&schemas);
    let public = schemas.list.iter().find(|s| s.schema_name == "public").unwrap();
    let of = |name: &str| {
        let t = public.tables.list.iter().find(|t| t.table_name == name).unwrap();
        let d = t.security.clone().unwrap_or_else(|| panic!("{name} declares security"));
        assert_eq!(d.problem(name, &t.columns.list.iter().map(|c| c.name.clone()).collect::<Vec<_>>()), None, "{name}");
        let r = resolved(&d, "public", name, &dbc);
        (d, r)
    };
    let step = |t: &str, c: &str, key: &str| Step { schema: "public".into(), table: t.into(), column: c.into(), key: key.into() };
    let (orders, r) = of("orders");
    assert_eq!((orders.alias.as_deref(), r.alias.as_str(), r.endpoint().as_str()), (Some("department"), "department", "public.orders.dept"));
    assert_eq!(r.dml(), vec![step("orders", "dept", "")]);
    assert!(!orders.rls && orders.defining(), "as read: rls is the mode's");
    // declared before its entity's table, and without an alias: the entity's
    let (items, r) = of("order_items");
    assert_eq!(items.alias.as_deref(), Some("department"), "taken from the table that defines public.orders.dept");
    assert_eq!(r.dml(), vec![step("order_items", "order_id", ""), step("orders", "dept", "id")], "to the parent, on its key");
    assert_eq!(r.alias, "department");
    let (notes, r) = of("notes");
    assert_eq!(r.dml(), vec![step("notes", "item_id", ""), step("order_items", "order_id", "item_no"), step("orders", "dept", "id")],
        "two hops: order_items joined on its own key item_no");
    assert_eq!((notes.alias.as_deref(), r.endpoint().as_str()), (Some("department"), "public.orders.dept"));
    let (tasks, r) = of("tasks");
    assert_eq!(tasks.alias.as_deref(), Some("project"), "the alias of public.projects.code, not its column's name");
    assert_eq!(r.dml(), vec![step("tasks", "project_id", ""), step("projects", "code", "id")]);
    let sql = notes.sql("public", "notes", &r_of(&notes, &dbc, "notes"), &Have { installed: true, ..Default::default() }, false);
    assert!(sql.contains("'item_id|public.order_items.order_id|orders.dept', 'department', NULL, false, 'public.orders.dept'"), "registered as written: {sql}");
    // what a deploy takes of the file
    for (mode, rls) in [(SecurityMode::Rls, true), (SecurityMode::Lsr(true), false), (SecurityMode::Lsr(false), false)] {
        let declared = declared(&schemas, mode);
        assert_eq!(declared.len(), 5, "{mode:?}");
        assert!(declared.values().filter(|d| d.defining()).count() == 2);
        assert!(declared.values().all(|d| d.rls == rls), "{mode:?}: every declared table as the mode says");
    }
    assert!(super::declared(&schemas, SecurityMode::Off).is_empty());
    assert!(super::declared(&schemas, SecurityMode::default()).is_empty(), "off unless a mode is named");
}

fn r_of(d: &Security, dbc: &crate::loader::InfoSchemaType, table: &str) -> Resolved {
    resolved(d, "public", table, dbc)
}

/// the SHOP file with one text replaced
fn shop(from: &str, to: &str) -> crate::utils::OrderedHashMap<crate::schema::Schema> {
    assert!(SHOP.contains(from), "{from}");
    parsed(&SHOP.replacen(from, to, 1))
}

/// [security] the file's checks - a path leads through tables and columns the file
/// defines, on one-column keys, the way its foreign keys say; an alias names one entity and an entity
/// has one alias - and their absence where the mode is not `Lsr(true)` (`link`'s `strict` off)
#[test]
fn the_files_checks() {
    let refused = |mut s: crate::utils::OrderedHashMap<crate::schema::Schema>| link(&mut s, true).unwrap_err();
    // the traversal
    assert!(refused(shop("schemaPath: [tasks.project_id, projects.code]", "schemaPath: [tasks.project_id, elsewhere.code]")).contains("not a table of this file"));
    assert!(refused(shop("schemaPath: [tasks.project_id, projects.code]", "schemaPath: [tasks.project_id, projects.nope]")).contains("nope is not a column of public.projects"));
    assert!(refused(shop("- column: {name: id, type: int, constraint: {primaryKey: true}}\n            - column: {name: code, type: text}",
        "- column: {name: id, type: int}\n            - column: {name: code, type: text}")).contains("primary key of one column"));
    assert!(refused(shop("schemaPath: [public.order_items.order_id, public.orders.dept]", "schemaPath: [public.order_items.order_id, public.projects.code]"))
        .contains("public.order_items.order_id references public.orders, not public.projects"));
    // the aliases: said once
    assert!(refused(shop("security: {schemaPath: [tasks.project_id, projects.code]}", "security: {schemaPath: [tasks.project_id, projects.code], alias: proj}"))
        .contains("public.projects.code is project in this file, not proj"));
    assert!(refused(shop("security: {schemaPath: code, alias: project}", "security: {schemaPath: code, alias: department}"))
        .contains("alias department names"), "two entities under one alias");
    // an entity nobody defines: the alias its paths say, the same one
    let mut plain = shop("          security: {schemaPath: code, alias: project}\n", "");
    link(&mut plain, true).expect("a path to a table with no declaration of its own");
    let tasks = plain.list[0].tables.list.iter().find(|t| t.table_name == "tasks").unwrap().security.clone().unwrap();
    assert_eq!((tasks.alias.clone(), tasks.alias_or_column()), (None, "code".to_string()), "nobody says: the last column's name");
    // with the checks off none of it is refused, and what the file knows is still linked
    for (from, to) in [("schemaPath: [tasks.project_id, projects.code]", "schemaPath: [tasks.project_id, elsewhere.code]"),
                       ("security: {schemaPath: code, alias: project}", "security: {schemaPath: code, alias: department}")] {
        let mut s = shop(from, to);
        link(&mut s, false).expect("not Lsr(true): nothing of the file is refused");
        let items = s.list[0].tables.list.iter().find(|t| t.table_name == "order_items").unwrap().security.clone().unwrap();
        assert_eq!(items.alias.as_deref(), Some("department"));
    }
}

/// [review 2026-11-22] the children of one template each have their own copy of its column: one
/// definition, not as many entities under one alias; two tables that each declare the column are two
#[test]
fn a_templates_children_are_one_definition() {
    let yaml = r#"
database:
  - schemaName: public
    tables:
      - table:
          tableName: orders_t
          template: true
          security: {schemaPath: [dept], alias: dept}
          columns:
            - column: {name: id, type: int, constraint: {primaryKey: true}}
            - column: {name: dept, type: text}
      - table:
          tableName: orders_2025
          template: [orders_t]
      - table:
          tableName: orders_2026
          template: [orders_t]
"#;
    let mut s = parsed(yaml);
    link(&mut s, true).expect("the children of one template");
    assert_eq!(declared(&s, SecurityMode::Rls).len(), 2, "the template itself is no table");
    let two = yaml.replace("          template: [orders_t]\n", "          security: {schemaPath: [dept], alias: dept}\n          columns:\n            - column: {name: id, type: int, constraint: {primaryKey: true}}\n            - column: {name: dept, type: text}\n");
    let mut s = parsed(&two);
    assert!(link(&mut s, true).unwrap_err().contains("alias dept names public.orders_2025.dept and public.orders_2026.dept"));
}

/// [security] a registry row read back: the path, the alias, the endpoint; a row of the kind read per
/// row, as a schema_guard before the alias wrote, does not read
#[test]
fn a_registry_row_reads_back() {
    let row: serde_json::Value = serde_json::json!({ "schema_name": "public", "table_name": "order_items",
        "column_name": "public.order_items.order_id|public.orders.dept", "kind": "department", "soft_delete": null, "rls": true,
        "endpoint": "public.orders.dept", "id_path": "[]" });
    let r = from_registry(&row).unwrap();
    assert_eq!((r.dml().len(), r.alias.as_str(), r.endpoint.as_deref(), r.defining()), (2, "department", Some("public.orders.dept"), false));
    assert_eq!(r.security(), Security { alias: Some("department".into()), rls: true, ..path(&["public.order_items.order_id", "public.orders.dept"]) });
    let old: serde_json::Value = serde_json::json!({ "schema_name": "public", "table_name": "orders", "column_name": "dept", "kind": "dept",
        "soft_delete": "deleted_at", "rls": false });
    assert_eq!(from_registry(&old).map(|r| (r.endpoint.clone(), r.defining())), Some((None, true)), "a registry before the endpoint");
    let per_row: serde_json::Value = serde_json::json!({ "schema_name": "public", "table_name": "documents", "column_name": "res_id",
        "kind": "public.documents.folder_id|public.folders.kind", "soft_delete": null, "rls": true });
    assert_eq!(from_registry(&per_row), None);
}

/// [review 2026-11-22] a step that is not a string is refused, not dropped - one step fewer would
/// define another entity; a schemaPath that is no list and no map of paths either, and a named path
/// that is no list
#[test]
fn a_step_that_is_no_string_is_refused() {
    let read = |text: &str| Security::read(&YamlLoader::load_from_str(text).unwrap().remove(0)["security"]);
    for bad in ["security: {schemaPath: [order_id, [orders.dept]]}", "security: {schemaPath: [order_items.order_id, 7, orders.dept]}",
                "security: {schemaPath: {a: {b: c}}}", "security: {schemaPath: 7}", "security: {schemaPath: {dml: [order_id, 7]}}",
                "security: {schemaPath: {dml: [dept], sal: [[sal]]}}", "security: {schemaPath: {dml: 7}}"] {
        assert!(read(bad).unwrap_err().contains("each a string"), "{bad}");
    }
    // a map is the named paths: a name that is not dml is a level's
    assert_eq!(read("security: {schemaPath: {a: b}}").unwrap().unwrap().schema_path, named(&[("a", &["b"])]));
    assert_eq!(read("security: {schemaPath: dept}").unwrap().unwrap().schema_path, named(&[("dml", &["dept"])]), "a list, or one step: the dml path alone");
    assert!(read("security: {alias: x}").unwrap().unwrap().problem("t", &[]).unwrap().contains("needs a schemaPath"));
}

/// [review 2026-11-22] a template's path that names the template's table is the child's: each child
/// defines its own column, the children still one definition; another table's path to a template
/// is said as that
#[test]
fn a_templates_path_naming_its_table() {
    let d = Security { alias: Some("dept".into()), ..path(&["orders_t.dept"]) };
    assert_eq!(d.of_child("orders_t", "orders_2025").dml(), vec!["orders_2025.dept".to_string()]);
    assert_eq!(path(&["public.nodes_t.parent_id", "public.nodes_t.dept", "public.other.x"]).of_child("nodes_t", "n1").dml(),
        vec!["n1.parent_id".to_string(), "n1.dept".to_string(), "public.other.x".to_string()]);
    assert_eq!(path(&["dept"]).of_child("orders_t", "orders_2025"), path(&["dept"]), "a column alone stands");
    let yaml = r#"
database:
  - schemaName: public
    tables:
      - table:
          tableName: orders_t
          template: true
          security: {schemaPath: [orders_t.dept], alias: dept}
          columns:
            - column: {name: id, type: int, constraint: {primaryKey: true}}
            - column: {name: dept, type: text}
      - table:
          tableName: orders_2025
          template: [orders_t]
      - table:
          tableName: orders_2026
          template: [orders_t]
"#;
    let mut s = parsed(yaml);
    link(&mut s, true).expect("each child's own column, one definition");
    let d = declared(&s, SecurityMode::Lsr(true));
    assert_eq!(d[&("public".to_string(), "orders_2026".to_string())].dml(), vec!["orders_2026.dept".to_string()]);
    let dbc = catalog_of(&s);
    assert_eq!(d[&("public".to_string(), "orders_2025".to_string())].resolve("public", "orders_2025", &dbc).unwrap().len(), 1);
    // a table that is no child and reaches the template
    let other = format!("{yaml}      - table:\n          tableName: notes\n          security: {{schemaPath: [notes.order_id, orders_t.dept]}}\n          columns:\n            - column: {{name: id, type: int, constraint: {{primaryKey: true}}}}\n            - column: {{name: order_id, type: int}}\n");
    let mut s = parsed(&other);
    assert!(link(&mut s, true).unwrap_err().contains("public.orders_t is a template, not a table"));
}

/// [review 2026-11-22] a registry row a schema_guard before the endpoint column wrote says its entity
/// by its path; a row whose names are not plain names is not read - they would go into DDL
#[test]
fn a_registry_row_before_the_endpoint_and_odd_names() {
    let old: serde_json::Value = serde_json::json!({ "schema_name": "public", "table_name": "order_items",
        "column_name": "public.order_items.order_id|orders.dept", "kind": "dept", "soft_delete": null, "rls": true });
    let r = from_registry(&old).unwrap();
    assert_eq!((r.endpoint.clone(), r.entity()), (None, Some("public.orders.dept".to_string())));
    let own: serde_json::Value = serde_json::json!({ "schema_name": "app", "table_name": "orders", "column_name": "dept", "kind": "dept", "rls": false });
    assert_eq!(from_registry(&own).unwrap().entity().as_deref(), Some("app.orders.dept"));
    let said: serde_json::Value = serde_json::json!({ "schema_name": "app", "table_name": "orders", "column_name": "dept", "kind": "dept", "rls": false,
        "endpoint": "app.orders.dept" });
    assert_eq!(from_registry(&said).unwrap().entity().as_deref(), Some("app.orders.dept"));
    // an alias or a path that is empty names nothing
    for (key, odd) in [("schema_name", "public; DROP TABLE x"), ("table_name", "orders\" x"), ("soft_delete", "deleted_at = now(); --"),
                       ("kind", ""), ("kind", "  "), ("column_name", ""), ("column_name", " | ")] {
        let mut row = said.clone();
        row[key] = serde_json::Value::String(odd.to_string());
        assert_eq!(from_registry(&row), None, "{key}");
    }
}

/// [review 2026-11-22] the rows of a table whose row-level security is forced go in with FORCE
/// lifted, whatever this deploy's declaration says of rls
#[test]
fn force_is_lifted_for_the_rows_of_a_forced_table() {
    assert_eq!(force_lifted("a", "t", String::new()), "");
    assert_eq!(force_lifted("a", "t", "insert;\n".into()), "ALTER TABLE a.t NO FORCE ROW LEVEL SECURITY;\ninsert;\nALTER TABLE a.t FORCE ROW LEVEL SECURITY;\n");
    assert_eq!(decl(false).around_data("a", "t", "insert;\n".into()), "insert;\n", "the declaration alone: not rls, not lifted");
}

/// [levels] a declaration's levels as a YAML says them: names of the `schemaPath` map beside `dml` -
/// `sal`, or any other; with them the dml path may be left out, an alias or a softDelete then not; a
/// name and every step are plain names
#[test]
fn levels_are_read_and_checked() {
    let read = |text: &str| Security::read(&YamlLoader::load_from_str(text).unwrap().remove(0)["security"]);
    let d = read("security: {schemaPath: {dml: [dept], sal: [public.docs.project_id, public.projects.sal]}, alias: dept}").unwrap().unwrap();
    // one map of named paths: dml, and each level's - nothing beside it
    assert_eq!(d.schema_path, named(&[("dml", &["dept"]), ("sal", &["public.docs.project_id", "public.projects.sal"])]));
    assert_eq!((d.dml(), d.levels().map(|(n, _)| n.as_str()).collect::<Vec<_>>()), (&["dept".to_string()][..], vec![SAL]), "dml names the dml path, the rest are levels");
    assert!(d.has_dml() && d.problem("docs", &[]).is_none());
    let several = read("security: {schemaPath: {dml: [dept], sal: [secrecy], review: review_level}}").unwrap().unwrap();
    assert_eq!(several.levels().map(|(n, _)| n.clone()).collect::<Vec<_>>(), vec!["review", "sal"]);
    assert_eq!(several.schema_path["review"], vec!["review_level".to_string()], "one step as a string");
    let alone = read("security: {schemaPath: {sal: [sal]}}").unwrap().unwrap();
    assert!(!alone.has_dml() && !alone.defining() && alone.problem("docs", &["id".into(), "sal".into()]).is_none(), "levels alone");
    // the keys of their own before: refused by name, whatever else the declaration says
    for old in ["security: {level: [sal]}", "security: {schemaPath: [dept], level: [sal]}", "security: {levels: {sal: [x]}}",
                "security: {schemaPath: {dml: [dept]}, levels: {review: [r]}}", "security: {levels: [sal]}"] {
        let e = read(old).unwrap_err();
        assert!(e.contains("is a name of schemaPath now") && e.contains("schemaPath: {dml: [..], sal: [..]}"), "{old}: {e}");
    }
    assert!(read("security: {schemaPath: {sal: [sal, 7]}}").unwrap_err().contains("each a string"));
    assert!(read("security: {schemaPath: {dml: [dept], sal: {a: b}}}").unwrap_err().contains("schemaPath sal is a list of"));
    // a name with no path, and a map with no name
    assert!(read("security: {schemaPath: {dml: [dept], sal: []}}").unwrap_err().contains("schemaPath sal has no step"));
    assert!(read("security: {schemaPath: {dml: [], sal: [sal]}}").unwrap_err().contains("schemaPath dml has no step"));
    assert!(read("security: {schemaPath: {}}").unwrap_err().contains("as a map names its paths"));
    assert!(read("security: {schemaPath: {7: [sal]}}").unwrap_err().contains("a path's name"));
    assert!(read("security: {alias: dept}").unwrap().unwrap().problem("t", &[]).unwrap().contains("needs a schemaPath: the dml path, or a map"));
    // [review 2026-11-22] the key's two spellings both said: refused - one of them would be passed by
    for both in ["security: {schemaPath: [dept], schema_path: {sal: [sal]}}", "security: {schemaPath: {sal: [sal]}, schema_path: [dept]}",
                 "security: {schemaPath: [dept], schema_path: [dept]}"] {
        assert!(read(both).unwrap_err().contains("schemaPath and schema_path are one key"), "{both}");
    }
    // [review 2026-11-22] a name is as it is written: with a space it is no second `sal` (or `dml`) that takes
    // the first one's place - it is no plain name, and the table is refused
    let spaced = read("security: {schemaPath: {dml: [dept], sal: [sal], ' sal': [secrecy]}}").unwrap().unwrap();
    assert_eq!((spaced.levels().count(), spaced.schema_path["sal"].clone()), (2, vec!["sal".to_string()]), "both kept: none overwritten");
    assert!(spaced.problem("docs", &[]).unwrap().contains("a level's name is letters"));
    let spaced = read("security: {schemaPath: {dml: [dept], ' dml': [other]}}").unwrap().unwrap();
    assert_eq!(spaced.dml(), vec!["dept".to_string()], "the dml path stands");
    assert!(spaced.problem("docs", &[]).unwrap().contains("a level's name is letters"));
    // dml in any case names the dml path: never a level beside it
    for odd in ["security: {schemaPath: {DML: [dept], sal: [sal]}}", "security: {schemaPath: {dml: [dept], Dml: [sal]}}"] {
        assert!(read(odd).unwrap().unwrap().problem("docs", &[]).unwrap().contains("names the dml path, never a level"), "{odd}");
    }
    // the map with dml alone is the list: one declaration, two spellings
    assert_eq!(read("security: {schemaPath: {dml: [dept]}, alias: dept}").unwrap(), read("security: {schemaPath: [dept], alias: dept}").unwrap());
    assert_eq!(read("security: {schema_path: {dml: dept, sal: sal}}").unwrap().unwrap().schema_path[SAL], vec!["sal".to_string()], "schema_path read as well, one step as a string");
    // dml is the dml path's name in the one map: a declaration made in code cannot say a level of that name beside it
    // [review 2026-11-22] a step with spaces around it is the step: read, registered and compared the same
    let spaced = read("security: {schemaPath: {dml: [' dept '], sal: ' sal'}}").unwrap().unwrap();
    assert_eq!(spaced.schema_path, named(&[("dml", &["dept"]), ("sal", &["sal"])]));
    for (text, why) in [("security: {schemaPath: {sal: [sal]}, alias: dept}", "alias and softDelete are of the dml path"),
                        ("security: {schemaPath: {sal: [sal]}, softDelete: deleted_at}", "alias and softDelete are of the dml path"),
                        ("security: {schemaPath: {dml: [dept], 'my level': [sal]}}", "a level's name is letters"),
                        ("security: {schemaPath: {dml: [dept], sal: [other.sal]}}", "its first step is a column of docs, not of other"),
                        ("security: {schemaPath: {dml: [dept], sal: ['sal; drop table x']}}", "plain names"),
                        ("security: {schemaPath: {dml: [dept], sal: [nope]}}", "nope is not a column of the table")] {
        let e = read(text).unwrap().unwrap().problem("docs", &["id".into(), "dept".into(), "sal".into(), "deleted_at".into()]).unwrap_or_default();
        assert!(e.contains(why), "{text}: {e}");
    }
    // a template's level naming the template's table is the child's
    let t = Security { schema_path: named(&[("dml", &["docs_t.dept"]), ("sal", &["docs_t.project_id", "public.projects.sal"])]), ..Default::default() };
    assert_eq!(t.of_child("docs_t", "docs_2026").schema_path, named(&[("dml", &["docs_2026.dept"]), ("sal", &["docs_2026.project_id", "public.projects.sal"])]));
}

const LEVELS: &str = r#"
database:
  - schemaName: public
    tables:
      - table:
          tableName: projects
          security: {schemaPath: {dml: [dept], sal: [sal]}, alias: dept}
          columns:
            - column: {name: id, type: int, constraint: {primaryKey: true}}
            - column: {name: dept, type: text}
            - column: {name: sal, type: smallint}
      - table:
          tableName: docs
          security:
            schemaPath:
              dml: [public.docs.project_id, public.projects.dept]
              sal: [public.docs.project_id, public.projects.sal]
              review: [review_level]
            softDelete: deleted_at
          columns:
            - column: {name: id, type: int, constraint: {primaryKey: true}}
            - column: {name: project_id, type: int, constraint: {foreignKey: {references: public.projects}}}
            - column: {name: review_level, type: int}
            - column: {name: deleted_at, type: timestamptz}
      - table:
          tableName: notices
          security: {schemaPath: {sal: [sal]}}
          columns:
            - column: {name: id, type: int, constraint: {primaryKey: true}}
            - column: {name: sal, type: int}
"#;

/// [levels] from a schema YAML to the policies: the dml condition and every level's, all of them; a
/// level in the row's own column compared with the user's, one through a join inside an EXISTS (a
/// row whose path reaches no row is reached by nobody, a NULL level counts as 0); a table of levels
/// alone has no dml condition, alias or entity; the registry holds the levels
#[test]
fn levels_in_the_policies_and_the_registry() {
    let mut schemas = parsed(LEVELS);
    link(&mut schemas, true).expect("the file's checks, the levels' paths too");
    let dbc = catalog_of(&schemas);
    let d = declared(&schemas, SecurityMode::Rls);
    let of = |name: &str| d[&("public".to_string(), name.to_string())].clone();
    let projects = of("projects");
    let sql = projects.sql("public", "projects", &r_of(&projects, &dbc, "projects"), &Have::default(), false);
    assert!(sql.contains("FOR SELECT USING ((labac.any_id('dept', 'select') OR (projects.dept)::text = ANY (labac.ids('dept', 'select'))) \
        AND (labac.level('sal') >= 1 AND coalesce(projects.sal, 0) <= labac.level('sal')));"), "{sql}");
    assert!(sql.contains("FOR INSERT WITH CHECK ((labac.any_id('dept', 'insert') OR (projects.dept)::text = ANY (labac.ids('dept', 'insert'))) \
        AND (labac.level('sal') >= 1 AND coalesce(projects.sal, 0) <= labac.level('sal')))"), "a row is not made above the user's level: {sql}");
    assert!(sql.contains("'public', 'projects', 'dept', 'dept', NULL, true, 'public.projects.dept', '{\"sal\":\"sal\"}'"), "{sql}");
    assert!(sql.contains("labac:v6:public.projects.dept>:dept:::sal=public.projects.sal>'"), "the levels are in the signature: {sql}");
    // a level through a join, and one of the row's own, beside a dml path and a soft delete
    let docs = of("docs");
    assert_eq!(docs.alias.as_deref(), Some("dept"), "the dml alias is the entity's; a level has none");
    let r = r_of(&docs, &dbc, "docs");
    assert_eq!(r.paths["sal"].iter().map(|s| (s.table.as_str(), s.column.as_str(), s.key.as_str())).collect::<Vec<_>>(),
        vec![("docs", "project_id", ""), ("projects", "sal", "id")]);
    let sql = docs.sql("public", "docs", &r, &Have::default(), false);
    let levels = "(labac.level('review') >= 1 AND coalesce(docs.review_level, 0) <= labac.level('review')) AND \
        (labac.level('sal') >= 1 AND EXISTS (SELECT 1 FROM public.projects j1 WHERE j1.id = docs.project_id AND coalesce(j1.sal, 0) <= labac.level('sal')))";
    assert!(sql.contains(&format!("FOR DELETE USING (EXISTS (SELECT 1 FROM public.projects j1 WHERE j1.id = docs.project_id \
        AND (labac.any_id('dept', 'delete') OR (j1.dept)::text = ANY (labac.ids('dept', 'delete')))) AND {levels});")), "{sql}");
    assert!(sql.contains(&format!("AND (labac.any_id('dept', 'soft_delete') OR (j1.dept)::text = ANY (labac.ids('dept', 'soft_delete'))))) AND {levels}) WITH CHECK")),
        "the update: its soft delete, and the levels: {}", sql.lines().find(|l| l.contains("labac_update ON public.docs FOR")).unwrap_or(&sql));
    assert!(sql.contains("'{\"review\":\"review_level\",\"sal\":\"public.docs.project_id|public.projects.sal\"}'"), "{sql}");
    // levels alone
    let notices = of("notices");
    let r = r_of(&notices, &dbc, "notices");
    assert!(r.dml().is_empty() && r.alias.is_empty() && r.endpoint().is_empty());
    let sql = notices.sql("public", "notices", &r, &Have::default(), false);
    assert!(sql.contains("CREATE POLICY labac_select ON public.notices FOR SELECT USING ((labac.level('sal') >= 1 AND coalesce(notices.sal, 0) <= labac.level('sal')));"), "{sql}");
    let policies: Vec<&str> = sql.lines().filter(|l| l.contains("CREATE POLICY")).collect();
    assert!(policies.len() == 4 && policies.iter().all(|l| !l.contains("labac.ids(") && !l.contains("labac.any_id(")), "no dml condition: {policies:?}");
    assert!(sql.contains("'public', 'notices', '', '', NULL, true, '', '{\"sal\":\"sal\"}'"), "{sql}");
    // what the registry holds reads back; registered so, nothing is made again
    let row = serde_json::json!({ "schema_name": "public", "table_name": "notices", "column_name": "", "kind": "", "soft_delete": null, "rls": true,
        "endpoint": "", "levels": "{\"sal\":\"sal\"}" });
    let reg = from_registry(&row).expect("a table of levels alone");
    assert_eq!((reg.security(), reg.entity(), reg.defining()), (Security { rls: true, ..notices.clone() }, None, false));
    let sig = signature(&r, None, false);
    let have = Have { installed: true, registered: Some(reg), enabled: true, forced: true,
        policies: POLICIES.iter().map(|(_, n)| (n.to_string(), sig.clone())).collect(), ..Default::default() };
    assert_eq!(notices.sql("public", "notices", &r, &have, false), "", "as declared: nothing to do");
    let with: serde_json::Value = serde_json::json!({ "schema_name": "public", "table_name": "docs", "column_name": "public.docs.project_id|public.projects.dept",
        "kind": "dept", "rls": true, "endpoint": "public.projects.dept", "levels": "{\"review\":\"review_level\",\"sal\":\"public.docs.project_id|public.projects.sal\"}" });
    assert_eq!(from_registry(&with).unwrap().security().schema_path, docs.schema_path, "the registry's two columns are the one map again");
    for odd in ["{\"my level\":\"sal\"}", "{\"sal\":\"a b\"}", "[1]", "{\"sal\":7}"] {
        let mut row = with.clone();
        row["levels"] = serde_json::Value::String(odd.to_string());
        assert_eq!(from_registry(&row), None, "{odd}");
    }
    // under lsr the levels are registered, no policy is made
    let lsr = declared(&schemas, SecurityMode::Lsr(true))[&("public".to_string(), "docs".to_string())].clone();
    let sql = lsr.sql("public", "docs", &r_of(&lsr, &dbc, "docs"), &Have::default(), false);
    assert!(sql.contains("\"sal\":\"public.docs.project_id|public.projects.sal\"") && !sql.contains("POLICY"), "{sql}");
}

/// [levels] the file's checks (lsr with its traversal) cover a level's path as the dml one; a policy
/// that would read its own table through a level's path recurses as one through the dml path does
#[test]
fn a_levels_path_in_the_files_checks_and_recursion() {
    let refused = |from: &str, to: &str| { let mut s = parsed(&LEVELS.replace(from, to)); link(&mut s, true).unwrap_err() };
    let e = refused("review: [review_level]", "review: [public.docs.review_level, public.elsewhere.rank]");
    assert!(e.contains("public.elsewhere is not a table of this file") && e.contains("level review"), "{e}");
    assert!(refused("sal: [public.docs.project_id, public.projects.sal]", "sal: [public.docs.project_id, public.projects.nope]")
        .contains("nope is not a column of public.projects"));
    let e = refused("sal: [public.docs.project_id, public.projects.sal]", "sal: [public.docs.project_id, public.notices.sal]");
    assert!(e.contains("public.docs.project_id references public.projects, not public.notices") && e.contains("level sal"), "{e}");
    let mut s = parsed(&LEVELS.replace("sal: [public.docs.project_id, public.projects.sal]", "sal: [public.docs.project_id, public.elsewhere.sal]"));
    link(&mut s, false).expect("not lsr with its traversal: the database is asked at deploy");
    // a tree's level: the row's level is its parent's
    let tree = Security { rls: true, schema_path: named(&[("sal", &["public.nodes.parent_id", "public.nodes.sal"])]), ..Default::default() };
    assert_eq!(recurses("public", "nodes", &tree, &Declared::new(), &[]).as_deref(), Some("public.nodes"));
    assert_eq!(recurses("public", "nodes", &Security { schema_path: named(&[("sal", &["sal"])]), rls: true, ..Default::default() },
        &Declared::new(), &[]), None, "its own column reads no other table");
}

/// [levels] a declaration written back (a dump of a database, ytserv's schema map): the dml path alone
/// is a list, as it is read; with levels `schemaPath` is the map, `dml` first - and what is written
/// reads back the same. `rls` is the deploy's: never written
#[test]
fn a_declaration_is_written_as_it_is_read() {
    let read = |text: &str| Security::read(&YamlLoader::load_from_str(text).unwrap().remove(0)["security"]).unwrap().unwrap();
    let plain = read("security: {schemaPath: [dept], alias: dept, softDelete: deleted_at}");
    assert_eq!(serde_json::to_value(&plain).unwrap(), serde_json::json!({ "schemaPath": ["dept"], "alias": "dept", "softDelete": "deleted_at" }));
    let both = read("security: {schemaPath: {review: [review_level], dml: [public.docs.project_id, public.projects.dept], sal: [sal]}}");
    assert_eq!(serde_json::to_value(&both).unwrap(), serde_json::json!({ "schemaPath": {
        "dml": ["public.docs.project_id", "public.projects.dept"], "review": ["review_level"], "sal": ["sal"] } }));
    let text = serde_json::to_string(&both).unwrap();
    assert!(text.starts_with("{\"schemaPath\":{\"dml\":"), "dml first: {text}");
    let alone = read("security: {schemaPath: {sal: [sal]}}");
    assert_eq!(serde_json::to_value(&alone).unwrap(), serde_json::json!({ "schemaPath": { "sal": ["sal"] } }), "levels alone: no dml, no alias");
    for d in [&plain, &both, &alone] {
        assert_eq!(&read(&format!("security: {}", serde_json::to_string(d).unwrap())), d, "read back as written");
    }
    let rls = serde_json::to_value(decl(true)).unwrap();
    assert!(rls.get("rls").is_none() && rls.get("levels").is_none() && rls.get("level").is_none(), "{rls}");
    // a registry row comes out the same way: its levels are names of the map
    let row = serde_json::json!({ "schema_name": "public", "table_name": "docs", "column_name": "public.docs.project_id|public.projects.dept",
        "kind": "dept", "rls": true, "endpoint": "public.projects.dept", "levels": "{\"review\":\"review_level\",\"sal\":\"public.docs.project_id|public.projects.sal\"}" });
    let from = from_registry(&row).unwrap().security();
    assert_eq!(serde_json::to_value(&from).unwrap()["schemaPath"], serde_json::json!({ "dml": ["public.docs.project_id", "public.projects.dept"],
        "review": ["review_level"], "sal": ["public.docs.project_id", "public.projects.sal"] }));
}
