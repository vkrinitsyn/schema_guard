//! [mysql] The plan, from a schema and the tables MySQL has, with no server: what is made, what is
//! changed in place, what is refused. A live server's run is tests/mysql_samples.rs (SG_MYSQL_URL).
use super::*;

const BASE: &str = "
database:
  - schemaName: shop
    tables:
      - table:
          tableName: customers
          columns:
            - column:
                name: id
                type: int auto_increment
                constraint:
                  primaryKey: true
            - column:
                name: email
                type: varchar(80)
                constraint:
                  nullable: false
                index:
                  name: customers_email_key
                  unique: true
            - column:
                name: active
                type: tinyint(1)
                defaultValue: \"1\"
          data:
            - ['1', 'a@x.com', '1']
      - table:
          tableName: orders
          columns:
            - column:
                name: id
                type: bigint auto_increment
                constraint:
                  primaryKey: true
            - column:
                name: customer
                type: int
                constraint:
                  foreignKey:
                    references: shop.customers
            - column:
                name: placed
                type: datetime(6)
                defaultValue: CURRENT_TIMESTAMP(6)
";

fn schemas(text: &str) -> crate::utils::OrderedHashMap<Schema> {
    crate::parse_yaml_schema(crate::load_schema_from_src(text.to_string()).unwrap(), "S1__a.yaml").unwrap()
}

fn plan_of(text: &str, existing: &HashMap<String, BTreeMap<String, MyTable>>, opt: &MigrationOptions) -> Result<MysqlPlan, String> {
    let s = schemas(text);
    let named: Vec<(String, &Schema)> = s.list.iter().map(|x| (x.schema_name.clone(), x)).collect();
    plan_on(&named, existing, opt)
}

fn col(name: &str, ty: &str, nullable: bool, default: Option<&str>, extra: &str) -> MyColumn {
    MyColumn { name: name.into(), column_type: ty.into(), nullable, default: default.map(|d| d.into()), extra: extra.into(), position: 0 }
}

/// BASE as MySQL 8 keeps it once made: COLUMN_TYPE without a display width, a literal default bare
fn made() -> HashMap<String, BTreeMap<String, MyTable>> {
    let mut db = BTreeMap::new();
    db.insert("customers".into(), MyTable {
        name: "customers".into(),
        columns: vec![col("id", "int", false, None, "auto_increment"), col("email", "varchar(80)", false, None, ""),
                      col("active", "tinyint(1)", true, Some("1"), "")],
        primary_key: vec!["id".into()],
        indexes: vec![MyIndex { name: "customers_email_key".into(), unique: true, columns: vec!["email".into()] }],
        ..Default::default()
    });
    db.insert("orders".into(), MyTable {
        name: "orders".into(),
        columns: vec![col("id", "bigint", false, None, "auto_increment"), col("customer", "int", true, None, ""),
                      col("placed", "datetime(6)", true, Some("CURRENT_TIMESTAMP(6)"), "DEFAULT_GENERATED")],
        primary_key: vec!["id".into()],
        fks: vec![MyFk { name: "fk_orders_customers_customer".into(), column: "customer".into(), ref_schema: "shop".into(),
                         ref_table: "customers".into(), ref_column: "id".into() }],
        ..Default::default()
    });
    [("shop".to_string(), db)].into_iter().collect()
}

fn opt() -> MigrationOptions {
    MigrationOptions::default()
}

#[test]
fn types_and_defaults_compare_as_mysql_keeps_them() {
    for (a, b) in [("int(11)", "int"), ("INTEGER", "int"), ("tinyint(1)", "tinyint(1)"), ("bool", "tinyint(1)"),
                   ("decimal", "decimal(10,0)"), ("numeric(12, 2)", "decimal(12,2)"), ("varchar( 80 )", "varchar(80)"),
                   ("bigint unsigned", "bigint unsigned"), ("int auto_increment", "int"), ("double precision", "double")] {
        assert_eq!(norm_type(a), b, "{}", a);
    }
    for (a, b) in [("(CURRENT_DATE)", "curdate()"), ("curdate()", "curdate()"), ("'x'", "x"), ("'it''s'", "it's"),
                   ("CURRENT_TIMESTAMP(6)", "current_timestamp(6)"), ("now(6)", "current_timestamp(6)"), ("(uuid())", "uuid()")] {
        assert_eq!(norm_default(a), b, "{}", a);
    }
}

#[test]
fn a_schema_is_made_on_an_empty_server_and_its_foreign_keys_last() {
    let p = plan_of(BASE, &HashMap::new(), &opt()).unwrap();
    let s = &p.statements;
    assert_eq!(s[0], "CREATE DATABASE IF NOT EXISTS `shop`");
    assert!(s[1].starts_with("CREATE TABLE `shop`.`customers` (\n  `id` int auto_increment NOT NULL,"), "{}", s[1]);
    assert!(s[1].contains("`active` tinyint(1) DEFAULT 1"), "{}", s[1]);
    assert!(s[1].contains("PRIMARY KEY (`id`)"), "{}", s[1]);
    assert!(s.iter().any(|x| x == "CREATE UNIQUE INDEX `customers_email_key` ON `shop`.`customers` (`email`)"), "{:?}", s);
    assert!(s.iter().any(|x| x.starts_with("INSERT IGNORE INTO `shop`.`customers` (`id`, `email`, `active`) VALUES ('1', 'a@x.com', '1')")));
    let last = s.last().unwrap();
    assert_eq!(last, "ALTER TABLE `shop`.`orders` ADD CONSTRAINT `fk_orders_customers_customer` FOREIGN KEY (`customer`) \
                      REFERENCES `shop`.`customers` (`id`)");
}

#[test]
fn a_second_run_changes_nothing() {
    let p = plan_of(BASE, &made(), &opt()).unwrap();
    assert!(p.statements.is_empty(), "{:?}", p.statements);
}

#[test]
fn a_column_is_added_and_a_wider_type_or_another_default_is_modified_in_place() {
    let more = BASE.replace("                defaultValue: CURRENT_TIMESTAMP(6)\n",
        "                defaultValue: CURRENT_TIMESTAMP(6)\n            - column:\n                name: note\n                type: text\n")
        .replace("type: varchar(80)", "type: varchar(200)")
        .replace("defaultValue: \"1\"", "defaultValue: \"0\"");
    let p = plan_of(&more, &made(), &opt()).unwrap();
    assert!(p.statements.contains(&"ALTER TABLE `shop`.`customers` MODIFY COLUMN `email` varchar(200) NOT NULL, \
                                    MODIFY COLUMN `active` tinyint(1) DEFAULT 0".to_string()), "{:?}", p.statements);
    assert!(p.statements.contains(&"ALTER TABLE `shop`.`orders` ADD COLUMN `note` text".to_string()), "{:?}", p.statements);
}

#[test]
fn a_narrower_type_needs_with_size_cut_and_a_changed_key_or_index_with_index_drop() {
    let narrow = BASE.replace("type: varchar(80)", "type: varchar(20)");
    assert!(plan_of(&narrow, &made(), &opt()).unwrap_err().contains("set with_size_cut"));
    let cut = MigrationOptions { with_size_cut: true, ..MigrationOptions::default() };
    assert!(plan_of(&narrow, &made(), &cut).unwrap().statements[0].contains("MODIFY COLUMN `email` varchar(20)"));
    let skip = MigrationOptions { without_failfast: true, ..MigrationOptions::default() };
    let p = plan_of(&narrow, &made(), &skip).unwrap();
    assert!(p.statements.is_empty() && p.skipped.iter().any(|s| s.contains("needs with_size_cut")), "{:?}", p);

    let not_unique = BASE.replace("                  unique: true\n", "                  unique: false\n");
    assert!(plan_of(&not_unique, &made(), &opt()).unwrap_err().contains("set with_index_drop"));
    let drop = MigrationOptions { with_index_drop: true, ..MigrationOptions::default() };
    let p = plan_of(&not_unique, &made(), &drop).unwrap();
    assert_eq!(p.statements, vec!["DROP INDEX `customers_email_key` ON `shop`.`customers`".to_string(),
                                  "CREATE INDEX `customers_email_key` ON `shop`.`customers` (`email`)".to_string()]);

    let mut no_key = made();
    no_key.get_mut("shop").unwrap().get_mut("orders").unwrap().primary_key = vec!["customer".into()];
    assert!(plan_of(BASE, &no_key, &opt()).unwrap_err().contains("primary key"));
}

#[test]
fn nullability_changes_in_place_and_a_column_the_schema_does_not_name_is_kept() {
    let mut have = made();
    have.get_mut("shop").unwrap().get_mut("orders").unwrap().columns.push(col("extra", "text", true, None, ""));
    let not_null = BASE.replace("                type: int\n", "                type: int\n                constraint:\n                  nullable: false\n                  foreignKey:\n                    references: shop.customers\n")
        .replacen("                constraint:\n                  foreignKey:\n                    references: shop.customers\n", "", 1);
    let p = plan_of(&not_null, &have, &opt()).unwrap();
    assert_eq!(p.statements, vec!["ALTER TABLE `shop`.`orders` MODIFY COLUMN `customer` int NOT NULL".to_string()]);
    assert!(!p.statements.iter().any(|s| s.contains("extra")));
}

#[test]
fn what_mysql_has_not_is_refused_or_left_out() {
    let trig = BASE.replace("                defaultValue: CURRENT_TIMESTAMP(6)\n",
        "                defaultValue: CURRENT_TIMESTAMP(6)\n          triggers:\n            - trigger:\n                name: t1\n                event: after insert\n                when: for each row\n                proc: app.audit()\n");
    assert!(plan_of(&trig, &HashMap::new(), &opt()).unwrap_err().contains("its triggers are a function's"));
    let skip = MigrationOptions { exclude_triggers: true, ..MigrationOptions::default() };
    assert!(plan_of(&trig, &HashMap::new(), &skip).is_ok());
    let part = BASE.replace("                type: datetime(6)\n", "                type: datetime(6)\n                partition_by: RANGE\n");
    assert!(plan_of(&part, &HashMap::new(), &opt()).unwrap_err().contains("a partition"));
    let granted = BASE.replace("          data:\n", "          grant:\n            - all: app_rw\n          data:\n");
    assert!(plan_of(&granted, &HashMap::new(), &opt()).unwrap().skipped.iter().any(|s| s.contains("its grants")));
}

#[test]
fn a_postgres_schema_mapped_to_mysql_is_planned() {
    let pg = "
source: postgres.16
database:
  - schemaName: shop
    tables:
      - table:
          tableName: items
          columns:
            - column:
                name: id
                type: bigserial
                constraint:
                  primaryKey: true
            - column:
                name: title
                type: varchar(120)
                sql: UNIQUE
            - column:
                name: active
                type: boolean
                defaultValue: \"true\"
            - column:
                name: added
                type: timestamptz
                defaultValue: now()
";
    let y = yaml_rust2::YamlLoader::load_from_str(pg).unwrap().remove(0);
    let m = crate::mapping::Mapping::builtin("postgres", "mysql").unwrap().unwrap();
    let out = m.apply(&y, &crate::mapping::Source::parse("mysql.8.0").unwrap()).unwrap();
    let s = crate::parse_yaml_schema(out.schema, "S1__a.yaml").unwrap();
    let named: Vec<(String, &Schema)> = s.list.iter().map(|x| (x.schema_name.clone(), x)).collect();
    let p = plan_on(&named, &HashMap::new(), &opt()).unwrap();
    let create = p.statements.iter().find(|x| x.starts_with("CREATE TABLE")).unwrap();
    assert!(create.contains("`id` bigint auto_increment NOT NULL"), "{}", create);
    assert!(create.contains("`added` datetime(6) DEFAULT CURRENT_TIMESTAMP(6)"), "{}", create);
    assert!(p.statements.iter().any(|x| x == "CREATE UNIQUE INDEX `items_title_key` ON `shop`.`items` (`title`)"), "{:?}", p.statements);
}
