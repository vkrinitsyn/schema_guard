## SchemaGuard

### Merging YAML defined schema and data into PostgreSQL database

Create a [YAML](yaml.md) file:

```yaml
database:
  - schema:
    schemaName: test_schema
    owner: postgres
    tables:
      - table:
          tableName: test_table
          description: The new table
          constraint: -- some SQL as table constraint
          sql: -- some SQL suffix on table create
          columns:
            - column:
                name: id
                type: serial
                defaultValue:
                constraint:
                  primaryKey: true
                  nullable: false
                  foreignKey:
                    references: --fk_table
                    sql: -- some SQL suffix on new FK create, like- on delete no action on update no action
                description:
                sql: -- some SQL suffix on new column create
            - column:
                name: test
                type: varchar(250)
                index: true
          triggers:
            - trigger:
                name: uniq_name_of_trigger
                event: before update
                when: for each row
                proc:
```
See [examples](test/example.yaml) and [template](test/example_template.yaml)

One line of code:

```rust
    let _ = schema_guard::migrate1(schema_guard::load_schema_from_file("file.yaml").unwrap(), "postgresql://")?;

```

Will create or upgrade existing Postgres database schema with desired tables without extra table creation.


> [!NOTE]
Not recommended to integrate schema migrate into application for production use
as such violate security concern and best practices.

