## SchemaGuard (tokio)

> [!NOTE]
> Standard non tokio edition available in /std branch:  https://github.com/vkrinitsyn/schema_guard/tree/std

> [!TIP]
> **Schema Guard Enterprise (`sge`)** is the complete tool built on this crate, and
> just as free: MIT, no license key. It adds a command line, versioned SQL scripts
> tracked in a history table, dry runs, ETL snapshots and loads, git as a script
> source, and YT Cluster support. See [dbinvent.com](https://www.dbinvent.com).

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
> [!NOTE]
This is a tokio implementation. for a standard - take a look at `std` branch of this repo.

> [!IMPORTANT]
> 2.0.0 parses YAML with [yaml-rust2](https://crates.io/crates/yaml-rust2), as yaml-rust
> is unmaintained: `migrate`, `load_schema_from_file`, `parse_yaml_schema` and the
> other functions that took or returned `yaml_rust::Yaml` now use `yaml_rust2::Yaml`.
> Files are still checked against `schema.yaml` by
> [yaml-validator](https://github.com/MathiasPius/yaml-validator) 0.2.0, vendored in
> `src/yaml_validator` (MIT, its LICENSE beside it) and ported to yaml-rust2.


Will create or upgrade existing Postgres database schema with desired tables without extra table creation.


> [!NOTE]
Feel free to take a look at Schema Guard Enterprise (`sge`), the full-featured
Postgres schema management tool built on this crate - just as free: [dbinvent.com](https://www.dbinvent.com)

### Checks

```shell
cargo audit      # known vulnerabilities (RustSec) in the dependencies Cargo.lock pins
cargo geiger     # unsafe code in schema_guard_tokio and in each dependency it uses
```

`cargo install cargo-audit cargo-geiger` installs them; with cargo-make,
`cargo make audit` and `cargo make geiger` run the same. A library keeps no `Cargo.lock` in git, so the audit reads the one the
last build resolved; `cargo generate-lockfile` makes one. geiger reads
every dependency file in the target directory, so give it one of its own
(`CARGO_TARGET_DIR=target/geiger`), or an old build's can name a source file
that is gone.
