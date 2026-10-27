## SchemaGuard (tokio)

> [!NOTE]
> Standard non tokio edition available in `std` branch:  https://github.com/vkrinitsyn/schema_guard/tree/std

> [!TIP]
> Also available the **Schema Guard Enterprise  ([sge](https://dbinvent.com/sge))** - is the complete tool built on this
> [crate](https://crates.io/crates/schema_guard), and
> just as free: MIT, no license key. It adds a command line, versioned SQL scripts
> tracked in a history table, dry runs, ETL snapshots and loads, git as a script
> source, and YT Cluster support. See [dbinvent.com](https://dbinvent.com).

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
> 2.2.0 reads a live database's table and column comments (`loader`), which no version
> before it did, and a database with a mixed-case table name (`"User"`) no longer fails
> that read (SG-E021). A comment with a `'` in it is written correctly.

> [!IMPORTANT]
> 2.0.0 parses YAML with [yaml-rust2](https://crates.io/crates/yaml-rust2), as yaml-rust
> is unmaintained: `migrate`, `load_schema_from_file`, `parse_yaml_schema` and the
> other functions that took or returned `yaml_rust::Yaml` now use `yaml_rust2::Yaml`.
> Files are still checked against `schema.yaml` by
> [yaml-validator](https://github.com/MathiasPius/yaml-validator) 0.2.0, vendored in
> `src/yaml_validator` (MIT, its LICENSE beside it) and ported to yaml-rust2.


Will create or upgrade existing Postgres database schema with desired tables without extra table creation.

### Apache Superset datasets

Since 2.2.0 the same schema - or a live database's tables - comes out as the bundle
Apache Superset imports (Datasets, Import), the v1 export format of Superset 2.1 and
later: one database, a dataset for each table with every column and its SQL type, a time
column marked for charts, and the `count` metric. A template table is left out.

```rust
use schema_guard_tokio::superset::{zip, SupersetDatabase};

let db = SupersetDatabase::new("app", "postgresql://app:secret@db.local:5432/app");
let files = schema_guard_tokio::superset_bundle(
    schema_guard_tokio::load_schema_from_file("schema.yaml")?, "schema.yaml", &db)?;
std::fs::write("app_superset.zip", zip(&files, "dataset_export")?)?;
```

- The password in the URI is written masked, as Superset's own export writes it, and
  the import's web form asks for it - the connection's password only: another secret
  among its query parameters (`sslpassword`) is written as it is, as Superset writes it. `superset import-datasources -p <file>.zip` has no
  way to give one: there, name a connection Superset has already by its UUID -
  `SupersetDatabase::existing(name, uri, uuid)` - which the import keeps as it is and
  asks no password for.
- Without a UUID, the connection's is made from its name, and a dataset's from the
  connection's and the table's, so a bundle exported again imports over its datasets.
  A connection Superset already holds under that name is taken over.
- An import takes over a dataset Superset holds for the same table of that connection -
  one made by hand too, on the first import, with no question asked: its descriptions,
  labels and time column are reset, and with the overwrite the command line always
  uses, its own metrics and calculated columns are deleted. Leave such tables out of
  the bundle: `superset::datasets_of_schemas` (or `datasets_of_info`), `retain` the ones
  to import, then `superset::bundle`.
- `superset_bundle_db(&mut tx, &db)` does the same for the tables of the database a
  transaction runs in; `files` is each file's path and YAML, for a caller that writes
  them out itself, and `superset::bundle` takes a fixed time for a bundle that comes
  out the same every time.
- `cargo run --example superset_bundle -- <schema.yaml> <name> <uri> <out.zip>` runs the
  lines above.

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
