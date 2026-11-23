## SchemaGuard (tokio)

[![ci](https://github.com/vkrinitsyn/schema_guard/actions/workflows/ci.yml/badge.svg)](https://github.com/vkrinitsyn/schema_guard/actions/workflows/ci.yml)
[![coverage](https://raw.githubusercontent.com/vkrinitsyn/schema_guard/badges/coverage.svg?sanitize=true)](https://github.com/vkrinitsyn/schema_guard/actions/workflows/ci.yml)
[![audit](https://github.com/vkrinitsyn/schema_guard/actions/workflows/audit.yml/badge.svg)](https://github.com/vkrinitsyn/schema_guard/actions/workflows/audit.yml)
[![crates.io](https://img.shields.io/crates/v/schema_guard_tokio.svg)](https://crates.io/crates/schema_guard_tokio)
[![docs.rs](https://docs.rs/schema_guard_tokio/badge.svg)](https://docs.rs/schema_guard_tokio)
[![license](https://img.shields.io/crates/l/schema_guard_tokio.svg)](LICENSE)

> [!NOTE]
> Standard non tokio edition available in `std` branch:  https://github.com/vkrinitsyn/schema_guard/tree/std

> [!TIP]
> Also available the **Schema Guard Enterprise  ([sge](https://dbinvent.com/sge))** - is the complete tool built on this
> [crate](https://crates.io/crates/schema_guard), It adds a command line, versioned SQL scripts
> tracked in a history table like Flyway *plus*, dry runs, ETL: snapshots and loads, git as a script
> source, Apache Superset schema format dump and `plan` phase like Terraform,
> All provided for free by [DBinvent.com](https://dbinvent.com) the YT Cluster - 
> The Infinite Scale Postgres cluster solution.

### Features: a database each

One feature a database, PostgreSQL's by default (`default = ["postgres"]`): its driver, its catalog
read into the schema's model and the changes deployed to it. Each database's tool is a binary of
its own, built with its feature (sge is PostgreSQL's). Without one, the model alone: a YAML schema
read, checked and compared with a loaded one - no database read or changed. Since 2.4.0: a crate
that leaves the default features out names `postgres` to keep what 2.x gave it.

```toml
schema_guard_tokio = "2.4"                                                      # PostgreSQL
schema_guard_tokio = { version = "2.4", default-features = false, features = ["postgres", "slog"] }
schema_guard_tokio = { version = "2.4", default-features = false, features = ["sqlite"] }    # SQLite
schema_guard_tokio = { version = "2.4", default-features = false, features = ["mysql"] }     # MySQL
```

Another vendor's schema is converted on apply, for its target only: a PostgreSQL dump applied to
PostgreSQL is applied as it is; a schema with no `source` is PostgreSQL's; custom SQL is ignored on
a mapping to another vendor (`yaml.md`).

`mysql` (through sqlx's MySQL driver, pure Rust; async): `schema_guard_tokio::mysql::migrate` deploys a YAML
schema to a `MySqlConnection`, `plan` says what it would run, `load_info_schema` reads the catalog. A
schema is a MySQL database (`public` the connection's current one); changes are made in place by
`ALTER TABLE`. MySQL commits each DDL by itself: everything is planned before the first statement
runs, and a statement that fails says how many ran before it.

`sqlite-core` (since 2.4.2): the same engine on a SQLite connection of your own, which implements
`sqlite::SqliteDb` (a query's rows, a batch, whether outside a transaction).

`sqlite` (SQLite embedded, through turso as yt-lite uses it; async): `schema_guard_tokio::sqlite::migrate` deploys a YAML
schema to a connection, `plan` says what it would run, `load_info_schema` reads the catalog into the
model. A schema is `main` (as `public` is read too) or a database attached by that name. What SQLite's
ALTER TABLE cannot do is done by its own table rebuild, in one transaction, foreign keys checked;
nothing is dropped, and a narrower type needs `with_size_cut`, as on PostgreSQL.

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

### Row access: security (2.5.0)

A table may say who sees which of its rows - by a value the user's attributes are compared with,
as signon's labac gives a user, per attribute and action, the values it may use (the actions and
their levels - select 1, insert 2, update 3, soft delete 4, delete 5, ddl 6 - are signon's: no
part of a table):

```yaml
      - table:
          tableName: orders                       # defines the entity public.orders.dept
          security: {schemaPath: [dept], alias: dept, softDelete: deleted_at}
      - table:
          tableName: order_items                  # follows its order: the alias is the entity's
          security: {schemaPath: [public.order_items.order_id, public.orders.dept]}
      - table:
          tableName: projects                     # the attribute is called project, the column code
          security: {schemaPath: [code], alias: project}
      - table:
          tableName: tasks
          security: {schemaPath: [public.tasks.project_id, public.projects.code]}
```

- **`schemaPath`** is where the value is, as `schema.table.column` steps (`table.column` in the
  table's schema; the first step may be the column alone): one column of the table, or the join
  hops to one - each step's column leading to the next step's table by that table's one-column
  primary key. The last step's column is the **entity** the path ends at.
- **`alias`** is the name of the user's attribute (signon's resource kind). It is said once, by the
  table that declares the entity as its own column - left out there, it is the column's name. A
  table whose path reaches the entity takes its alias and does not repeat it; one that says
  another is refused. A path to an entity nobody defines takes the alias it says, else the last
  column's name.
- **The deploy's mode** decides what is made of the declarations, for every table of the deploy -
  a table does not say its own (`MigrationOptions::security`, a `SecurityMode`):
  - `Off`, the default: the declarations are ignored - nothing installed, registered or changed.
  - `Lsr(traversal)`: the tables are registered for ytproxy's rewrite, with no policy. Nothing in
    the database tests such a path, so `Lsr(true)` checks the file before anything is changed:
    every step is a column of a table defined in the same file, each hop's table has a one-column
    primary key, a foreign key the YAML declares on a hop's column points at the next step's
    table, and the aliases are unique - an alias names one entity, an entity has one alias (the
    children of one template count as one definition). `Lsr(false)` leaves the checks out, for a
    schema spread over several files (sge): the database's registry then answers for an entity's
    alias - the defining file deployed first, or the alias said.
  - `Rls`: row-level security policies on every declared table; the database tests the paths.

  A mode read from text (`"rls".parse()`) is `lsr` (its traversal checked) or `rls`; anything
  else is `Off`.
- **`softDelete`** is the column a soft delete sets: an UPDATE that sets it, or touches a row it
  marks, needs the soft delete values.
- **`schemaPath` as a map** names several paths of one table: `dml` - the path above - and the
  row's **levels**, each by its name: `sal`, the security access level, or any name of the schema's
  own. A level is a second kind of access beside the dml one, and ANDed with it: the user's level
  of that name must be the same as or higher than the row's, whatever department the user is of.
  A level's path is as the dml one - the row's own column, or the hops to one (a document's level
  is its project's). A list is the short form of `dml` alone; `dml` may be left out of the map: a
  table may be covered by levels alone (no `alias`, no `softDelete` then).

```yaml
          security: {schemaPath: {dml: [dept], sal: [sal]}, alias: dept}    # by department, and by level
          security:                                                         # a document: its project's
            schemaPath:                                                     # department and level, and a
              dml: [public.docs.project_id, public.projects.dept]           # review level of its own
              sal: [public.docs.project_id, public.projects.sal]
              review: [review_level]
          security: {schemaPath: {sal: [sal]}}                              # by level alone
```

  A user with no level of a name (0) reaches no row of a table that declares it; a row whose level
  is NULL counts as 0; a level's column is a whole number. A row whose path to a level reaches no
  row is reached by nobody. A row is not made, or moved, above the user's level (the INSERT and
  UPDATE checks).

schema_guard deploys the declarations after every table of the file exists, the defining ones
first (`migrate` does; a program that deploys table by table calls `security::link` on the parsed
file and `security::deploy_all` after its tables, as sge does). It installs the SQL part once (schema `labac`: the key table, `labac.enter(token)`,
`labac.ids(alias, action)`, the registry `labac.tables` - the path, the alias, the entity),
registers the table - its levels with it - and grants `labac.enter` to the table's grantees. The
SQL part has `labac.level(name)`, the level of a name the entered token holds, and
`labac.any_id(alias, action)`: the action given on every resource of the alias, whatever the row's
value (signon's entry on `*`) - then only the levels are compared. Under `Rls` it enables
and forces row-level security and makes a policy per action: a row is reached when the value at
its path's end is one of `labac.ids(alias, action)` for the token the session entered - through an
EXISTS over the path's joins, whose keys come from the catalog at that moment (nothing of them is
stored). **A hidden parent denies:** PostgreSQL applies a table's policies to a policy that reads
it, so a row that follows a path through another declared table is reached only when that table's
row is read too - its department for a select, and its levels, also the ones the following table
does not declare (a task of a project above the user's level is hidden). ytproxy's lsr rewrite
does the same. A user with the action on every resource of the alias is asked nothing of the path. A defining table whose alias changes carries it to the registered tables that reach it:
their registry rows and their policies. A path whose policy would come back to its own table (a
tree, or two tables reading each other) is refused under `Rls`: such a schema deploys under lsr.
Under `Lsr` a table is only registered - ytproxy rewrites the statements instead - and labac's
policies are taken off where an earlier deploy made them. An alias that contradicts its entity's
is refused at deploy under either mode. A table's `data` rows go in with FORCE lifted
for them wherever the table's row-level security is forced - also when this deploy takes it off,
which it does after the rows. A template's declaration is each child's own: a step that names the
template's table names the child's. A database an earlier schema_guard deployed is read as it is:
its registry answers for the aliases before the SQL part is brought up to date. The key itself (`labac.key`) is never in a
YAML: YT Cluster's ytserv writes it.

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
