# agent.md - schema_guard_tokio

A guide for coding agents (and people) who use this crate or change it. Each
statement below was checked against the source in `src/` for version 2.4.0.

## What it is

`schema_guard_tokio` turns a YAML description of PostgreSQL schemas and tables
into the DDL that brings a live database up to it, and runs that DDL over
`tokio-postgres`. It is the async sibling of the `schema_guard` crate, which
uses the blocking `postgres` client. The two share the YAML format and the
rules.

The model is **declarative and append-only**. The YAML says what must exist,
not the complete state of the database:

- **Absence means unchanged.** A column, primary key, index, trigger or grant
  that exists in the database but is not in the YAML is left alone; no `DROP`
  is generated for it. A partial table definition is therefore normal: it
  means "make sure these columns exist".
- **Only an explicitly different declaration is a change.** Changes that can
  lose data or access are refused unless an option allows them (see
  `MigrationOptions`).

So one YAML file can be applied to an empty database, to an old one, or twice
in a row, and each time it produces only what is missing.

## Features: a database each (since 2.4.0)

| feature | what it brings |
|---|---|
| `postgres` (default) | tokio-postgres: `migrate*`, `migrate_plan`, `loader::load_info_*`, `pg_error*`, `set_lc_messages`, `superset_bundle_db`, the `_pg` deploys |
| `mysql` | sqlx's MySQL driver (pure Rust, tokio), async: `mysql::{migrate, plan, plan_on, load_info_schema}` on a `MySqlConnection` |
| `sqlite` | turso, the embedded SQLite yt-lite uses (no system library), async: `sqlite::{migrate, plan, load_info_schema}` on a `turso::Connection` |
| `slog` | executed SQL logged at debug |

With no feature the crate still parses, validates and maps YAML (`parse_yaml_schema`,
`mapping`) and builds the model; nothing connects. A user of the defaults gets what 2.x
gave. With `default-features = false`, name `postgres` to keep it. One binary per database
is the plan (sge-sqlite and the like), each with its feature only.

## Using it

### The rule

Do not write `CREATE TABLE IF NOT EXISTS` or `ALTER TABLE ... ADD COLUMN IF NOT
EXISTS` by hand next to a schema_guard schema. A hand-written `CREATE` is a
no-op on an existing table, so every new column then needs a second,
hand-maintained `ALTER`, and the two drift. Add the column to the YAML and let
the crate generate the DDL.

### API (`src/lib.rs`)

| function | what |
|---|---|
| `load_schema_from_file(path) -> Result<Yaml, String>` | reads a file and **validates** it against the built-in schema (`src/schema.yaml`, via `yaml-validator`) |
| `load_schema_from_src(text) -> Result<Yaml, String>` | the same for a string, e.g. an `include_str!` with placeholders already substituted |
| `migrate1(yaml, db_url) -> Result<usize, String>` | connects (`NoTls`), opens **one transaction**, applies with default options, commits. Returns the number of tables changed |
| `migrate_opt(yaml, db_url, &MigrationOptions)` | the same with options |
| `migrate(yaml, &mut Transaction, dry_run, file_name, &opt)` | applies inside **your** transaction (you commit). `dry_run: Some(&f)` hands each table's SQL to `f` instead of executing it. `file_name` only labels errors |
| `parse_yaml_schema(yaml, file_name)` | parse only, no database: `OrderedHashMap<Schema>`, with templates resolved |
| `loader::load_info_schema(db_name, &mut Transaction)` | introspection: what the database has now, as `InfoSchemaType` (schema → table → `PgTable`: columns, PK, FKs, indexes, triggers, grants, owner, and since 2.2.0 the table and column comments - before, none was ever read, and a mixed-case table name failed the read) |
| `set_logger(slog::Logger)` | feature `slog` only: executed SQL is logged at debug |
| `superset_bundle(yaml, file_name, &SupersetDatabase)` | since 2.2.0: the parsed schema's tables (templates left out) as Apache Superset's v1 import bundle - `Vec<(path, YAML)>`: `metadata.yaml`, `databases/<name>.yaml`, `datasets/<name>/<schema>.<table>.yaml`; no file name begins with `.` or `_` (Superset's import passes over those) |
| `superset_bundle_db(&mut Transaction, &SupersetDatabase)` | the same for the live database's tables, read by `load_info_schema` |
| `superset::zip(&files, root) -> Result` | the bundle as the ZIP Superset imports: under one folder (Superset drops the first path part), stored, CRC-32; SG-E072 past 65,535 files or 4 GiB (no ZIP64) |
| `SupersetDatabase::new(name, uri)` / `::existing(name, uri, uuid) -> Result` | the connection: by a UUID made from its name (made, or one of that name taken over), or one Superset has already (lower-cased; SG-E073 for no UUID), kept as it is - the only way `superset import-datasources` works, as it cannot give the masked password; a `password=` query parameter is moved into the user info, masked; `{:?}` masks it too. An import takes over a dataset of the same table, a hand-made one too - leave such tables out with `datasets_of_*` + `retain` |
| `superset::datasets_of_schemas(&[Schema])` / `datasets_of_info`, `superset::bundle(datasets, db, SystemTime)` | the steps apart, e.g. to filter the tables; a fixed time makes the bundle the same every time. Re-imported, a bundle resets what was edited of its datasets in Superset |

`migrate1` / `migrate_opt` connect without TLS. For TLS, or to apply several
files atomically, open the transaction yourself and call `migrate`.

### SQLite (`src/sqlite.rs`, feature `sqlite`)

Through turso (`=0.9.0-pre.5`), the embedded SQLite yt-lite uses (owner, 2026-11-19): async, on a
`turso::Connection`; a connection that attaches databases is built with `experimental_attach(true)`.
An index, trigger or view made again in an attached database is qualified with it.

| function | what |
|---|---|
| `sqlite::plan(yaml, &Connection, file_name, &opt).await -> SqlitePlan` | the statements, nothing run: `statements`, `rebuilt` (tables rebuilt, and why), `skipped` (left out, and why) |
| `sqlite::migrate(yaml, &Connection, plan_only, file_name, &opt).await` | planned, then run in **one transaction** of its own (refused when the connection already is in one); foreign keys off around a rebuild and checked by hand before the commit (turso has no `foreign_key_check`) |
| `sqlite::load_info_schema(&Connection).await` | the catalog (`sqlite_schema`, `PRAGMA table_xinfo` / `index_list` / `foreign_key_list`) into the same `InfoSchemaType` as PostgreSQL's loader: a schema per database of the connection |

- A schema is a database of the connection: `main` (also for `public`), or one the caller
  `ATTACH`ed under that name; any other name is refused.
- What SQLite's `ALTER TABLE` cannot do (a type, NOT NULL, default, key or FK changed; a
  column that is a key, unique, of its own `sql`, NOT NULL without a constant default) is
  SQLite's own table rebuild: the new table under `_sg_new_<t>`, rows copied, the old one
  dropped, renamed - its views dropped before and made again after - indexes and
  triggers made again. Kept from the old `CREATE TABLE`: a column the YAML does not name,
  whole; a named one's UNIQUE, CHECK, COLLATE, AUTOINCREMENT; the table's constraints and
  options (`STRICT`, `WITHOUT ROWID`); AUTOINCREMENT's counter.
- As on PostgreSQL: nothing dropped, a narrower or incompatible type needs `with_size_cut`,
  a changed index `with_index_drop`, failfast unless `without_failfast`.
- Not in SQLite: partitions (refused), PostgreSQL's function triggers (refused unless
  `exclude_triggers`), owners and grants (left out, in `skipped`).

### MySQL (`src/mysql.rs`, feature `mysql`)

| function | what |
|---|---|
| `mysql::plan(yaml, &mut MySqlConnection, file_name, &opt).await -> MysqlPlan` | the statements, nothing run: `statements`, `skipped` |
| `mysql::plan_on(&[(database, &Schema)], &existing, &opt)` | the same from a model of what MySQL has, no connection: what the unit tests drive |
| `mysql::migrate(yaml, &mut MySqlConnection, plan_only, file_name, &opt).await` | planned in full, then run statement by statement |
| `mysql::load_info_schema(&mut MySqlConnection, &[databases]).await` | information_schema (TABLES, COLUMNS, STATISTICS, KEY_COLUMN_USAGE, TRIGGERS) into `InfoSchemaType`: a schema per database; none named, every one but MySQL's own |

- A schema is a MySQL database, made when not there; `public` is the connection's current one.
- sqlx 0.9 with TLS (rustls) and without RSA (RUSTSEC-2023-0071 in sqlx 0.8's rsa): MySQL 8's
  default sign-in, caching_sha2_password, needs TLS over TCP - a server makes its certificate by
  itself - or a Unix socket.
- In place, by `ALTER TABLE`: ADD COLUMN, MODIFY COLUMN (type, nullability, default,
  auto_increment), ADD / DROP PRIMARY KEY, indexes; foreign keys added after every table.
  Compared as MySQL keeps them: an integer's display width dropped, its synonyms (`bool`,
  `integer`, `decimal`) by MySQL's names, a literal default bare, `CURRENT_DATE` as `curdate()`.
- **DDL is not transactional in MySQL:** every refusal is found while planning, before any
  statement runs; one that fails then stops the run and says how many ran before it.
- Not in MySQL: PostgreSQL's function triggers (refused unless `exclude_triggers`), partitions
  (refused), grants and owners (left out, in `skipped`).
- `tests/mysql_samples.rs`: every sample planned for MySQL with no server; deployed to a live one,
  read back and applied again with nothing to do when asked for -
  `SG_MYSQL_URL=mysql://user:pw@host/db cargo test --features mysql --test mysql_samples -- --ignored`
  (2026-11-19: passed on a throwaway `mysql:8.0` container, with an in-place change test).

### Source and mapping (`src/mapping.rs`, `src/mappings/`)

**The rule** (owner, 2026-11-19): a schema is converted on apply, for its target
only - a PostgreSQL dump applied to PostgreSQL is applied as it is, nothing mapped. A schema
written by hand, with no `source`, is PostgreSQL's. Custom SQL is ignored on a mapping to
another vendor.


A schema's top-level `source: vendor.version` (e.g. `postgres.16`) says what wrote it; sge
writes it on a snapshot. A mapping turns a schema of one vendor into another's, declared in
YAML: a default set of rules plus version cases, the nearest case at or below the target's
version applying (`mysql.5` covers 5.7; an empty `mysql.8` stops it at 8).

| item | what |
|---|---|
| `mapping::Source::parse("mysql.8.0")`, `Source::of(&yaml)` | a side: vendor and version |
| `Mapping::builtin(from, to)` | the built-in files `src/mappings/<from>-<to>.yaml`, each way between `postgres`, `mysql` and `sqlite` |
| `Mapping::parse(text)` | one of your own |
| `mapping.apply(&yaml, &target) -> Result<Mapped{schema, notes}, Vec<String>>` | the schema rewritten, with a note for each thing dropped or changed; `Err` lists every refusal at once |

- Value rules (types, defaults: a regex with captures, `to` / `keep` / `refuse`, an
  optional `note`, for a default an optional column `type` filter) and feature rules at
  fixed places (index method, concurrently, nulls, collate, partition, triggers, grants,
  owners, data: `keep` / `drop` / `refuse`).
- Custom SQL of the schema (`sql`, `constraint`) is the source vendor's: removed and noted on
  a cross-vendor apply. Its keywords `UNIQUE`, `NOT NULL`, `NULL`, `PRIMARY KEY` in a
  column's `sql` become schema fields first (`index.unique` named `<table>_<column>_key`,
  `constraint.nullable`, `constraint.primaryKey`).
- **The matrix** (`src/mappings/matrix.yaml`): a section per source vendor, in it a row per
  type, default or feature of that vendor's, a column per target (e.g. `postgres.16`,
  `mysql.8.0`, `mysql.5.7`, `sqlite.3`). Two quick tests read
  it: every cell holds, and every rule of every mapping is reached by a row - a rule added
  with no row fails. Add a row with every rule.
- Nothing calls `apply` on a deploy yet: the per-database binaries will.

### Errors and language (`src/error.rs`, `src/i18n.rs`)

| function | what |
|---|---|
| `Table::deploy_pg`, `deploy_fk_pg`, `Schema::deploy_all_tables_pg`, `deploy_all_fk_pg` | as the `String` versions, but return `DeployError`: `Pg { step, sql, file, err }` keeps the native `tokio_postgres::Error` (also its `source()`), `Message(String)` is schema_guard's own |
| `pg_error(&err)` | a Postgres error in full: severity, SQLSTATE, message, position, DETAIL, HINT, WHERE; a client error with its cause chain |
| `pg_error_in(&err, sql)` | the same, with the line of `sql` the server points at and a `^` under it, as psql prints it |
| `set_lang(lang)` | the language of schema_guard's messages; else the logger's `o!("lang" => ..)`, else `LANG`, else English. An empty value counts as unset. **Global** to the process, not per call |
| `set_lc_messages(&client, lang)` | asks the server for its messages in `lang` when the role may set `lc_messages`; ignores a refusal. Call it **before** `BEGIN`: a refused `SET` aborts an open transaction |

Error codes (`SG-Ennn`) are the same in every language.

Placeholders are the caller's job. For example, a schema stored with
`@extschema@` gets it replaced with the target schema name before
`load_schema_from_src`.

### `MigrationOptions` (all `false` by default)

| option | when `true` |
|---|---|
| `with_size_cut` | apply a narrowing or incompatible column type change (`varchar(100)` → `varchar(50)`, `int` → `varchar(5)`, which uses `USING col::type`) |
| `with_index_drop` | an index whose definition changed is dropped and recreated; a changed **primary key** is dropped and re-added (it is backed by an index) |
| `with_trigger_drop` | a trigger whose definition changed is dropped and recreated |
| `exclude_triggers` | triggers are neither created nor compared, e.g. when the trigger function is installed later by an extension |
| `with_revoke` | a privilege that is no longer granted in the YAML is revoked |
| `without_failfast` | a refused change is **skipped** and its SQL printed to stderr (or logged), instead of failing |
| `with_ddl_retry` | each DDL statement runs in a `DO` block that retries up to 100 times on `lock_not_available`, with `lock_timeout` 1000 ms; a serial column added to an existing table is then expanded into an explicit sequence, because `serial` is not valid inside `EXECUTE` |

**Failfast is the default.** A refused change returns `Err`. With
`migrate1` / `migrate_opt` the whole migration then rolls back, because it is
one transaction, and nothing is applied. The index, trigger and grant errors
say "but without_failfast is enabled" when they mean the opposite (failfast is
on); the SQL they print is correct.

## What it generates

| situation | DDL |
|---|---|
| schema missing (not `public`) | `CREATE SCHEMA IF NOT EXISTS s [AUTHORIZATION <table owner>]` |
| table missing | `CREATE TABLE` with every column, the PK (a composite PK as `PRIMARY KEY (a, b)`), the table `constraint`, `PARTITION BY` and the `sql` suffix; then owner, triggers, table and column comments |
| column missing | `ALTER TABLE ... ADD COLUMN name type [primary key] [not null] [default x] [sql]` |
| type widened or compatible (`varchar(50)`→`(100)`, `int4`→`int8`, `float4`→`float8`, `varchar`→`text`, a number/date/uuid → a `varchar` big enough for it) | `ALTER COLUMN ... TYPE`, always applied |
| type narrowed or incompatible | refused unless `with_size_cut` |
| database `NOT NULL`, YAML nullable | `ALTER COLUMN ... DROP NOT NULL` (never for a PK column) |
| database nullable, YAML `nullable: false` | **nothing**: `NOT NULL` is never added to an existing column |
| `defaultValue` changed on an existing column | **nothing**: defaults are only set when the column is created |
| PK declared and different from the database's | refused unless `with_index_drop`; a table whose YAML declares no PK keeps its PK |
| index (`index: true` or an object) missing | `CREATE [UNIQUE] INDEX [CONCURRENTLY] IF NOT EXISTS`; skipped when an existing index already covers the same columns |
| index changed | refused unless `with_index_drop` |
| trigger missing / changed | `CREATE TRIGGER` / refused unless `with_trigger_drop` |
| grant added / removed | `GRANT` / refused unless `with_revoke` |
| table `owner` differs | `ALTER TABLE ... OWNER TO` |
| `foreignKey` | `ALTER TABLE ... ADD CONSTRAINT fk_<schema>_<table>_<reftable>_<col> FOREIGN KEY ... REFERENCES` once, after all tables of the run |
| `data` rows | `INSERT ... ON CONFLICT (<pk>) DO NOTHING`, see the traps below |

## YAML in short

The full format is `src/schema.yaml` (the validator's own schema), with
worked examples in `tests/example.yaml` and `tests/example_template.yaml`, and
notes in `yaml.md`.

```yaml
database:
  - schema:
    schemaName: app            # default: public
    tables:
      - table:
          tableName: job
          description: one row per job
          columns:
            - column:
                name: id
                type: serial
                constraint: { primaryKey: true, nullable: false }
            - column:
                name: name
                type: varchar(64)
                constraint: { nullable: false }
                index: { name: job_name_uq, unique: true }   # uniqueness is a unique INDEX
            - column:
                name: tenant
                type: int
                index: { name: job_tenant_state }             # same name on two columns
            - column:                                         #   = one composite index
                name: state
                type: varchar(16)
                defaultValue: "'new'"
                index: { name: job_tenant_state }
          triggers:
            - trigger:
                name: job_event
                event: after insert or update or delete
                when: for each row
                proc: app.job_event()
```

- **Uniqueness** is expressed as a unique index, not a column constraint.
  Postgres enforces both the same way.
- **Composite index:** give several columns the same index `name`. `name: "+"`
  or no name generates `idx_<table>_<cols>`.
- **Templates:** a table with `template: true` is never created. `template:
  [other_table, schema.table]` copies their columns, triggers, grants, owner,
  description, `sql` and `constraint` first; the table's own definitions then
  override by name.
- **Partitioning:** `partition_by: RANGE | LIST | HASH` on one column (at most
  one per table) adds `PARTITION BY` to the `CREATE TABLE`. The `pg_partman`
  block passes validation but nothing acts on it yet.

## Traps (each verified in the code)

1. **`--` outside a quoted text cuts a value.** Every SQL field passes through
   `utils::as_esc`: `type`, `defaultValue`, `sql`, `constraint`, trigger
   fields, index fields. What follows a `--` is taken as the author's SQL
   comment and dropped. Since 2.3.1 a `--` inside `'...'`, `"..."` or
   `$tag$...$tag$` is kept (a default such as `'a--b'` is whole), and a
   `description`, of a table or a column, is never cut: it is text, written
   into the COMMENT as one literal.
2. **Columns are nullable unless declared otherwise.** `nullable` defaults to
   `true`. And because `NOT NULL` is never added later, a column first
   deployed as nullable stays nullable.
3. **A `NOT NULL` column added to a table with rows needs a `defaultValue`**,
   or Postgres rejects the `ADD COLUMN` and the migration rolls back.
4. **Column comments are written only when the table is created.** A column
   added later gets no `COMMENT ON COLUMN`; the table comment is written
   whenever the table changed in the run.
5. **`concurrently: true` fails inside a transaction**, and `migrate1` /
   `migrate_opt` / `migrate` always run in one (`CREATE INDEX CONCURRENTLY
   cannot run inside a transaction block`). Use it only for SQL taken from
   `dry_run` and run on its own.
6. **A foreign key is added only when the referenced table is in the same
   YAML run** (its PK is read from the YAML, not from the database). Otherwise
   the FK is skipped without an error.
7. **`data` rows are inserted only in a run where that table had a DDL
   change**, so a row added to an unchanged table is not inserted. A value is
   written as one literal, `'<value>'`: since 2.3.1 a `'` in it is doubled, so
   `O'Brien` is that name (before, it broke the statement). Write a value as
   the text it is - a `''` in the YAML is two quotes in the row. There is no
   way to write NULL.
8. **Parsed but not acted on:** `data_file`, the table `transaction` key,
   `pg_partman`, and the schema-level `roles`, `functions`, `procedures`,
   `views`, `sequences`.
   A schema-level `owner` is also not used (`Schema::new` reads the owner from
   `schemaName`); set `owner` on each table instead.
9. **Identifiers are sanitised, SQL fragments are not.** Column, trigger and
   grant names go through `safe_sql_name`; `type`, `defaultValue`, `sql`,
   `constraint`, `proc` are inserted verbatim. A schema file is code: review
   it like code, and never build one from user input.
10. **Changing a `sql` suffix does not change the database.** It is used only
    when its object is created.

## Working on the crate

| file | responsibility |
|---|---|
| `src/lib.rs` | public API, `MigrationOptions`, YAML loading and validation |
| `src/schema.yaml` | the validator schema: the YAML format itself |
| `src/schema.rs` | one schema: tables, template resolution, deploy order (tables, then FKs) |
| `src/table.rs` | the diff for one table: create, add column, type change rules (`analyze_type_change`), nullability, owner, triggers, PK, data rows, the retry `DO` wrapper |
| `src/column.rs` | column, constraint, FK, index and trigger definitions parsed from YAML |
| `src/index.rs` | desired vs existing indexes, `CREATE INDEX` |
| `src/grant.rs` | desired vs existing privileges, `GRANT` / `REVOKE` |
| `src/loader.rs` | `information_schema` / `pg_catalog` introspection into `PgTable` |
| `src/utils.rs` | `OrderedHashMap`, YAML accessors, `as_esc`, `safe_sql_name` |
| `src/mapping.rs`, `src/mappings/*.yaml` | the source field, the declarative mappings, the matrix |
| `src/sqlite.rs` | SQLite's catalog read and deploy (feature `sqlite`) |
| `src/mysql.rs`, `src/mysql_test.rs` | MySQL's catalog read and deploy (feature `mysql`) |

```sh
cargo build                # 0 warnings expected
cargo build --release
cargo test --lib                                     # PostgreSQL's default: no database needed
cargo test --lib --features sqlite                   # SQLite too, on in-memory databases
cargo test --lib --no-default-features --features sqlite
cargo check --no-default-features                    # the model alone
```

`cargo test --features sqlite --test sqlite_samples`: every sample of `tests/*.yaml` (PostgreSQL's)
mapped to SQLite, deployed to the built-in one, read back, applied again with nothing to do, written
to; and the same samples left as they are for PostgreSQL. A new sample there is taken up by itself.

Every feature combination is to build and pass: a change gated wrong shows in one of them
only. The tests under `tests/` that need PostgreSQL carry `required-features = ["postgres"]`.

- **The unit tests need no database, and nothing here tests against one.** To
  see what a change generates, run `migrate` with a `dry_run` closure against a
  scratch database and read the SQL. Get the database owner's approval before
  creating or dropping one.
- **Keep the model:** never generate `DROP` for something absent from the
  YAML, keep destructive changes behind an option, and keep failfast as the
  default.
- **When the format changes,** update `src/schema.yaml` (the validator) and
  the examples in `tests/` together, or valid files start failing
  validation.
- **Versions:** bump `version` in `Cargo.toml` for every change released.
  Downstream crates may test an unreleased fix with `[patch.crates-io]`
  pointing at a local copy, so say in the change which version carries it.
  Publishing to crates.io is the maintainer's step.

## State on 2026-11-19

- 2.4.0 published (sqlite through turso, mysql, the mappings). 2.4.1, uncommitted: a type rule's
  `indexed: true` (an indexed text to MySQL's varchar(255), as MySQL indexes no longtext), a key's
  nullability no change in SQLite - both found by sge-sqlite and sge-mysql's round trip.

- Version 2.4.0, uncommitted, not published (crates.io has 2.3.1): the database features,
  `source` and the mappings with their matrix, SQLite. 2.4, not 3: `postgres` is a default,
  so a user of the defaults gets what 2.x gave (owner, 2026-11-19).
- `cargo test --lib`: 183 pass by default, 200 with `sqlite`, 198 with `sqlite` alone; SQLite through
  turso (rusqlite before, 2026-11-19).
- Mappings each way between PostgreSQL, MySQL and SQLite; a literal default goes through the rules
  (a boolean's 0 and 1 to false and true).
- Fixed in it: `bpchar(n)` read with its length; a `char` with no length compared as one.
- Publish order: schema_guard_tokio 2.4.0, then sge 2.4.0 (it depends on `^2.4.0`).
