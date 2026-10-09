# agent.md — schema_guard_tokio

A guide for coding agents (and people) who use this crate or change it. Each
statement below was checked against the source in `src/` for version 1.10.2.

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
| `loader::load_info_schema(db_name, &mut Transaction)` | introspection: what the database has now, as `InfoSchemaType` (schema → table → `PgTable`: columns, PK, FKs, indexes, triggers, grants, owner) |
| `set_logger(slog::Logger)` | feature `slog` only: executed SQL is logged at debug |

`migrate1` / `migrate_opt` connect without TLS. For TLS, or to apply several
files atomically, open the transaction yourself and call `migrate`.

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

1. **`--` cuts a value.** Every string field passes through `utils::as_esc`,
   which truncates at the first `--`: `type`, `defaultValue`, `sql`,
   `constraint`, `description`, trigger fields, index fields. A default such
   as `'a--b'` or a comment inside `sql` is silently cut. Never put `--` in a
   value.
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
   change**, so a row added to an unchanged table is not inserted. Values are
   written as `'<value>'` with **no escaping**: a value containing `'` breaks
   the statement, and there is no way to write NULL.
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

```sh
cargo build                # 0 warnings expected
cargo build --release
cargo test                 # offline: parses tests/example.yaml, no database needed
```

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

## State on 2026-10-09

- Version 1.10.2, branch `features/tokio`. `cargo build` (debug and release):
  0 warnings. `cargo test`: 5/5 pass.
- Uncommitted in `src/table.rs`: the primary-key guard
  `!desired_pk.is_empty() && desired_pk != existing_pk`. Without it, a partial
  table definition (one new column, no PK listed) read as "remove the primary
  key". With it, a PK is changed only when the YAML declares a different one.
