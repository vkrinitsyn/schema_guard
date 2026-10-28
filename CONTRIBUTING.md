# Contributing to Schema Guard (sg)

Thank you for helping. Schema Guard (the `schema_guard_tokio` crate) is MIT licensed;
a contribution is accepted under the same license (see [LICENSE](LICENSE)).

## Issues

Open an issue on GitHub for a bug or a feature request. For a bug, give the crate's
version (`schema_guard_tokio` in your `Cargo.lock`), the PostgreSQL version, the YAML
schema - the smallest one that shows it - the call you made, and the error it
returned.

A security problem is not reported in a public issue: email it to
**support@dbinvent.com**, with "schema guard security" in the subject.

## Branches

- `main` - the tokio edition, the `schema_guard_tokio` crate.
- `std` - the standard, non-tokio edition. A change that belongs to both editions
  goes to each branch.

## Pull requests

1. Fork the repository and branch from `main` (or `std`, for the standard edition).
2. Keep a change to one subject; describe what it changes and why.
3. Build and test it (below); add tests for what you change.
4. Messages a user reads go through the locale files (`locales/`): add a new key to
   every one of them.
5. Open the pull request against the branch you started from.

## Build and test

```
cargo build
cargo test
```

The unit tests need no database. The PostgreSQL tests (`tests/pg_errors.rs`) start a
throwaway cluster of their own with the newest PostgreSQL installed
(`/usr/lib/postgresql/<ver>/bin`, or `PG_BIN`), reached on its unix socket only;
without one installed they say so and pass. The examples run with
`cargo run --example <name>` (see `examples/`).

Before a pull request, run the checks as well:

```
cargo audit      # known vulnerabilities in the dependencies Cargo.lock pins
cargo geiger     # unsafe code in schema_guard_tokio and its dependencies
```

A library keeps no `Cargo.lock` in git: `cargo generate-lockfile` makes one for the
audit. With cargo-make, `cargo make audit` and `cargo make geiger` run the same.

## Code of conduct

Taking part in this project means following its
[Code of Conduct](CODE_OF_CONDUCT.md).
