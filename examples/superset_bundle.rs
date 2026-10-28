//! A YAML schema as the ZIP Apache Superset imports (Datasets, Import):
//! `cargo run --example superset_bundle -- <schema.yaml> <database name> <sqlalchemy uri> <out.zip>`
use schema_guard_tokio::superset::{zip, SupersetDatabase};

fn main() {
    let a: Vec<String> = std::env::args().collect();
    if a.len() != 5 {
        eprintln!("usage: superset_bundle <schema.yaml> <database name> <sqlalchemy uri> <out.zip>");
        std::process::exit(2);
    }
    let fail = |e: String| -> ! {
        eprintln!("{}", e);
        std::process::exit(1)
    };
    let yaml = schema_guard_tokio::load_schema_from_file(&a[1]).unwrap_or_else(|e| fail(e));
    let db = SupersetDatabase::new(&a[2], &a[3]);
    let files = schema_guard_tokio::superset_bundle(yaml, &a[1], &db).unwrap_or_else(|e| fail(e));
    let bytes = zip(&files, "dataset_export").unwrap_or_else(|e| fail(e));
    std::fs::write(&a[4], bytes).unwrap_or_else(|e| fail(e.to_string()));
    println!("{}: {} file(s)", a[4], files.len());
}
