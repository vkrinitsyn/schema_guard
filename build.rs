// [i18n] rust-i18n reads locales/ as the crate compiles: a message edited there rebuilds it
fn main() {
    println!("cargo:rerun-if-changed=locales");
}
