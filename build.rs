
const DIESEL_TOML: &str = "diesel.toml";

fn main()  {
    #[cfg(feature = "diesel")] diesel();
}


#[allow(dead_code)]
pub(crate) fn diesel() {
    println!("cargo:info=Loading {} ", DIESEL_TOML);
    // let config: Config = toml::from_str(DIESEL_TOML).expect(format!("{} expected by feature diesel", DIESEL_TOML));
    // let r = config.print_schema.unwrap().file.unwrap();

}
/*
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Config {
    print_schema: Option<FileConfig>,
}

#[derive(Debug, Deserialize)]
struct FileConfig {
    file: Option<String>,
}
*/
