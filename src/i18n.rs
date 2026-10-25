//! [i18n] The language of schema_guard's messages - errors, validation errors, log lines.
//!
//! The first of: [set_lang]; the `lang` value of the logger given to
//! [crate::set_logger] - `o!("lang" => "ru")`, so a caller already handing over its
//! logger needs nothing more; the environment's LANG; English. A locale such as
//! `ru_RU.UTF-8` is taken by its language, and a language without its messages is
//! English.
//!
//! The messages are in locales/*.yml, each with every language: en, ru, and - not
//! reviewed - fr, de, zh (Simplified Chinese), ja. An error's starts with its code,
//! `SG-Ennn`, the same in every language, so it can be looked up whatever the language.

use std::sync::RwLock;

use lazy_static::lazy_static;

lazy_static! {
    /// set_lang's
    static ref LANG: RwLock<Option<String>> = RwLock::new(None);
    /// the logger's, read by set_logger
    static ref LOGGER_LANG: RwLock<Option<String>> = RwLock::new(None);
}

/// The language of the messages from here on: `en`, `ru`, `fr`, `de`, `zh`, `ja`, or a
/// locale of one (`ru_RU.UTF-8`). Wins over the logger's and LANG.
pub fn set_lang(lang: &str) {
    if let Ok(mut l) = LANG.write() {
        *l = Some(language(lang));
    }
}

/// The logger's language, or none - when set_logger is given one without it
#[allow(dead_code)]
pub(crate) fn set_logger_lang(lang: Option<String>) {
    if let Ok(mut l) = LOGGER_LANG.write() {
        *l = lang.map(|l| language(&l));
    }
}

/// The language the messages are in now
pub(crate) fn lang() -> String {
    choose(
        LANG.read().ok().and_then(|l| l.clone()),
        LOGGER_LANG.read().ok().and_then(|l| l.clone()),
        std::env::var("LANG").ok(),
    )
}

fn choose(set: Option<String>, logger: Option<String>, env: Option<String>) -> String {
    set.or(logger)
        .or_else(|| env.map(|e| language(&e)))
        .filter(|l| !l.is_empty())
        .unwrap_or_else(|| "en".to_string())
}

/// a locale's language: its first two letters, lower case
fn language(locale: &str) -> String {
    locale.trim().chars().take(2).collect::<String>().to_lowercase()
}

/// The logger's `lang` value, as o!("lang" => ...) set it
#[cfg(feature = "slog")]
pub(crate) fn lang_of(logger: &slog::Logger) -> Option<String> {
    struct Lang(Option<String>);
    impl slog::Serializer for Lang {
        fn emit_arguments(&mut self, key: slog::Key, val: &std::fmt::Arguments) -> slog::Result {
            if key == "lang" {
                self.0 = Some(val.to_string());
            }
            Ok(())
        }
    }
    let mut found = Lang(None);
    let rs = record_static!(slog::Level::Info, "");
    let args = format_args!("");
    let _ = slog::KV::serialize(logger.list(), &slog::Record::new(&rs, &args, b!()), &mut found);
    found.0
}

/// [i18n] The message `key` in the language of [lang], each %{name} placeholder filled
macro_rules! tr {
    ($key:expr $(, $name:ident = $value:expr)* $(,)?) => {
        rust_i18n::t!($key, locale = &$crate::i18n::lang() $(, $name = $value)*).into_owned()
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    /// [i18n] set_lang over the logger's, the logger's over LANG, LANG over English
    #[test]
    fn the_language_is_the_first_one_given() {
        let s = |v: &str| Some(v.to_string());
        assert_eq!(choose(s("de"), s("fr"), s("ru_RU.UTF-8")), "de");
        assert_eq!(choose(None, s("fr"), s("ru_RU.UTF-8")), "fr");
        assert_eq!(choose(None, None, s("ru_RU.UTF-8")), "ru");
        assert_eq!(choose(None, None, None), "en");
        assert_eq!(choose(None, None, s("")), "en");
        assert_eq!(language("JA_jp"), "ja");
    }

    /// [i18n] a language without messages, or a key without one in it, is English
    #[test]
    fn a_message_falls_back_to_english() {
        let en = rust_i18n::t!("lib.empty_file", locale = "en").into_owned();
        assert!(en.starts_with("SG-E"), "{}", en);
        assert_eq!(rust_i18n::t!("lib.empty_file", locale = "xx").into_owned(), en);
        assert_ne!(rust_i18n::t!("lib.empty_file", locale = "ru").into_owned(), en);
    }

    /// locales/*.yml, as key => language => message
    fn messages() -> std::collections::BTreeMap<String, std::collections::BTreeMap<String, String>> {
        let dir = format!("{}/locales", env!("CARGO_MANIFEST_DIR"));
        let mut all = std::collections::BTreeMap::new();
        for f in std::fs::read_dir(&dir).unwrap() {
            let f = f.unwrap().path();
            let y = &yaml_rust2::YamlLoader::load_from_str(&std::fs::read_to_string(&f).unwrap()).unwrap()[0];
            for (k, v) in y.as_hash().unwrap() {
                let k = k.as_str().unwrap();
                if k == "_version" {
                    continue;
                }
                let langs = v.as_hash().unwrap_or_else(|| panic!("{}: {}", f.display(), k)).iter()
                    .map(|(l, m)| (l.as_str().unwrap().to_string(), m.as_str().unwrap().to_string())).collect();
                assert!(all.insert(k.to_string(), langs).is_none(), "{} twice", k);
            }
        }
        all
    }

    /// [i18n] every message in every language, with the same placeholders; an error's
    /// code the same in every language, and no two errors with one code
    #[test]
    fn every_message_is_in_every_language() {
        let placeholders = |m: &str| {
            let mut p: Vec<String> = m.split("%{").skip(1).map(|r| r.split('}').next().unwrap().to_string()).collect();
            p.sort();
            p
        };
        let mut codes = std::collections::BTreeMap::new();
        for (key, langs) in messages() {
            let en = &langs["en"];
            assert_eq!(langs.keys().map(String::as_str).collect::<Vec<_>>(), ["de", "en", "fr", "ja", "ru", "zh"], "{}", key);
            let code = en.split(' ').next().filter(|c| c.starts_with("SG-E"));
            for (l, m) in &langs {
                assert_eq!(placeholders(m), placeholders(en), "{} {}", key, l);
                assert_eq!(m.split(' ').next().filter(|c| c.starts_with("SG-E")), code, "{} {}: the code", key, l);
            }
            if let Some(c) = code {
                assert!(codes.insert(c.to_string(), key.clone()).is_none(), "{} for {} and {}", c, codes[c], key);
            }
        }
        assert!(codes.len() > 30, "{}", codes.len());
    }

    /// [i18n] every key the code asks for is in locales/, and every message there asked for
    #[test]
    fn every_key_used_has_its_message_and_every_message_a_use() {
        let all = messages();
        let calls = regex::Regex::new(r#"(?:\btr!\(|\berror:|=>)\s*"([a-z_]+\.[a-z_.]+)""#).unwrap();
        let mut used = std::collections::BTreeSet::new();
        let mut files = vec![std::path::PathBuf::from(format!("{}/src", env!("CARGO_MANIFEST_DIR")))];
        while let Some(f) = files.pop() {
            if f.is_dir() {
                files.extend(std::fs::read_dir(&f).unwrap().map(|e| e.unwrap().path()));
                continue;
            }
            let src = std::fs::read_to_string(&f).unwrap();
            // tr!("key"), the validator's error: "key", and DeployStep's arms, => "key"
            for c in calls.captures_iter(&src) {
                assert!(all.contains_key(&c[1]), "{}: no message {}", f.display(), &c[1]);
                used.insert(c[1].to_string());
            }
        }
        let unused: Vec<&String> = all.keys().filter(|k| !used.contains(*k)).collect();
        assert!(unused.is_empty(), "messages nothing asks for: {:?}", unused);
    }

    #[cfg(feature = "slog")]
    #[test]
    fn the_logger_carries_its_language() {
        let root = slog::Logger::root(slog::Discard, o!("app" => "sge"));
        assert_eq!(lang_of(&root), None);
        assert_eq!(lang_of(&root.new(o!("lang" => "ja"))), Some("ja".to_string()));
    }
}
