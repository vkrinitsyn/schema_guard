use std::collections::BTreeMap;

use serde::{Serialize, Serializer};
use serde::ser::SerializeSeq;
use yaml_rust2::Yaml;

#[inline]
pub fn as_str_esc(input: &Yaml, field: &str) -> String {
    as_esc(as_str(input, field, "").as_str())
}

/// A value of the YAML as SQL text: what follows a `--` is the author's SQL comment and is cut off.
/// A `--` inside a quoted text - `'...'`, `"..."` or `$tag$...$tag$` - is part of that text and is
/// kept: `'a--b'::text` as a default stayed `'a` before 2.3.1, a statement that does not parse.
#[inline]
pub fn as_esc(val: &str) -> String {
    match comment_start(val) {
        None => val.to_string(),
        Some(i) => val[0..i].trim().into(),
    }
}

/// Where the SQL comment of `val` starts: its first `--` outside every quoted text
fn comment_start(val: &str) -> Option<usize> {
    let bytes = val.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            // '...' and "...": to the closing quote; a doubled quote inside closes and opens again
            q @ (b'\'' | b'"') => {
                i += 1;
                while i < bytes.len() && bytes[i] != q {
                    i += 1;
                }
            }
            // $tag$...$tag$ (the tag may be empty): to the same tag again
            b'$' => {
                if let Some(len) = dollar_tag(&bytes[i..]) {
                    let tag = &val[i..i + len];
                    // never closed: all of the rest is its text, and no comment follows
                    let end = val[i + len..].find(tag)?;
                    i += len + end + len - 1;
                }
            }
            b'-' if bytes.get(i + 1) == Some(&b'-') => return Some(i),
            _ => {}
        }
        i += 1;
    }
    None
}

/// The length of a dollar-quote tag at the start of `bytes` (`$$`, `$body$`), if there is one
fn dollar_tag(bytes: &[u8]) -> Option<usize> {
    let close = bytes.iter().skip(1).position(|b| *b == b'$')? + 1;
    let name = &bytes[1..close];
    let ok = name.iter().all(|b| b.is_ascii_alphanumeric() || *b == b'_') && !name.first().is_some_and(|b| b.is_ascii_digit());
    ok.then_some(close + 1)
}

#[inline]
pub fn as_str(input: &Yaml, field: &str, def: &str) -> String {
    if input.is_null() {
        def.into()
    } else {
        match &input[field] {
            Yaml::Real(v) => v.to_string(),
            Yaml::Integer(v) => v.to_string(),
            Yaml::String(v) => v.to_string(),
            Yaml::Boolean(v) => v.to_string(),
            _ => def.into(),
        }
    }
}

#[inline]
pub fn as_vec(input: &Yaml, field: &str) -> Vec<Vec<String>> {
    let mut data = Vec::new();
    if !input.is_null() {
        if let Yaml::Array(aa) = &input[field] {
            for a in aa {
                let mut row = Vec::new();
                if let Some(vv) = a.as_vec() {
                    for v in vv {
                        row.push(v.as_str().unwrap_or("").to_string());
                    }
                }
                data.push(row);
            }
        }
    }
    data
}

#[inline]
pub fn as_stro(input: &Yaml, field: &str) -> Option<String> {
    if input.is_null() {
        None
    } else {
        match &input[field] {
            Yaml::String(v) => Some(v.to_string()),
            _ => None,
        }
    }
}

#[inline]
pub fn as_bool(input: &Yaml, field: &str, default: bool) -> bool {
    if input.is_null() {
        default
    } else {
        match &input[field] {
            Yaml::Integer(i) => i == &1i64,
            Yaml::String(s) => str2bool(s.as_str(), default),
            Yaml::Boolean(v) => *v,
            _ => default
        }
    }
}

#[inline]
pub fn str2bool(input: &str, default: bool) -> bool {
    if input.len() == 0 {
        default
    } else {
        let input = input.to_lowercase();
        input.starts_with("+")
            || input.starts_with("yes")
            || input.starts_with("true")
            || input.starts_with("ok")
            || input.starts_with("on")
            || input.starts_with("y")
            || input.starts_with("1")
    }
}

#[inline]
pub fn safe_sql_name(input: String) -> String {
    match input
        .chars()
        .position(|c| c == ' ' || c == '.' || c == ';' || c == '\n' || c == '\t')
    {
        None => input,
        Some(i) => input[0..i].into(),
    }
}

pub trait Named {
    fn get_name(&self) -> String;
}

/// String key for BTreeMap, sorted by adding order
#[derive(Debug, Clone)]
pub struct OrderedHashMap<T: Named + Serialize> {
    pub(crate) map: BTreeMap<String, usize>,
    pub list: Vec<T>,
}

impl<T: Named + Serialize> OrderedHashMap<T> {
    #[inline]
    pub fn new() -> Self {
        OrderedHashMap {
            map: Default::default(),
            list: vec![],
        }
    }
    #[inline]
    pub fn append(&mut self, value: T) -> Result<(), String> {
        let key_name = value.get_name();
        if key_name.len() == 0 {
            Err(tr!("utils.empty_name"))
        } else if self.map.contains_key(&key_name) {
            Err(tr!("utils.duplicate", name = key_name))
        } else {
            self.map.insert(key_name, self.list.len());
            self.list.push(value);
            Ok(())
        }
    }
    #[inline]
    pub fn get(&self, key: &String) -> Option<&T> {
        match self.map.get(key) {
            None => None,
            Some(id) => Some(&self.list[*id]),
        }
    }

    #[inline]
    pub fn get_mut(&mut self, key: &String) -> Option<&mut T> {
        match self.map.get_mut(key) {
            None => None,
            Some(id) => Some(&mut self.list[*id]),
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.list.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.map.len() == 0
    }
}

impl<T: Named + Serialize> Serialize for OrderedHashMap<T> {
    fn serialize<S>(&self, serializer: S) -> Result<<S as Serializer>::Ok, <S as Serializer>::Error> where
        S: Serializer, T: Named + Serialize {
        let mut seq = serializer.serialize_seq(Some(self.len()))?;
        for element in &self.list {
            seq.serialize_element(element)?;
        }
        seq.end()
    }
}


#[cfg(test)]
mod tests {
    use crate::column::Trig;

    use super::*;

    #[test]
    fn defb_test() {
        defb_test_t(true);
        defb_test_t(false);
        assert!(str2bool("", true));
        defb_test_f(true);
        defb_test_f(false);
        assert!(!str2bool("", false));
    }

    fn defb_test_t(def: bool) {
        for v in vec!["true", "yes", "+", "Y", "OK", "ok"] {
            assert!(str2bool(v, def));
        }
    }

    fn defb_test_f(def: bool) {
        for v in vec!["false", "no"] {
            //
            assert!(!str2bool(v, def));
        }
    }


    /// [2.3.1] a `--` starts the author's comment only outside a quoted text
    #[test]
    fn a_double_dash_inside_a_quoted_text_is_kept() {
        assert_eq!(as_esc("now() -- when it was made"), "now()");
        assert_eq!(as_esc("-- all of it"), "");
        assert_eq!(as_esc("plain"), "plain");
        assert_eq!(as_esc(""), "");
        assert_eq!(as_esc("a - b"), "a - b", "one dash is no comment");
        assert_eq!(as_esc("'a--b'::text"), "'a--b'::text");
        assert_eq!(as_esc("'a--b'::text -- the default"), "'a--b'::text");
        assert_eq!(as_esc("'it''s -- fine' -- cut here"), "'it''s -- fine'", "a doubled quote stays inside");
        assert_eq!(as_esc("\"odd--name\" -- c"), "\"odd--name\"");
        assert_eq!(as_esc("$$ a -- b $$ -- c"), "$$ a -- b $$");
        assert_eq!(as_esc("$fn$ x -- y $fn$::text -- c"), "$fn$ x -- y $fn$::text");
        assert_eq!(as_esc("price > $1 -- a parameter, no quote"), "price > $1");
        assert_eq!(as_esc("'never closed -- so it is its text"), "'never closed -- so it is its text");
        assert_eq!(as_esc("$$never closed -- text"), "$$never closed -- text");
        assert_eq!(as_esc("\u{442}\u{435}\u{441}\u{442} '--' -- \u{434}\u{430}"), "\u{442}\u{435}\u{441}\u{442} '--'", "not cut inside a character");
    }

    #[test]
    fn safe_test() {
        assert_eq!("a".to_string(), safe_sql_name("a;".to_string()));
        assert_eq!("a".to_string(), safe_sql_name("a".to_string()));
        assert_eq!("a".to_string(), safe_sql_name("a ".to_string()));
        assert_eq!("".to_string(), safe_sql_name("".to_string()));
        assert_eq!("a".to_string(), safe_sql_name("a. ".to_string()));
        assert_eq!("a".to_string(), safe_sql_name("a\n ".to_string()));
        assert_eq!("a".to_string(), safe_sql_name("a\t ".to_string()));
    }

    #[test]
    fn serialize_test() {
        let mut l: OrderedHashMap<Trig> = OrderedHashMap::new();
        let _ = l.append(Trig {
            name: "a".to_string(),
            event: "b".to_string(),
            when: "c".to_string(),
            proc: "d".to_string(),
        });
        assert_eq!("[{'name':'a','event':'b','when':'c','proc':'d'}]".replace("'", "\""),
                   serde_json::to_string(&l).unwrap());
    }
}

