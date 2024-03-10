use std::collections::BTreeMap;

use serde::{Serialize, Serializer};
use serde::ser::SerializeSeq;
use yaml_rust::Yaml;

#[inline]
pub fn as_str_esc(input: &Yaml, field: &str) -> String {
    as_esc(as_str(input, field, "").as_str())
}

#[inline]
pub fn as_esc(val: &str) -> String {
    match val.find("--") {
        None => {
            if val.len() > 0 {
                format!("{}", val)
            } else {
                "".into()
            }
        }
        Some(i) => val[0..i].trim().into(),
    }
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
            Err("Empty".into())
        } else if self.map.contains_key(&key_name) {
            Err(format!("Duplicate {}", key_name))
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

