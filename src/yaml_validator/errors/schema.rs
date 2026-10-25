#![macro_use]

use thiserror::Error;

use super::GenericError;
use crate::yaml_validator::breadcrumb::{Breadcrumb, BreadcrumbSegment, BreadcrumbSegmentVec};

/// [i18n] Each kind says what is wrong in the messages' language - see GenericError.
/// MalformedField's `error` is in it already, put there by tr! where it is made
#[derive(Error, Debug, PartialEq, Eq)]
pub enum SchemaErrorKind<'a> {
    WrongType {
        expected: &'static str,
        actual: &'a str,
    },
    MalformedField { error: String },
    FieldMissing { field: &'a str },
    ExtraField { field: &'a str },
    UnknownType { unknown_type: &'a str },
    Multiple { errors: Vec<SchemaError<'a>> },
}

impl<'a> std::fmt::Display for SchemaErrorKind<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&match self {
            SchemaErrorKind::WrongType { expected, actual } => tr!("validation.wrong_type", expected = expected, actual = actual),
            SchemaErrorKind::MalformedField { error } => tr!("validation.malformed", error = error),
            SchemaErrorKind::FieldMissing { field } => tr!("validation.field_missing", field = field),
            SchemaErrorKind::ExtraField { field } => tr!("validation.extra_field", field = field),
            SchemaErrorKind::UnknownType { unknown_type } => tr!("validation.unknown_type", unknown = unknown_type),
            SchemaErrorKind::Multiple { errors } => tr!("validation.multiple", errors = format!("{:?}", errors)),
        })
    }
}

/// A wrapper type around SchemaErrorKind containing path information about where the error occurred.
#[derive(Debug, PartialEq, Eq)]
pub struct SchemaError<'schema> {
    pub kind: SchemaErrorKind<'schema>,
    pub state: Breadcrumb<'schema>,
}

impl<'a> SchemaError<'a> {
    fn flatten(&self, fmt: &mut std::fmt::Formatter<'_>, root: String) -> std::fmt::Result {
        match &self.kind {
            SchemaErrorKind::Multiple { errors } => {
                for err in errors {
                    err.flatten(fmt, format!("{}{}", root, self.state))?;
                }
            }
            err => writeln!(fmt, "{}{}: {}", root, self.state, err)?,
        }

        Ok(())
    }

    pub fn add_path_name(path: &'a str) -> impl Fn(SchemaError<'a>) -> SchemaError<'a> {
        move |mut err: SchemaError<'a>| -> SchemaError<'a> {
            err.state.push(BreadcrumbSegment::Name(path));
            err
        }
    }

    pub fn add_path_index(index: usize) -> impl Fn(SchemaError<'a>) -> SchemaError<'a> {
        move |mut err: SchemaError<'a>| -> SchemaError<'a> {
            err.state.push(BreadcrumbSegment::Index(index));
            err
        }
    }
}

impl<'a> std::fmt::Display for SchemaError<'a> {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.flatten(fmt, "#".to_string())
    }
}

impl<'a> SchemaErrorKind<'a> {
    pub fn with_path(self, path: BreadcrumbSegmentVec<'a>) -> SchemaError<'a> {
        SchemaError {
            kind: self,
            state: Breadcrumb::new(path),
        }
    }

    pub fn with_path_name(self, path: &'a str) -> SchemaError<'a> {
        let mut err: SchemaError = self.into();
        err.state.push(BreadcrumbSegment::Name(path));
        err
    }

    pub fn with_path_index(self, index: usize) -> SchemaError<'a> {
        let mut err: SchemaError = self.into();
        err.state.push(BreadcrumbSegment::Index(index));
        err
    }
}

impl<'a> From<SchemaErrorKind<'a>> for SchemaError<'a> {
    fn from(kind: SchemaErrorKind<'a>) -> SchemaError<'a> {
        SchemaError {
            kind,
            state: Breadcrumb::default(),
        }
    }
}

impl<'a> From<Vec<SchemaError<'a>>> for SchemaError<'a> {
    fn from(errors: Vec<SchemaError<'a>>) -> Self {
        SchemaErrorKind::Multiple { errors }.into()
    }
}

impl<'a> From<GenericError<'a>> for SchemaErrorKind<'a> {
    fn from(e: GenericError<'a>) -> Self {
        match e {
            GenericError::WrongType { expected, actual } => {
                SchemaErrorKind::WrongType { expected, actual }
            }
            GenericError::FieldMissing { field } => SchemaErrorKind::FieldMissing { field },
            GenericError::ExtraField { field } => SchemaErrorKind::ExtraField { field },
            GenericError::Multiple { errors } => SchemaErrorKind::Multiple {
                errors: errors
                    .into_iter()
                    .map(SchemaErrorKind::from)
                    .map(SchemaError::from)
                    .collect(),
            },
        }
    }
}

impl<'a> From<GenericError<'a>> for SchemaError<'a> {
    fn from(e: GenericError<'a>) -> Self {
        SchemaErrorKind::from(e).into()
    }
}

#[cfg(test)]
mod tests {
    use crate::yaml_validator::types::*;
    use crate::yaml_validator::utils::load_simple;
    use crate::yaml_validator::{Context, Validate};
    use std::convert::TryFrom;
    #[test]
    fn test_error_path() {
        let yaml = load_simple(
            r#"
            items:
              test:
                type: integer
              something:
                type: object
                items:
                  level2:
                    type: object
                    items:
                      leaf: 
                        notype: hello
            "#,
        );

        let err = SchemaObject::try_from(&yaml).unwrap_err();

        // in whatever language the messages are in
        assert_eq!(
            format!("{}", err),
            format!("#.items.something.items.level2.items.leaf: {}\n", tr!("validation.field_missing", field = "type")),
        );
    }

    #[test]
    fn test_error_path_validation() {
        let yaml = load_simple(
            r#"
            items:
              test:
                type: integer
              something:
                type: object
                items:
                  level2:
                    type: array
                    items:
                      type: object
                      items:
                        num:
                          type: integer
            "#,
        );

        let schema = SchemaObject::try_from(&yaml).unwrap();
        let document = load_simple(
            r#"
            test: 20
            something:
              level2:
                - num: abc
                - num:
                    hash: value
                - num:
                    - array: hello
                - num: 10
                - num: jkl
            "#,
        );
        let ctx = Context::default();
        let err = schema.validate(&ctx, &document).unwrap_err();

        let wrong = |actual: &str| tr!("validation.wrong_type", expected = "integer", actual = actual);
        assert_eq!(
            format!("{}", err),
            format!("#.something.level2[0].num: {}\n#.something.level2[1].num: {}\n\
                #.something.level2[2].num: {}\n#.something.level2[4].num: {}\n",
                wrong("string"), wrong("hash"), wrong("array"), wrong("string"))
        );
    }
}
