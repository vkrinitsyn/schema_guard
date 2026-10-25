pub(crate) mod schema;
pub(crate) mod validation;

pub use schema::{SchemaError, SchemaErrorKind};
pub use validation::{ValidationError, ValidationErrorKind};

use thiserror::Error;

/// [i18n] Each kind says what is wrong in the messages' language (crate::i18n) - Display
/// by hand, through tr!, rather than thiserror's #[error] in English
#[derive(Error, Debug, PartialEq, Eq)]
pub enum GenericError<'a> {
    WrongType {
        expected: &'static str,
        actual: &'a str,
    },
    FieldMissing { field: &'a str },
    ExtraField { field: &'a str },
    Multiple { errors: Vec<GenericError<'a>> },
}

impl<'a> std::fmt::Display for GenericError<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&match self {
            GenericError::WrongType { expected, actual } => tr!("validation.wrong_type", expected = expected, actual = actual),
            GenericError::FieldMissing { field } => tr!("validation.field_missing", field = field),
            GenericError::ExtraField { field } => tr!("validation.extra_field", field = field),
            GenericError::Multiple { errors } => tr!("validation.multiple", errors = format!("{:?}", errors)),
        })
    }
}
