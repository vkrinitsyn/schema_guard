use thiserror::Error;

use crate::yaml_validator::breadcrumb::{Breadcrumb, BreadcrumbSegment, BreadcrumbSegmentVec};

use super::GenericError;

/// [i18n] Each kind says what is wrong in the messages' language - see GenericError.
/// ValidationError's `error` is the key of its message, in locales/validation.yml
#[derive(Error, Debug, PartialEq, Eq)]
pub enum ValidationErrorKind<'a> {
    WrongType {
        expected: &'static str,
        actual: &'a str,
    },
    ValidationError { error: &'a str },
    FieldMissing { field: &'a str },
    ExtraField { field: &'a str },
    UnknownType { unknown_type: &'a str },
    Multiple { errors: Vec<ValidationError<'a>> },
    UnknownSchema { uri: &'a str },
}

impl<'a> std::fmt::Display for ValidationErrorKind<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&match self {
            ValidationErrorKind::WrongType { expected, actual } => tr!("validation.wrong_type", expected = expected, actual = actual),
            ValidationErrorKind::ValidationError { error } => tr!("validation.special", error = tr!(*error)),
            ValidationErrorKind::FieldMissing { field } => tr!("validation.field_missing", field = field),
            ValidationErrorKind::ExtraField { field } => tr!("validation.extra_field", field = field),
            ValidationErrorKind::UnknownType { unknown_type } => tr!("validation.unknown_type", unknown = unknown_type),
            ValidationErrorKind::Multiple { errors } => tr!("validation.multiple", errors = format!("{:?}", errors)),
            ValidationErrorKind::UnknownSchema { uri } => tr!("validation.unknown_schema", uri = uri),
        })
    }
}

impl<'a> ValidationErrorKind<'a> {
    pub fn with_path(self, path: BreadcrumbSegmentVec<'a>) -> ValidationError<'a> {
        ValidationError {
            kind: self,
            state: Breadcrumb::new(path),
        }
    }

    pub fn with_path_name(self, path: &'a str) -> ValidationError<'a> {
        let mut err: ValidationError = self.into();
        err.state.push(BreadcrumbSegment::Name(path));
        err
    }

    pub fn with_path_index(self, index: usize) -> ValidationError<'a> {
        let mut err: ValidationError = self.into();
        err.state.push(BreadcrumbSegment::Index(index));
        err
    }
}

impl<'a> From<ValidationErrorKind<'a>> for ValidationError<'a> {
    fn from(kind: ValidationErrorKind<'a>) -> ValidationError<'a> {
        ValidationError {
            kind,
            state: Breadcrumb::default(),
        }
    }
}

impl<'a> From<Vec<ValidationError<'a>>> for ValidationError<'a> {
    fn from(errors: Vec<ValidationError<'a>>) -> Self {
        ValidationErrorKind::Multiple { errors }.into()
    }
}

impl<'a> From<GenericError<'a>> for ValidationErrorKind<'a> {
    fn from(e: GenericError<'a>) -> Self {
        match e {
            GenericError::WrongType { expected, actual } => {
                ValidationErrorKind::WrongType { expected, actual }
            }
            GenericError::FieldMissing { field } => ValidationErrorKind::FieldMissing { field },
            GenericError::ExtraField { field } => ValidationErrorKind::ExtraField { field },
            GenericError::Multiple { errors } => ValidationErrorKind::Multiple {
                errors: errors
                    .into_iter()
                    .map(ValidationErrorKind::from)
                    .map(ValidationError::from)
                    .collect(),
            },
        }
    }
}

impl<'a> From<GenericError<'a>> for ValidationError<'a> {
    fn from(e: GenericError<'a>) -> Self {
        ValidationErrorKind::from(e).into()
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct ValidationError<'a> {
    pub kind: ValidationErrorKind<'a>,
    pub state: Breadcrumb<'a>,
}

impl<'a> ValidationError<'a> {
    fn flatten(&self, fmt: &mut std::fmt::Formatter<'_>, root: String) -> std::fmt::Result {
        match &self.kind {
            ValidationErrorKind::Multiple { errors } => {
                for err in errors {
                    err.flatten(fmt, format!("{}{}", root, self.state))?;
                }
            }
            err => writeln!(fmt, "{}{}: {}", root, self.state, err)?,
        }

        Ok(())
    }

    pub fn add_path_name(path: &'a str) -> impl Fn(ValidationError<'a>) -> ValidationError<'a> {
        move |mut err: ValidationError<'a>| -> ValidationError<'a> {
            err.state.push(BreadcrumbSegment::Name(path));
            err
        }
    }

    pub fn add_path_index(index: usize) -> impl Fn(ValidationError<'a>) -> ValidationError<'a> {
        move |mut err: ValidationError<'a>| -> ValidationError<'a> {
            err.state.push(BreadcrumbSegment::Index(index));
            err
        }
    }
}

impl<'a> std::fmt::Display for ValidationError<'a> {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.flatten(fmt, "#".to_string())
    }
}
