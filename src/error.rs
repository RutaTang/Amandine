use std::{
    error::Error,
    fmt::{Display, Formatter, Result as FmtResult},
};

/// Error type for the DB
#[derive(Debug)]
pub struct DBError<'a>(pub &'a str);

impl<'a> Display for DBError<'a> {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        write!(f, "DBError: {}", self.0)
    }
}

impl<'a> Error for DBError<'a> {}
