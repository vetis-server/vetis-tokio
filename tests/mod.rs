use std::error::Error;

mod common;
mod vetis_macros;

pub type TestResult<T> = std::result::Result<T, Box<dyn Error>>;
