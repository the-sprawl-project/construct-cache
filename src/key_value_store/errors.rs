use std::fmt::{Debug, Display, Formatter, Result};

#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub enum ErrorKind {
    ErrorNone,
    FileOpenError,
    FileWriteError,
    FileReadError,
    DataDecodeError,
}

pub struct RWError {
    pub kind_: ErrorKind,
    pub context_: String,
}

impl Display for RWError {
    fn fmt(&self, f: &mut Formatter) -> Result {
        match write!(
            f,
            "{}, context: {}",
            error_kind_to_str(self.kind_),
            self.context_
        ) {
            Ok(_) => {}
            Err(e) => {
                panic!("{}", e)
            }
        };

        Ok(())
    }
}

impl Debug for RWError {
    fn fmt(&self, f: &mut Formatter) -> Result {
        match write!(
            f,
            "Error! {{ kind: {}, context: {} }}",
            error_kind_to_str(self.kind_),
            self.context_
        ) {
            Ok(_) => {}
            Err(e) => {
                panic!("{}", e)
            }
        };

        Ok(())
    }
}

fn error_kind_to_str(ek: ErrorKind) -> String {
    let ret = match ek {
        ErrorKind::ErrorNone => "",
        ErrorKind::FileOpenError => "Cannot open file",
        ErrorKind::FileReadError => "Cannot read file",
        ErrorKind::FileWriteError => "Cannot write to file",
        ErrorKind::DataDecodeError => "Data decode error",
    };
    String::from(ret)
}
