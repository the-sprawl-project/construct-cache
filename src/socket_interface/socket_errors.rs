use std::fmt::{Debug, Display, Formatter, Result};

#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub enum ErrorKind {
    ErrorNone,
    ParseError,
    ConnectError,
}

pub struct SocketError {
    pub kind_: ErrorKind,
    pub context_: String,
}

impl Display for SocketError {
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

impl Debug for SocketError {
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
        ErrorKind::ParseError => "Cannot parse payload",
        ErrorKind::ConnectError => "Cannot connect to server",
    };
    String::from(ret)
}
