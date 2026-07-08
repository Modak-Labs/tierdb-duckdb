use std::ffi::{c_char, CString};
use std::ptr;

use crate::catalog::Conn;

#[repr(C)]
pub struct TierDBConnResult {
    pub err: *mut c_char,
    pub value: *mut Conn,
}

#[repr(C)]
pub struct TierDBStringResult {
    pub err: *mut c_char,
    pub value: *mut c_char,
}

#[repr(C)]
pub struct TierDBStatus {
    pub err: *mut c_char,
}

fn into_c_string(s: String) -> *mut c_char {
    CString::new(s)
        .unwrap_or_else(|_| CString::new("tierdb: message contained a NUL byte").unwrap())
        .into_raw()
}

impl TierDBConnResult {
    pub fn ok(conn: Conn) -> Self {
        Self {
            err: ptr::null_mut(),
            value: Box::into_raw(Box::new(conn)),
        }
    }

    pub fn err(message: impl Into<String>) -> Self {
        Self {
            err: into_c_string(message.into()),
            value: ptr::null_mut(),
        }
    }
}

impl TierDBStringResult {
    pub fn ok(value: String) -> Self {
        Self {
            err: ptr::null_mut(),
            value: into_c_string(value),
        }
    }

    pub fn err(message: impl Into<String>) -> Self {
        Self {
            err: into_c_string(message.into()),
            value: ptr::null_mut(),
        }
    }
}

impl TierDBStatus {
    pub fn ok() -> Self {
        Self {
            err: ptr::null_mut(),
        }
    }

    pub fn err(message: impl Into<String>) -> Self {
        Self {
            err: into_c_string(message.into()),
        }
    }
}
