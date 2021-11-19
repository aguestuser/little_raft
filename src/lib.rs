#[cfg(test)]
#[macro_use]
extern crate lazy_static;

pub mod error;
pub mod node;
pub mod rpc;
pub mod state;
mod test_support;

pub const NEWLINE: u8 = '\n' as u8;
