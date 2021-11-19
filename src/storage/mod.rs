use std::io::Result;

pub mod memory;
pub mod lsm;

pub use memory::MemStorage;
pub use lsm::LSMStorage;


pub trait Storage {
    fn set(&mut self, key: &str, value: String) -> Result<()>;
    fn delete(&mut self, key: &str) -> Result<()>;
    fn get(&self, key: &str) -> Option<&String>;
}

