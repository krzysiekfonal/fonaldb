use std::fs::File;
use std::io::Result;

pub mod memory;
pub mod lsm;
pub mod wal;
pub mod sstable;

pub use memory::MemStorage;
pub use lsm::LSMStorage;
pub use wal::WALStorage;


pub trait Storage {
    fn set(&mut self, key: &str, value: &str) -> Result<()>;
    fn delete(&mut self, key: &str) -> Result<()>;
    fn get(&self, key: &str) -> Option<&String>;
}

pub trait Load {
    fn load(&mut self, mem_storage: &mut MemStorage) -> Result<()>;
}

pub trait Save {
    fn save(&self, mem_storage: &MemStorage) -> Result<()>;
}

pub trait Reset {
    fn reset(&mut self);
}
