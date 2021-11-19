use std::collections::BTreeMap;
use std::fs::File;
use std::io::{Result, Write};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

use super::Storage;

pub struct MemStorage {
    content: BTreeMap<String, String>,
    mem_usage: usize,
}

impl MemStorage {
    pub fn new() -> MemStorage {
        MemStorage {
            content: BTreeMap::new(),
            mem_usage: 0,
        }
    }

    pub fn get_mem_usage(&self) -> usize {
        self.mem_usage
    }

    pub fn snapshot(&self, file: &mut File) {
        for (key, value) in self.content.iter() {
            file.write_u8(key.len() as u8);
            file.write_all(key.as_bytes());
            file.write_u16::<LittleEndian>(value.len() as u16);
            file.write_all(value.as_bytes());
        }
    }
}

impl Storage for MemStorage {
    fn set(&mut self, key: &str, value: String) -> Result<()> {
        let bytes = key.len() + value.len();
        let old_value = self.content.insert(String::from(key), value);
        self.mem_usage += bytes;

        if let Some(v) = old_value {
            self.mem_usage -= v.len();
        }

        Ok(())
    }

    fn delete(&mut self, key: &str) -> Result<()> {
        if let Some(v) = self.content.remove(key) {
            self.mem_usage -= v.len() + key.len();
        }

        Ok(())
    }

    fn get(&self, key: &str) -> Option<&String> {
        self.content.get(key)
    }
}