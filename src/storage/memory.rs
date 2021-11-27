use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::io::{Result, Write};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

use super::Storage;
use std::borrow::BorrowMut;

pub struct MemStorage {
    content: BTreeMap<String, String>,
    deleted_content: BTreeSet<String>,
    mem_usage: usize,
}

impl MemStorage {
    pub fn new() -> MemStorage {
        MemStorage {
            content: BTreeMap::new(),
            deleted_content: BTreeSet::new(),
            mem_usage: 0,
        }
    }

    pub fn get_mem_usage(&self) -> usize {
        self.mem_usage
    }

    pub fn scan<F>(&self, mut op: F) where F: FnMut(&str, &str)->() {
        self.content.iter().for_each(|(k,v)| op(k,v));
    }

    pub fn is_deleted(&self, key: &str) -> bool {
        self.deleted_content.contains(key)
    }
}

impl Storage for MemStorage {
    fn set(&mut self, key: &str, value: &str) -> Result<()> {
        let old_value = self.content.insert(
            String::from(key),
            String::from(value)
        );
        self.mem_usage += key.len() + value.len();

        if let Some(v) = old_value {
            self.mem_usage -= v.len();
        }

        Ok(())
    }

    fn delete(&mut self, key: &str) -> Result<()> {
        if let Some(v) = self.content.remove(key) {
            self.mem_usage -= v.len() + key.len();
            self.deleted_content.insert(key.to_string());
        }

        Ok(())
    }

    fn get(&self, key: &str) -> Option<&String> {
        self.content.get(key)
    }
}
