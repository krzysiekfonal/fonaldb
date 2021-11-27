use std::path::PathBuf;
use std::io::{Result, Read, Write};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

use super::{Storage, Load, Save};
use std::fs::{OpenOptions, File};
use crate::storage::MemStorage;

pub struct SSTable {
    block_size: usize,
    path: PathBuf,
}

impl SSTable {
    pub fn new(path: PathBuf) -> SSTable {
        SSTable {
            path,
            block_size: 64 * 1024,
        }
    }

    pub fn open(&self, write: bool) -> Result<File> {
        OpenOptions::new().
            read(true).
            write(write).
            create(true).
            open(&self.path)
    }
}

impl Load for SSTable {
    fn load(&mut self, mem_storage: &mut MemStorage) -> Result<()> {
        let mut sstable_file = self.open(false)?;

        while let Ok(key_size) = sstable_file.read_u8() {
            let mut key_buf = vec![0; key_size as usize];
            sstable_file.read_exact(&mut key_buf)?;
            let key = std::str::from_utf8(&key_buf).unwrap();

            let value_size = sstable_file.read_u16::<LittleEndian>()?;
            let mut value_buf = vec![0; value_size as usize];
            sstable_file.read_exact(&mut value_buf)?;
            let value = std::str::from_utf8(&value_buf).unwrap();

            // SSTable loads into memory only in case it is not there (in memory data is
            // always newer one)
            if mem_storage.get(key) == None || mem_storage.is_deleted(key) {
                mem_storage.set(key, value);
            }
        }

        Ok(())
    }
}

impl Save for SSTable {
    fn save(&self, mem_storage: &MemStorage) -> Result<()> {
        let mut file = self.open(true)?;
        mem_storage.scan(|key, value| {
            file.write_u8(key.len() as u8);
            file.write_all(key.as_bytes());
            file.write_u16::<LittleEndian>(value.len() as u16);
            file.write_all(value.as_bytes());
        });

        Ok(())
    }
}
