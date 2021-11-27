use std::fs::{OpenOptions, File};
use std::io::{Result, Write, Read, Seek};
use std::path::PathBuf;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

use super::wal::WALStorage;
use super::memory::MemStorage;
use super::sstable::SSTable;
use super::{Storage, Load, Save, Reset};

pub struct LSMStorage {
    content: MemStorage,
    buffered_content: Option<MemStorage>,
    mem_capacity: usize,
    file_capacity: usize,
    wal: WALStorage,
    path: PathBuf,
    sstable: SSTable,
}

impl LSMStorage {
    pub fn new(mem_capacity: usize, file_capacity: usize, db_name: &str, path: &str) -> LSMStorage {
        let mut path = PathBuf::from(path);
        path.push(db_name);
        std::fs::create_dir_all(&path).expect("Cannot create db path");
        let mut lsm = LSMStorage {
            content: MemStorage::new(),
            buffered_content: None,
            mem_capacity,
            file_capacity,
            wal: WALStorage::new(path.join("wal.dat")),
            sstable: SSTable::new(path.join("level0.dat")),
            path,
        };
        lsm.wal.load(&mut lsm.content);
        return lsm;
    }

    fn merge(&mut self) -> Result<()>{
        println!("LSM procedure launched");
        // move content to buffered_content and clear WAL
        self.buffered_content = Some(std::mem::replace(&mut self.content,MemStorage::new()));
        self.wal.reset();
        let mut buffered_content = self.buffered_content.as_mut().unwrap();

        self.sstable.load(buffered_content);

        // save merged content from buffered_content
        self.sstable.save(buffered_content);

        Ok(())
    }
}

impl Storage for LSMStorage {
    fn set(&mut self, key: &str, value: &str) -> Result<()> {
        self.wal.set(key, value)?;
        self.content.set(key, value);

        if self.content.get_mem_usage() > self.mem_capacity {
            self.merge();
        }

        Ok(())
    }

    fn delete(&mut self, key: &str) -> Result<()> {
        self.wal.delete(key)?;
        self.content.delete(key);

        Ok(())
    }

    fn get(&self, key: &str) -> Option<&String> {
        self.content.get(key).or_else(|| {
            if let Some(buffer) = &self.buffered_content {
                buffer.get(key)
            } else {
                None
            }
        })
    }
}
