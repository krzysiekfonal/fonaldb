use std::fs::{OpenOptions, File};
use std::io::{Result, Write, Read, Seek};
use std::path::{PathBuf};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

use super::memory::MemStorage;
use super::Storage;

pub struct LSMStorage {
    content: Box<MemStorage>,
    buffered_content: Option<Box<MemStorage>>,
    mem_capacity: usize,
    file_capacity: usize,
    wal: File,
    path: PathBuf,
}

impl LSMStorage {
    pub fn new(mem_capacity: usize, file_capacity: usize, db_name: &str, path: &str) -> LSMStorage {
        let mut path = PathBuf::from(path);
        path.push(db_name);
        std::fs::create_dir_all(&path).expect("Cannot create db path");
        let mut disk_storage = LSMStorage {
            content: Box::new(MemStorage::new()),
            buffered_content: None,
            mem_capacity,
            file_capacity,
            wal: OpenOptions::new().
                append(true).
                create(true).
                read(true).
                open(path.join("wal.dat")).
                expect("Cannot create WAL file"),
            path,
        };
        disk_storage.init_data_from_wal();
        return disk_storage;
    }

    fn init_data_from_wal(&mut self) {
        let mut wal_data = String::new();
        self.wal.read_to_string(&mut wal_data); //std::fs::read_to_string(self.db_path.join("wal.dat")).unwrap_or_default();
        println!("Read data from wal {}", wal_data);

        for entry in wal_data.split(';').
            map(|s| s.splitn(3, '|').collect::<Vec<&str>>()) {
            match entry[0] {
                "S" => self.content.set(entry[1], entry[2].to_string()).unwrap(),
                "D" => self.content.delete(entry[1]).unwrap(),
                _ => (),
            };
        }
    }

    fn add_to_wal(&mut self, operation: String) -> Result<()> {
        if let Err(e) = self.wal.write_all(operation.as_bytes()) {
            println!("Cannot execute write operation because of error while writing to WAL: {}", e);
            return Err(e);
        }

        Ok(())
    }

    fn lsm(&mut self) -> Result<()>{
        println!("LSM procedure launched");
        // move content to buffered_content and clear WAL
        self.buffered_content = Some(std::mem::replace(
            &mut self.content,
            Box::new(MemStorage::new())));
        self.wal.set_len(0);
        self.wal.rewind();
        let mut buffered_content = self.buffered_content.as_mut().unwrap();

        // read level0 file into memory
        let mut level0_file = OpenOptions::new().
            read(true).
            write(true).
            create(true).
            open(self.path.join("level0.dat")).
            expect("Cannot open/create level0 file");

        // read level0_file into (key,value) pairs and merge them with buffered_content
        // let mut level0_content = BTreeMap::new();
        while let Ok(key_size) = level0_file.read_u8() {
            let mut key_buf = vec![0; key_size as usize];
            level0_file.read_exact(&mut key_buf)?;
            let key = std::str::from_utf8(&key_buf).unwrap();

            let value_size = level0_file.read_u16::<LittleEndian>()?;
            let mut value_buf = vec![0; value_size as usize];
            level0_file.read_exact(&mut value_buf)?;
            let value = std::str::from_utf8(&value_buf).unwrap();

            if let None = buffered_content.get(key) {
                buffered_content.set(key, value.to_string());
            }
        }

        // save merged content from buffered_content
        level0_file.rewind();
        level0_file.set_len(0);
        buffered_content.snapshot(&mut level0_file);

        Ok(())
    }
}

impl Storage for LSMStorage {
    fn set(&mut self, key: &str, value: String) -> Result<()>{
        self.add_to_wal(format!("S|{}|{};", key, &value))?;
        self.content.set(key, value);

        if self.content.get_mem_usage() > self.mem_capacity {
            self.lsm();
        }

        Ok(())
    }

    fn delete(&mut self, key: &str) -> Result<()>{
        self.add_to_wal(format!("D|{};", key))?;
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