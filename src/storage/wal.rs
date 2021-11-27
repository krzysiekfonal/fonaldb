use std::fs::{File, OpenOptions};
use std::io::{Result, Write, Read, Seek};
use std::path::PathBuf;

use super::{MemStorage, Storage, Load, Reset};

pub struct WALStorage {
    content: File,
}

impl WALStorage {
    pub fn new(path: PathBuf) -> WALStorage {
        WALStorage {
            content: OpenOptions::new().
                append(true).
                create(true).
                read(true).
                open(path).
                expect("Cannot create WAL file")
        }
    }
}

impl Storage for WALStorage {
    fn set(&mut self, key: &str, value: &str) -> Result<()> {
        if let Err(e) = self.content.write_all(format!("S|{}|{};", key, value).as_bytes()) {
            println!("Cannot execute write operation because of error while writing to WAL: {}", e);
            return Err(e);
        }

        Ok(())
    }

    fn delete(&mut self, key: &str) -> Result<()> {
        if let Err(e) = self.content.write_all(format!("D|{};", key).as_bytes()) {
            println!("Cannot execute write operation because of error while writing to WAL: {}", e);
            return Err(e);
        }

        Ok(())
    }

    fn get(&self, key: &str) -> Option<&String> {
        None
    }
}

impl Load for WALStorage {
    fn load(&mut self, storage: &mut MemStorage) -> Result<()>{
        let mut wal_data = String::new();
        self.content.read_to_string(&mut wal_data); //std::fs::read_to_string(self.db_path.join("wal.dat")).unwrap_or_default();
        println!("Read data from wal {}", wal_data);

        for entry in wal_data.split(';').
            map(|s| s.splitn(3, '|').collect::<Vec<&str>>()) {
            match entry[0] {
                "S" => storage.set(entry[1], entry[2])?,
                "D" => storage.delete(entry[1])?,
                _ => (),
            };
        }

        Ok(())
    }
}

impl Reset for WALStorage {
    fn reset(&mut self) {
        self.content.set_len(0);
        self.content.rewind();
    }
}
