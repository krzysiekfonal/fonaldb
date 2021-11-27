use crate::storage::Storage;

pub struct Executor<'a> {
    storage: &'a mut dyn Storage,
}

impl<'a> Executor<'a> {
    pub fn new(storage: &'a mut dyn Storage) -> Executor {
        Executor {
            storage,
        }
    }

    pub fn set(&mut self, key: &str, value: &str) {
        self.storage.set(key, value);
    }

    pub fn del(&mut self, key: &str) {
        self.storage.delete(key);
    }

    pub fn get(&self, key: &str) -> Option<&String> {
        self.storage.get(key)
    }
}
