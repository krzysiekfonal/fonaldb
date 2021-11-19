use std::io;
use fonaldb::processor::Processor;
use fonaldb::storage::{LSMStorage, MemStorage, Storage};

fn main() {
    let mut buffer = String::new();
    let stdin = io::stdin();
    let mut storage = LSMStorage::new(100, 100, "my_db", "/Users/krzysztoffonal/.fonaldb");
    let mut processor = Processor::new(&mut storage);

    stdin.read_line(&mut buffer).unwrap();
    while buffer.trim() != "exit" {
        processor.process(buffer.as_str());
        buffer.clear();
        stdin.read_line(&mut buffer).unwrap();
    }
}
