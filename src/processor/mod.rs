mod executor;
mod parser;

use executor::Executor;
use parser::{Parser, SimpleParser};
use crate::storage::Storage;

pub struct Processor<'a> {
    executor: Executor<'a>,
    parser: Box<dyn Parser>,
}

impl<'a> Processor<'a> {
    pub fn new(storage: &'a mut dyn Storage) -> Processor {
        Processor {
            executor: Executor::new(storage),
            parser: Box::new(SimpleParser),
        }
    }

    pub fn process(&mut self, statement: &str) {
        for raw_query in statement.trim().split_terminator(";") {
            let terms = self.parser.parse(raw_query);

            match terms[0] {
                "set" => {
                    self.executor.set(terms[1], terms[2].to_string());
                    println!("Value {} set as {}", terms[1], terms[2]);
                },
                "del" => {
                    self.executor.del(terms[1]);
                    println!("Key {} deleted", terms[1]);
                },
                "get" => match self.executor.get(terms[1]) {
                    Some(v) => println!("{}", v),
                    None => println!("Value not found"),
                },
                &_ => panic!("Wrong command!"),
            }
        }
    }
}
