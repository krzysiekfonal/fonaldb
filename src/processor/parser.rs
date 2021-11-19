pub trait Parser {
    fn parse<'a>(&self, raw_query: &'a str) -> Vec<&'a str>;
}

pub struct SimpleParser;

impl Parser for SimpleParser {
    fn parse<'a>(&self, raw_query: &'a str) -> Vec<&'a str> {
        raw_query.splitn(3, char::is_whitespace).collect()
    }
}