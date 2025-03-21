/*pub fn add(left: u64, right: u64) -> u64 {
    left + right
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let result = add(2, 2);
        assert_eq!(result, 4);
    }
}*/


use http::{request, HeaderName, HeaderValue, Request, Response, StatusCode, Version};
use serde;
use serde_json;

use std::{
    collections::HashMap, env, hash::Hash, io::{prelude::*, BufReader}, net::{TcpListener, TcpStream}, str::FromStr
};

pub struct HttpReader<'a>{
    buf_reader: &'a mut BufReader<&'a TcpStream>,
    line: String,
    found_empty_line: bool
}
impl <'a> HttpReader<'a> {
    pub fn new(buf_reader: &'a mut BufReader<&'a TcpStream>) -> Self{
        return {
            Self { buf_reader, line: "".to_string(), found_empty_line: false }
        }
    }
    pub fn get_n_chars(&mut self, content_length: usize) -> Option<String>{
        if !self.found_empty_line{
            return None;
        }
        let mut buf:Vec<u8> = vec![0; content_length];
        let _ = self.buf_reader.read(&mut buf);
        //return String::from_utf8(buf).ok();
        return Some(String::from_utf8(buf).unwrap());
    }
    pub fn make_request(&mut self) -> Request<Option<String>>{
        let mut http_request: Vec<_> =  self.collect();

        let content_length:Option<usize> =
        http_request.iter()
        .filter(|line| {
            line.contains("Content-Length:")||line.contains("content-length:")
        })
        .filter_map(|line| {line.split(": ")}
        .skip(1)
        .next()?
        .parse::<usize>()
        .ok())
        .next();
        if cfg!(debug_assertions){
            println!("{:#?}",&content_length);
        }
        
        let mut data = None;
        if cfg!(debug_assertions){
            println!("LENTGHT!!! {content_length:#?}");
        }
        if let Some(content_length) = content_length{
            data = self.get_n_chars(content_length);
        }

        let mut builder = Request::builder();
        let binding = http_request.remove(0);
        let mut split = binding.split(" ");
        builder = builder.method(split.next().unwrap()).uri(split.next().unwrap());

        builder = builder.version(
        match split.next().unwrap(){
            "HTTP/0.9"  => Version::HTTP_09,
            "HTTP/1.0"  => Version::HTTP_10,
            "HTTP/1.1" => Version::HTTP_11,
            "HTTP/2.0" => Version::HTTP_2,
            "HTTP/3.0" => Version::HTTP_3,
            _ => Version::default()
        });

        let mut request = builder.body(data).unwrap();

        for request_data in http_request.iter(){
            let request_data_ = request_data.as_str();
            if cfg!(debug_assertions){
                println!("{}", request_data);
            }
            let spl: Vec<String> = request_data_.split(": ").map(|v| v.to_string()).collect();
            //let spl: Vec<String> = request_data_.split(": ").map(|v| v.to_string()).collect();
            if let (Ok(name), Ok(value)) = (HeaderName::from_str(&spl[0]), HeaderValue::from_str(&spl[1].clone())){
                let _ = request.headers_mut().insert(name, value);
            }
        };
        request
    }
}
impl <'a> Iterator for &mut HttpReader <'a>{
    type Item = String;

    fn next(&mut self) -> Option<Self::Item> {
        if self.found_empty_line{
            return None;
        }
        self.line.clear();
        match self.buf_reader.read_line(&mut self.line){
            Ok(s) =>{
                self.line = self.line.lines().collect();
                match s > 0{
                    true => {
                        if self.line.is_empty(){
                            self.found_empty_line = true;
                            return None;
                        }
                        return Some(self.line.clone());
                    },
                    false => {
                        None
                    },
                }
            }
            ,
            Err(_) => {
                self.found_empty_line = true;
                None
            },
        }
    }
}