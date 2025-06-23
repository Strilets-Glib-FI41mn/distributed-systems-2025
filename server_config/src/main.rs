use std::fs;
use serde::Deserialize;

use std::{
    io::{prelude::*, BufReader}, net::{TcpListener, TcpStream}
};
use http_reader::HttpReader;
use rand::seq::IndexedRandom; // 0.9.1


use clap::Parser;
#[derive(Parser,Default,Debug)]
struct Arguments {
    #[arg(short = 'p', long = "path")]
    pub port : i32,
    #[arg(group = "source", long = "path")]
    pub filepath: Option<String>,
    #[arg(group = "source", long, value_delimiter = ' ', num_args = 1..)]
    pub list: Option<Vec<String>>,
    #[arg(long, short = 'd', action)]
    pub debug: bool
}
#[derive(Deserialize)]
struct Config{pub adresses: Vec<String>}
fn main() {
    let args = Arguments::parse();
    print!("{:?}", args);
    //return;
    
    let config =
    match args.list{
        Some(logging) => Config{adresses: logging},
        None => {
            match args.filepath{
                Some(path) => {
                    let contents = fs::read_to_string(&path)
                    .expect("Something went wrong reading the file");
                    toml::from_str(&contents)
                    .expect("Failed to parse TOML of the server configuration file")
                },
                None => panic!("No IPs for logging service were provided!"),
            }
        },
    };

    let port = args.port;
    let show_debug = args.debug;
    
    let config_adress: String = format!("127.0.0.1:{}", &port);
    let listener = TcpListener::bind(config_adress).unwrap();

    
    for stream in listener.incoming() {
        let stream = stream.unwrap();
        handle_connection(stream,  &config.adresses, show_debug);
    }
}

fn handle_connection(mut stream: TcpStream, logging_adresses: &Vec<String>, show_debug:bool){
    let mut buf_reader = BufReader::new(&stream);
    let mut line_consumer = HttpReader::new(&mut buf_reader);
    let request = line_consumer.make_request();
    if show_debug{println!("{:?}", request);}
    
    let mut response = "HTTP/1.1 401 Not Implemented\r\n\r\n".to_owned();

    let adress = logging_adresses.choose(&mut rand::rng()).expect("Somehow the config service has no logging adresses");

    match *request.method(){
        http::Method::GET => {
            response = format!("HTTP/1.1 200 OK\nContent-Type: plain/text\nContent-Length: {}\n\n{}", adress.as_bytes().len(), adress);
            let _ = stream.write_all(response.as_bytes());
        }
        _ =>{
            stream.write_all(response.as_bytes()).unwrap();
        }
    }

    
}
