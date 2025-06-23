use std::{
    io::{prelude::*, BufReader}, net::{TcpListener, TcpStream}
};
use http_reader::HttpReader;
//use reqwest::blocking::{Request, RequestBuilder};
use reqwest::blocking::Client;
use uuid::Uuid;

use clap::Parser;

#[derive(Parser, Default,Debug)]
struct Arguments {
    #[arg(short = 'p', long)]
    pub port : Option<i32>,
    #[arg(long)]
    pub server_config: String,
    #[arg(long)]
    pub mesage_service: String,
    #[arg(long, short = 'd', action)]
    pub debug: bool
}


fn main() {
    let args = Arguments::parse();
    let port = args.port.unwrap_or(7878);
    if args.debug{
        println!("using port {port}")
    }
    let address: String = format!("127.0.0.1:{}", &port);
    let listener = TcpListener::bind(address).unwrap();

    
    for stream in listener.incoming() {
        let stream = stream.unwrap();
        handle_connection(stream, &args.server_config, &args.mesage_service, args.debug);
    }
}

fn handle_connection(mut stream: TcpStream, server_config: &String, message_adress: &str, show_debug:bool){
    let mut buf_reader = BufReader::new(&stream);
    let mut line_consumer = HttpReader::new(&mut buf_reader);
    let request = line_consumer.make_request();
    if show_debug{println!("{:?}", request);}
    
    let mut response = "HTTP/1.1 401 Not Implemented\r\n\r\n".to_owned();

    let http_client = Client::new();

    let adress = 
    match request.method(){
        &http::Method::POST | &http::Method::GET =>{
            match http_client.get(server_config).send() {
                Ok(address) => {
                    match address.bytes(){
                        Ok(adress) => {             
                            match std::str::from_utf8(&adress){
                                Ok(actuall_adress) => {Some(actuall_adress.to_owned())},
                                Err(err) => {
                                    println!("Error found while reading adress of logging service: {}", err);
                                    return;
                                },
                            }
                        },
                        Err(err) =>{
                            println!("Error found while reading adress of logging service: {}", err);
                            return;
                        }
                    }
                },
                Err(err) => {
                    println!("{}", err);
                    return
                }
            }
        }
        _ =>{
            None
        }
    };
    

    match *request.method(){
        http::Method::POST =>{
            response = "HTTP/1.1 500 Internal Server Error\r\n\r\n".to_owned();
            if let Some(body) = request.body(){
                
                let id = Uuid::new_v4();
                
                let http_result = http_client
                .post(format!("{}/post", adress.unwrap()))
                .body(format!("{id}: {body}")).send();
                if show_debug{println!("{:?}", http_result);}
                match http_result{
                    Ok(resp) => {
                        response = 
                        match resp.text(){
                            Ok(val) => val,
                            Err(err) => err.to_string(),
                        }
                    },
                    Err(err) => response = err.to_string(),
                }
            }

            stream.write_all(response.as_bytes()).unwrap();
        }
        http::Method::GET => {
            let http_client = Client::new();
            let http_result = http_client
                .get(adress.unwrap())
                .send();

            
            let http_client = Client::new();
            let http_result_message = http_client
                .get(message_adress.to_string())
                .send();
            let response = 
            
            if let (Ok(a), Ok(b)) = (http_result, http_result_message){
                if let (Ok(text_1), Ok(text_2)) = (a.text(), b.text()){
                    Some(format!("HTTP/1.1 200 OK\r\n\r\n{}; {}\r\n", &text_1, &text_2))
                }else{
                    None
                }
            }else{
                None
            };
            stream.write_all(response.unwrap_or("HTTP/1.1 500 Internal Server Error\r\n\r\n".to_owned()).as_bytes()).unwrap();
            
        }
        _ =>{
            stream.write_all(response.as_bytes()).unwrap();
        }
    }

    
}
