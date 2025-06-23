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
    //#[arg(long)]
    //pub mesage_service: String,
    #[arg(long, short = 'd', action)]
    pub debug: bool
}


fn main() {
    let args = Arguments::parse();
    let port = args.port.unwrap_or(7878);
    if args.debug{
        println!("using port {port}")
    }
    let facade_address: String = format!("127.0.0.1:{}", &port);
    let listener = TcpListener::bind(facade_address).unwrap();

    
    for stream in listener.incoming() {
        let stream = stream.unwrap();
        handle_connection(stream, &args.server_config,  args.debug);
    }
}

fn handle_connection(mut stream: TcpStream, server_config: &String, debug:bool){
    let mut buf_reader = BufReader::new(&stream);
    let mut line_consumer = HttpReader::new(&mut buf_reader);
    let request = line_consumer.make_request();
    if debug{println!("{:?}", request);}
    
    let mut response = "HTTP/1.1 401 Not Implemented\r\n\r\n".to_owned();

    let http_client = Client::new();

    let logging_adress = 
    match request.method(){
        &http::Method::POST | &http::Method::GET =>{
            match get_data_from_config(&server_config, "logging", debug){
                Some(ok) => Some(ok),
                None => return,
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
                .post(format!("{}/post", logging_adress.unwrap()))
                .body(format!("{id}: {body}")).send();
                if debug{println!("{:?}", http_result);}
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
            let http_result_logging = http_client
                .get(format!("http://{}/get", logging_adress.unwrap()))
                .send();

            
            let http_client = Client::new();
            let message_adress =  get_data_from_config(&server_config, "message", debug);
            if message_adress.is_none(){
                stream.write_all("HTTP/1.1 500 Internal Server Error\r\n\r\n".to_owned().as_bytes()).unwrap();
                return;
            }
            let http_result_message = http_client
                .get(format!("http://{}/get",message_adress.unwrap().to_string()))
                //.get(format!("{}/get",message_adress.to_string()))
                .send();
            if debug{
                println!("http_result_logging {:?}", &http_result_logging);
                println!("http_result_message {:?}", &http_result_message);
            }
            let response = 
            
            match (http_result_logging, http_result_message){
                (Ok(l), Ok(m)) => {
                    if debug{
                        println!("Responses:\n{:?} {:?}\r\n", &l, &m)
                    }
                    if let (Ok(text_1), Ok(text_2)) = (l.text(), m.text()){
                        if debug{
                            println!("{} {}\r\n", &text_1, &text_2)
                        }
                        Some(format!("{} {}\r\n", &text_1, &text_2))
                    }else{
                        None
                    }
                },
                (Ok(_), Err(message_error)) => {
                    println!("Message error: {}", message_error);
                    None
                },
                (Err(logging_error), Ok(_)) => {
                    println!("Logging error: {}", logging_error);
                    None
                },
                (Err(message_error), Err(logging_error)) => {
                    println!("Message error: {}", message_error);
                    println!("Logging error: {}", logging_error);
                    None
                },
            };
            stream.write_all(response.unwrap_or("HTTP/1.1 500 Internal Server Error\r\n\r\n".to_owned()).as_bytes()).unwrap();
            
        }
        _ =>{
            stream.write_all(response.as_bytes()).unwrap();
        }
    }

    
}


fn get_data_from_config(server_config: &str, name: &str, debug: bool) -> Option<String>{
    let http_client = Client::new();
    match http_client.get(format!("http://{}/get/{}",server_config, name)).send() {
        Ok(address) => {
            if debug{
                println!("{:#?}", &address);
            }
            match address.bytes(){
                Ok(adress) => {             
                    match std::str::from_utf8(&adress){
                        Ok(actuall_adress) => {println!("address???? {}", actuall_adress); Some(format!("{}",actuall_adress.to_owned()))},
                        Err(err) => {
                            println!("Error found while converting adress of {} service to UTF-8: {}", name, err);
                            return None;
                        },
                    }
                },
                Err(err) =>{
                    println!("Error found while reading adress of {} service: {}", name, err);
                    return None;
                }
            }
        },
        Err(err) => {
            println!("Response error: {}", err);
            return None;
        }
    }
}