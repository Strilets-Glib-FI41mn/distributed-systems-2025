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
    if debug{println!("Request aquired:\n{:?}", request);}
    
    let mut response = "HTTP/1.1 401 Not Implemented\r\n\r\n".to_owned();

    let http_client = Client::new();

    let logging_adresses = 
    match request.method(){
        &http::Method::POST | &http::Method::GET =>{
            match get_data_from_config(server_config, "logging", debug){
                Some(ok) => Some(ok),
                None => return,
            }
        }
        _ =>{
            None
        }
    };
    let logging_adresses = serde_json::from_str(&logging_adresses.unwrap_or("".to_owned())).unwrap_or(Vec::<String>::new());

    if debug{
        println!("Logging adresses:\n{:#?}", &logging_adresses);
    }
    match *request.method(){
        http::Method::POST =>{
            
            response = "HTTP/1.1 500 Internal Server Error\r\n\r\n".to_owned();
            let mut code = "400 Bad Request";
            if let Some(body) = request.body(){
                
                let id = Uuid::new_v4();
                let mut http_result;// = Ok(String::default());
                for logging_adress in logging_adresses{
                    http_result = http_client
                    .post(format!("http://{}/post", logging_adress))
                    .body(format!("{id}: {body}")).send();
                    if debug{println!("Result of sending put request to logging at {}\n{:?}", &logging_adress, &http_result);}
                    match http_result{
                        Ok(resp) => {
                            match resp.text(){
                                Ok(val) => {
                                    code = "200 OK";
                                    response = val.clone();
                                    break;
                                },
                                Err(err) => response = err.to_string(),
                            }
                        },
                        Err(err) => response = err.to_string(),
                    }
                    };
                    
                }
                stream.write_all(
                    format!("HTTP/1.1 {}\n\nContent-Length: {}\n\n{}",&code, response.as_bytes().len(), &response).as_bytes()
                ).unwrap();
            }
        
        http::Method::GET => {
            let http_client = Client::new();
            let mut http_result_logging = None ;
            for logging_adress in logging_adresses{
                http_result_logging = Some(http_client
                .get(format!("http://{}/get", logging_adress))
                .send());
                if debug{println!("Result of sending get request to loggig at {}\n{:?}", &logging_adress, &http_result_logging);}
                if let Some(res) =  &http_result_logging{
                    if res.is_ok(){
                        break
                    }
                }
            }
            
            let http_client = Client::new();
            let message_adresses =  get_data_from_config(server_config, "message", debug);

            let mut http_result_message = None ;

            let message_adresses = serde_json::from_str(&message_adresses.unwrap_or("".to_owned())).unwrap_or(Vec::<String>::new());
            if debug{
                println!("{:#?}", &message_adresses);
            }
            for message_adress in message_adresses{
                http_result_message = Some(http_client
                .get(format!("http://{}/get", message_adress))
                .send());
            if debug{println!("Result of sending get request to message at {}\n{:?}", &message_adress, &http_result_logging);}
            if let Some(res) =  &http_result_message{
                if res.is_ok(){
                    break
                }
            }
            }
            
            let (http_result_logging_res, http_result_message_res) = (http_result_logging.expect("logging addresses were not provided"), http_result_message.expect("message adresses were not provided"));

            let response = 
            
            match (http_result_logging_res, http_result_message_res){
                (Ok(l), Ok(m)) => {
                    if debug{
                        println!("Responses:\n{:?} {:?}\r\n", &l, &m)
                    }
                    if let (Ok(text_1), Ok(text_2)) = (l.text(), m.text()){
                        if debug{
                            println!("{} {}\r\n", &text_1, &text_2)
                        }
                        let both_text = format!("logging: {}\nmessage: {}",  &text_1, &text_2);
                        Some(format!("HTTP/1.1 200 OK\nContent-Length: {}\n\n{}", both_text.as_bytes().len(), both_text))
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
                        Ok(actuall_adress) => {
                            Some(actuall_adress.to_owned().to_string())},
                        Err(err) => {
                            println!("Error found while converting adress of {} service to UTF-8: {}", name, err);
                            None
                        },
                    }
                },
                Err(err) =>{
                    println!("Error found while reading adress of {} service: {}", name, err);
                    None
                }
            }
        },
        Err(err) => {
            println!("Response error: {}", err);
            None
        }
    }
}