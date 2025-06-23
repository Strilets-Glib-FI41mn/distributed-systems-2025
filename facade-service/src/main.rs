//use http::{request, HeaderName, HeaderValue, Request, Response, StatusCode, Version};
//use serde;
//use serde_json;


//use std::{env, io::BufReader, net::{TcpListener, TcpStream}};
/* */
use std::{
    env, io::{prelude::*, BufReader}, net::{TcpListener, TcpStream}
};
use http_reader::HttpReader;
//use reqwest::blocking::{Request, RequestBuilder};
use reqwest::blocking::Client;
use uuid::Uuid;
use rand::seq::IndexedRandom; // 0.9.1
fn main() {
    let mut port = "7878".to_owned();
    let mut logging_service_ports = vec!["9898".to_owned(), "9899".to_owned(), "9900".to_owned()];
    let message_service_port = "7325".to_owned();
    let mut show_debug = false;
    let args: Vec<_> = env::args().skip(1).collect();
    for arg in &args{
        let (key, value) = 
        match arg.contains('=') {
            true => {
                let str_vec: Vec<&str> = arg.split('=').collect();
                (String::from(str_vec[0]), Some(String::from(str_vec[1])))
            },
            false => {
                (arg.to_owned(), None)
            }
        };
        match key.as_str(){
            "--port" | "-p" => {
                if let Some(port_string) = &value{
                    if let Ok(val) = port_string.parse::<i32>(){
                        if val < 65535{
                            port = port_string.clone();
                        }
                    }
                }
            }
            "--logging_service_ports" => {
                if let Some(ports) = &value{
                    logging_service_ports = vec![];
                    let ports:Vec<&str> =  ports.split(',').collect();
                    for port in ports{
                        if let Ok(val) = port.parse::<i32>(){
                            if val < 65535{
                                    logging_service_ports.push(port.to_owned());
                                }
                            }
                        }
                    }
                }
            "-d" | "--debug" =>{
                show_debug = true;
            }
            _ => {
                println!("Unknown argument {arg}");
            }
        }
    }
    if show_debug{
        for argument in &args {
            println!("{argument}");
        }
        println!("using port {port}")
    }
    for logging_service_port in &logging_service_ports{
        if port == *logging_service_port{
            panic!("facade port {} is equal to logging port {}",&port, &logging_service_port);
        }
    }
    let logging_adressess:Vec<_> = logging_service_ports.into_iter().map(|x| format!( "http://127.0.0.1:{}", &x)).collect();
    let address: String = "127.0.0.1:".to_owned() + &port;
    //let logging_adress: String = "http://127.0.0.1:".to_owned() + &logging_service_port;
    let message_adress: String = "http://127.0.0.1:".to_owned() + &message_service_port;
    let listener = TcpListener::bind(address).unwrap();

    
    for stream in listener.incoming() {
        let stream = stream.unwrap();
        handle_connection(stream, &logging_adressess, &message_adress, show_debug);
        //handle_connection(stream)
    }
}

fn handle_connection(mut stream: TcpStream, logging_adresses: &Vec<String>, message_adress: &str, show_debug:bool){
    let mut buf_reader = BufReader::new(&stream);
    let mut line_consumer = HttpReader::new(&mut buf_reader);
    let request = line_consumer.make_request();
    if show_debug{println!("{:?}", request);}
    
    let mut response = "HTTP/1.1 401 Not Implemented\r\n\r\n";

    let adress = match logging_adresses.choose(&mut rand::rng()) {
        Some(i) => Some(i),
        None    => None
    }.expect("Somehow the fasade service has no logging adresses");

    match *request.method(){
        http::Method::POST =>{
            response = "HTTP/1.1 500 Internal Server Error\r\n\r\n";
            if let Some(body) = request.body(){
                
                let id = Uuid::new_v4();
                let http_client = Client::new();
                let http_result = http_client
                .post(format!("{}/post", adress))
                .body(format!("{id}: {body}")).send();
                if show_debug{println!("{:?}", http_result);}
                response = "HTTP/1.1 200 OK\r\n\r\n";
            }

            stream.write_all(response.as_bytes()).unwrap();
        }
        http::Method::GET => {
            let http_client = Client::new();
            let http_result = http_client
                .get(adress.to_string())
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
