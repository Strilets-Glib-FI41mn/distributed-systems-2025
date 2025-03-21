//use http::{request, HeaderName, HeaderValue, Request, Response, StatusCode, Version};
//use serde;
//use serde_json;


//use std::{env, io::BufReader, net::{TcpListener, TcpStream}};
/* */
use std::{
    collections::HashMap, env, hash::Hash, io::{prelude::*, BufReader}, net::{TcpListener, TcpStream}, str::FromStr
};
use http_reader::HttpReader;
//use reqwest::blocking::{Request, RequestBuilder};
use reqwest::blocking::{Client, ClientBuilder};
use uuid::Uuid;
fn main() {
    let mut port = "7878".to_owned();
    let mut logging_service_port = "9898".to_owned();
    let mut message_service_port = "7325".to_owned();
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
            "--logging_service_port" => {
                if let Some(port_string) = &value{
                    if let Ok(val) = port_string.parse::<i32>(){
                        if val < 65535{
                            logging_service_port = port_string.clone();
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
    if &port == &logging_service_port{
        panic!("facade port {} is equal to logging port {}",&port, &logging_service_port);
    }
    let address: String = "127.0.0.1:".to_owned() + &port;
    let logging_adress: String = "http://127.0.0.1:".to_owned() + &logging_service_port;
    let message_adress: String = "http://127.0.0.1:".to_owned() + &message_service_port;
    let listener = TcpListener::bind(address).unwrap();

    
    for stream in listener.incoming() {
        let stream = stream.unwrap();
        handle_connection(stream, &logging_adress, &message_adress, show_debug);
        //handle_connection(stream)
    }
}

fn handle_connection(mut stream: TcpStream, logging_adress: &str, message_adress: &str, show_debug:bool){
    let mut buf_reader = BufReader::new(&stream);
    let mut line_consumer = HttpReader::new(&mut buf_reader);
    let request = line_consumer.make_request();
    if show_debug{println!("{:?}", request);}
    
    let mut response = "HTTP/1.1 401 Not Implemented\r\n\r\n";

    match *request.method(){
        http::Method::POST =>{
            response = "HTTP/1.1 202\r\n\r\n";
            if let Some(body) = request.body(){
                let id = Uuid::new_v4();
                let http_client = Client::new();
                let http_result = http_client
                .post(format!("{}/post", logging_adress))
                .body(format!("{id}: {body}")).send();
                if show_debug{println!("{:?}", http_result);}
                //let http_client = ClientBuilder::new();
                response = "HTTP/1.1 200 OK\r\n\r\n";
            }

            stream.write_all(response.as_bytes()).unwrap();
        }
        http::Method::GET => {
            let http_client = Client::new();
            let http_result = http_client
                .get(format!("{}", logging_adress))
                .send();

            
            let http_client = Client::new();
            let http_result_message = http_client
                .get(format!("{}", message_adress))
                .send();
            let response = 
            
            if let (Ok(a), Ok(b)) = (http_result, http_result_message){
                if let (Ok(text_1), Ok(text_2)) = (a.text(), b.text()){
                    Some(format!("HTTP/1.1 202 Ok\r\n\r\n{}; {}", &text_1, &text_2))
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
