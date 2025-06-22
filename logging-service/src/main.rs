use std::{collections::HashMap, env, io::{prelude::*, BufReader}, net::{TcpListener, TcpStream}, str::FromStr, sync::{Arc, Mutex}};

use http_reader::HttpReader;

use uuid::Uuid;
use hazelcast_rest::HazelcastRestClient;

fn main() {
    //let mut port = "7878".to_owned();
    let storage = Arc::new(Mutex::new(HashMap::new()));
    let mut logging_service_port = "9898".to_owned();
    let mut hazelcast_port = "5701".to_owned();
    let mut show_debug = false;
    let mut hazelcast_map = "map".to_owned();
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
            "--port" | "-p" | "--logging_service_port" => {
                if let Some(port_string) = &value{
                    if let Ok(val) = port_string.parse::<i32>(){
                        if val < 65535{
                            logging_service_port = port_string.clone();
                        }
                    }
                }
            }"--hazelcast_port" => {
                if let Some(port_string) = &value{
                    if let Ok(val) = port_string.parse::<i32>(){
                        if val < 65535{
                            hazelcast_port = port_string.clone();
                        }
                    }
                }
            }
            "--hazelcast_map" => {
                if let Some(val) = &value{
                    hazelcast_map = val.into();
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
        println!("using port {logging_service_port}");
    }
    let ip_address = "127.0.0.1:";
    let logging_adress: String = ip_address.to_owned() + &logging_service_port;
    //let hazelcast_adress: String = ip_address.to_owned() + &hazelcast_port;
    let listener = TcpListener::bind(logging_adress).unwrap();
    //let hazelcast_port = Arc::<String>::new(hazelcast_port);
    
    for stream in listener.incoming() {
        let stream = stream.unwrap();
        handle_connection(stream, storage.clone(), show_debug, ip_address.to_owned(), hazelcast_port.clone(), &hazelcast_map);
        //handle_connection(stream)
    }
}

fn handle_connection(mut stream: TcpStream, data: Arc<Mutex<HashMap<Uuid, String>>>, show_debug: bool, ip_address: String, hazelcast_port: String, hazelcast_map: &str){
    let mut buf_reader = BufReader::new(&stream);
    let mut line_consumer = HttpReader::new(&mut buf_reader);
    let request = line_consumer.make_request();
    if show_debug {println!("{:?}", request);}


    match *request.method(){
        http::Method::POST => {
            if let Some(body) = request.body(){
            
                let values: Vec<&str> = body.split(": ").take(2).collect();
                if values.len() == 2{
                    if let Ok(val) =  Uuid::from_str(values[0]){
                        
                        println!("Got POST request with data: '{:#?}'", &values);
                        let mut storage = data.lock().unwrap();
                        storage.insert(val.clone(), values[1].to_owned().clone());
                        
                        let client = HazelcastRestClient::new(&ip_address, &hazelcast_port);
                        let res = client.map_put::<String>(hazelcast_map,&Into::<String>::into(val), values[1].to_owned());
                        println!("{res:?}");

                    let response = "HTTP/1.1 201 Created\r\n\r\n";
                    stream.write_all(response.as_bytes()).unwrap();
                    }
                }else{
                    let response = "HTTP/1.1 500 Internal Server Error\r\n\r\n";
                    stream.write_all(response.as_bytes()).unwrap();
                }
                
                //let http_client = ClientBuilder::new();
            }else{

                let response = "HTTP/1.1 401\r\n\r\n";
                stream.write_all(response.as_bytes()).unwrap();
            }
        }
        http::Method::GET => {
            let storage = data.lock().unwrap();
            let body: Vec<_> = storage.iter().map(|(_, msg)| msg.clone()).collect();
            let body = body.join(", ").to_string();
            let length = body.as_bytes().len();
            
            let response = format!("HTTP/1.1 200 OK\nContent-Length: {length}\n\n{body}");
            
            stream.write_all(response.to_string().as_bytes()).unwrap();
        }
        _ =>{
            
            let response = "HTTP/1.1 501 Not Implemented\r\n\r\n";
            stream.write_all(response.as_bytes()).unwrap();
        }
    }

}
