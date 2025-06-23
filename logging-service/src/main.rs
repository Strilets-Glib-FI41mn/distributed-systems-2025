use std::{env, io::{prelude::*, BufReader}, net::{TcpListener, TcpStream}, str::FromStr};

use http_reader::HttpReader;

use uuid::Uuid;
use hazelcast_rest::HazelcastRestClient;
use std::process::Command;

fn main() {
    //let mut port = "7878".to_owned();
    //let storage = Arc::new(Mutex::new(HashMap::new()));
    let mut logging_service_port = "9898".to_owned();
    let mut hazelcast_port = "5701".to_owned();
    let mut show_debug = false;
    let mut hazelcast_map = "test".to_owned();
    let mut hazelcast_ip = "127.0.0.1".to_owned();
    let mut listening_ip = "127.0.0.1".to_owned();
    let mut cluster_name = "lab".to_owned();
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
            "--hazelcast_ip" => {
                if let Some(val) = &value{
                    hazelcast_ip = val.clone();
                }
            }
            "--listening_ip" =>{
                if let Some(val) = &value{
                    listening_ip = val.clone();
                }
            }
            "--cluster_name" =>{
                if let Some(val) = &value{
                    cluster_name = val.clone();
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
    let logging_adress: String = format!("{}:{}", listening_ip.clone(), &logging_service_port); // hazelcast_ip.to_owned() + &logging_service_port;
    //let hazelcast_adress: String = hazelcast_ip.to_owned() + &hazelcast_port;
    let listener = TcpListener::bind(logging_adress).unwrap();
    //let hazelcast_port = Arc::<String>::new(hazelcast_port);
    
    for stream in listener.incoming() {
        let stream = stream.unwrap();
        handle_connection(stream, show_debug, hazelcast_ip.to_owned(), hazelcast_port.clone(), &hazelcast_map, &cluster_name);
        //handle_connection(stream)
    }
}

fn handle_connection(mut stream: TcpStream, show_debug: bool, hazelcast_ip: String, hazelcast_port: String, hazelcast_map: &str, cluster_name: &str){
    let mut buf_reader = BufReader::new(&stream);
    let mut line_consumer = HttpReader::new(&mut buf_reader);
    let request = line_consumer.make_request();
    if show_debug {println!("{:?}", request);}


    match *request.method(){
        http::Method::POST => {
            if let Some(body) = request.body(){
            
                let values: Vec<&str> = body.split(": ").take(2).collect();
                if values.len() == 2{
                    if let Ok(_) =  Uuid::from_str(values[0]){
                        
                        //let mut storage = data.lock().unwrap();
                        //storage.insert(val.clone(), values[1].to_owned().clone());
                        
                        if show_debug {
                            println!("key for the hazelcast: {} hazelcast value: {}", &values[0], &values[1]);
                            println!("IP: {} port: {}", &hazelcast_ip, &hazelcast_port);
                        }
                        let client = HazelcastRestClient::new(&hazelcast_ip, &hazelcast_port);
                        //use serde_json::json;

                        //let json_data = json!({ "value": values[1]});
                        let res = client.map_put(hazelcast_map,&Into::<String>::into(values[0]), &vec![("Content-Type", "plain/text"), ("factoryId", "-1")], values[1]);
                        if show_debug {
                            println!("{res:?}");
                        }

                        match res{
                            Ok(response) => {
                                //let response = "HTTP/1.1 201 Created\r\n\r\n";
                                stream.write_all(response.as_bytes()).unwrap();
                            },
                            Err(_) => {
                                let response = "HTTP/1.1 500 Internal Server Error\r\n\r\n";
                                stream.write_all(response.as_bytes()).unwrap();
                            },
                        }
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
            let path = format!("{}:{}",&hazelcast_ip,  &hazelcast_port);
            let output = Command::new("zsh")

            .args(["run_get_data.sh".into(), "--ip", &path, "--cluster_name", &cluster_name, "--map_name", &hazelcast_map])
            .output();
            if show_debug{

                let path = env::current_dir().unwrap();
                println!("The current directory is {}", path.display());
                println!("Output of runnig the script is: {:?}", output);
                println!("{}", core::str::from_utf8(&Command::new("ls").arg("run_get_data.sh").output().unwrap().stdout).unwrap());
            }
            if let Ok(output) = output{
                match core::str::from_utf8(&output.stdout){
                    Ok(output) =>{
                        let response = format!("HTTP/1.1 200 OK\nContent-Length: {}\n\n{}", output.as_bytes().len(), output);
                        stream.write_all(response.to_string().as_bytes()).unwrap();
                    }
                    Err(er) =>{
                        let response = format!("HTTP/1.1 500 Internal Server Error\nContent-Length: {}\n\n{}", &er.to_string().as_bytes().len(), &er.to_string());
                        stream.write_all(response.as_bytes()).unwrap();
                    }
                }
            }
        }
        _ =>{
            
            let response = "HTTP/1.1 501 Not Implemented\r\n\r\n";
            stream.write_all(response.as_bytes()).unwrap();
        }
    }

}
