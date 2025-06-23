use std::{env, io::{prelude::*, BufReader}, net::{TcpListener, TcpStream}, str::FromStr};

use http_reader::HttpReader;

use uuid::Uuid;
use hazelcast_rest::HazelcastRestClient;
use std::process::Command;


use clap::Parser;
#[derive(Parser,Default,Debug)]
struct Arguments {
    #[arg(short = 'p', long = "port", value_name = "PORT of logging service")]
    pub port : i32,
    #[arg(value_name = "IP of logging service")]
    pub ip: Option<String>,
    #[arg(long, short = 'd', action)]
    pub debug: bool,
    #[arg(long, value_name = "PORT of hazelcast instance")]
    pub hazelcast_port: Option<i32>,
    #[arg(long, value_name = "IP of hazelcast instance")]
    pub hazelcast_ip: Option<String>,
    #[arg(long, value_name = "MAP of name on Hazelcast")]
    pub hazelcast_map: String,
    #[arg(long, value_name = "NAME of the Hazelcast cluster")]
    pub cluster_name: String
}


fn main() {
    let args = Arguments::parse();
    let hazelcast_port = args.hazelcast_port.unwrap_or(5701);
    let hazelcast_ip = args.hazelcast_ip.as_ref().map_or("127.0.0.1", |v| v);
    let listening_ip = args.ip.as_ref().map_or("127.0.0.1", |v| v);
    
    let logging_adress: String = format!("{}:{}", listening_ip, &args.port); 
    let listener = TcpListener::bind(logging_adress).unwrap();
    
    for stream in listener.incoming() {
        let stream = stream.unwrap();
        handle_connection(stream, args.debug, hazelcast_ip.to_owned(), hazelcast_port.clone(), &args.hazelcast_map, &args.cluster_name);
    }
}

fn handle_connection(mut stream: TcpStream, show_debug: bool, hazelcast_ip: String, hazelcast_port: i32, hazelcast_map: &str, cluster_name: &str){
    let mut buf_reader = BufReader::new(&stream);
    let mut line_consumer = HttpReader::new(&mut buf_reader);
    let request = line_consumer.make_request();
    if show_debug {println!("{:?}", request);}


    match *request.method(){
        http::Method::POST => {
            if let Some(body) = request.body(){
            
                let values: Vec<&str> = body.split(": ").take(2).collect();
                if values.len() == 2{
                    if Uuid::from_str(values[0]).is_ok(){
                        
                        let client = HazelcastRestClient::new(&hazelcast_ip, &hazelcast_port);

                        let res = client.map_put(hazelcast_map,&Into::<String>::into(values[0]), &vec![("Content-Type", "plain/text"), ("factoryId", "-1")], values[1]);
                        if show_debug {
                            println!("{res:?}");
                        }

                        match res{
                            Ok(response) => {
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
            }else{
                
                let response = "HTTP/1.1 401\r\n\r\n";
                stream.write_all(response.as_bytes()).unwrap();
            }
        }
        http::Method::GET => {
            let path = format!("{}:{}",&hazelcast_ip,  &hazelcast_port);
            let output = Command::new("zsh")

            .args(["run_get_data.sh", "--ip", &path, "--cluster_name", cluster_name, "--map_name", hazelcast_map])
            .output();
            if show_debug{
                let path = env::current_dir().unwrap();
                println!("The current directory is {}", path.display());
                println!("Output of runnig the script is: {:?}", output);
            }
            if let Ok(output) = output{
                match core::str::from_utf8(&output.stdout){
                    Ok(output) =>{
                        let response = format!("HTTP/1.1 200 OK\nContent-Length: {}\n\n{}", output.as_bytes().len(), output);
                        stream.write_all(response.as_bytes()).unwrap();
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
