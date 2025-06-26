use std::{collections::HashMap, env, io::{prelude::*, BufReader}, net::{TcpListener, TcpStream}, str::FromStr, time::Duration};

use http_reader::HttpReader;

use rs_consul::{types::*, Config, Consul};
use uuid::Uuid;
use hazelcast_rest::HazelcastRestClient;
use std::process::Command;

use rand::seq::SliceRandom;
use clap::Parser;


#[derive(Parser,Default,Debug)]
struct Arguments {
    #[arg(short = 'p', long = "port", value_name = "PORT of logging service")]
    pub port : u16,
    #[arg(value_name = "IP of logging service")]
    pub ip: Option<String>,
    #[arg(long, short = 'd', action)]
    pub debug: bool,
    
    #[arg(long, value_name = "consul adress OPTIONAL")]
    pub consul_address: Option<String>,
    #[arg(long)]
    pub hazelcast_number: Option<u16>
}

#[tokio::main]
async fn main() {
    let args = Arguments::parse();

    let consul_config = {
        match args.consul_address{
            Some(address) =>{
                Config {
                    address, 
                    token: None, // No token required in development mode
                    ..Default::default() // Uses default values for other settings
                }
            }
            None =>{
                Config::from_env()
            }
        }
    };
    let id = Uuid::new_v4(); //node name
    let service_name = "logging-service"; //service name
    let node = "logging-service"; //service name

    let consul = Consul::new(consul_config);
    let logging_service_port = args.port;
    let logging_service_ip = args.ip.as_ref().map_or("127.0.0.1", |v| v);
    let logging_adress: String = format!("{}:{}", logging_service_ip, logging_service_port);
    let listener = TcpListener::bind(logging_adress).unwrap();


    let payload = RegisterEntityPayload {
        ID: Some(id.to_string()),
        Node: node.to_string(),
        Address: logging_service_ip.to_owned(), //server address
        Datacenter: None,
        TaggedAddresses: Default::default(),
        NodeMeta: Default::default(),
        Service: Some(RegisterEntityService {
            ID: Some(args.port.to_string()),
            Service: service_name.to_string(),
            Tags: vec![],
            TaggedAddresses: Default::default(),
            Meta: Default::default(),
            Port: Some(logging_service_port), 
            Namespace: None,
        }),
        Checks: vec![RegisterEntityCheck{ Node: Some(node.to_string()), CheckID: Some(id.to_string()), Name: "still_here".to_owned(), 
        Notes: None, Status: Some("passing".to_owned()),
        ServiceID: None, 
        
        Definition: HashMap::from([
            //("args".to_owned(), "curl localhost".to_owned()),
            ("http".to_owned(), format!("http://{logging_service_ip}:{logging_service_port}/get/health").to_owned()),
            ("name".to_owned(), "/health".to_owned()),
            ("interval".to_owned(), "10s".to_owned()),
            ("timeout".to_owned(), "4s".to_owned()),
            ("method".to_owned(), "GET".to_owned()),
        ])
        }],
        SkipNodeUpdate: None,
    };

    consul.register_entity(&payload).await.expect("messages service relies on consul agent registration");

    
    for stream in listener.incoming() {
        let stream = stream.unwrap();
        handle_connection(stream, args.debug, &consul, args.hazelcast_number).await;
    }
}

async fn handle_connection(mut stream: TcpStream, show_debug: bool, consul: &Consul, hazelcast_number: Option<u16>){
    let mut buf_reader = BufReader::new(&stream);
    let mut line_consumer = HttpReader::new(&mut buf_reader);
    let request = line_consumer.make_request();
    if show_debug {println!("{:?}", request);}
    if request.method() == &http::Method::GET && request.uri().path().split("/").collect::<Vec<_>>().get(2) == Some(&"health"){
        stream.write_all("HTTP/1.1 200 OK\r\n\r\n".to_owned().as_bytes()).unwrap();
        return;
    }

    let (hazelcast_map, hazelcast_ip, hazelcast_port) =
    match *request.method() {

    http::Method::POST | http::Method::GET =>{

            let hazelcast_adress =  consul.read_key(ReadKeyRequest{
                key: "hazelcast_adress",
                namespace: "",
                datacenter: "",
                recurse: true,
                separator: "",
                consistency: ConsistencyMode::Default,
                index: None,
                wait: Duration::from_secs(5),
            }).await;
            if show_debug {
                println!("hazelcast adress(es) found: {:?}", hazelcast_adress);
            }
            let hazelcast_adress = 
            match hazelcast_adress{
                Ok(hazelcast_adress) => {
                    let mut vector:Vec<(String, String)> = hazelcast_adress.response.iter().map(|response| (response.value.clone(), response.key.clone())).filter(|val| val.0.is_some()).map(|val| (val.0.unwrap(), val.1)).collect();
                    if let Some(hazelcast_number) = hazelcast_number{
                        let found: Vec<_> = vector.iter().filter(|(val, key)| key == &format!("hazelcast_adress_{hazelcast_number}")).collect();
                        if let Some(found) = found.get(0){
                            found.0.to_owned()
                        }else{    
                            vector.shuffle(&mut rand::rng());
                            match vector.get(0){
                                Some(value) => value.0.to_owned(),
                                None => "".to_owned(),
                            }   
                        }
                    } else{ 
                        vector.shuffle(&mut rand::rng());
                        match vector.get(0){
                            Some(value) => value.0.to_owned(),
                            None => "".to_owned(),
                        }   
                    }
                },
                Err(_) => "".to_owned(),
            };

            let (hazelcast_ip, hazelcast_port) ={
                let split:Vec<String> = hazelcast_adress.split(":").map(|str_| str_.to_owned()).collect();
                (split.get(0).map(|val| val.to_owned()), split.get(1).map(|val| val.to_owned()))
            };

            
            let hazelcast_map =  consul.read_key(ReadKeyRequest{
                key: "hazelcast_map",
                namespace: "",
                datacenter: "",
                recurse: true,
                separator: "",
                consistency: ConsistencyMode::Default,
                index: None,
                wait: Duration::from_secs(5),
            }).await;
            if show_debug {
                println!("hazelcast map(s) found: {:?}", hazelcast_map);
            }
            let hazelcast_map = 
            match hazelcast_map{
                Ok(hazelcast_map) => {
                    let vector:Vec<String> = hazelcast_map.response.iter().map(|response| response.value.clone()).filter(|val| val.is_some()).map(|val| val.unwrap()).collect();
                    vector.get(0).map(|val| val.to_owned())
                },
                Err(_) => None,
            };
            (hazelcast_map, hazelcast_ip, hazelcast_port)
    }
    _ => {(None, None, None)}
    };

    match *request.method(){
        http::Method::POST => {
            if let (Some(body), Some(hazelcast_map), Some(hazelcast_ip), Some(hazelcast_port)) = (request.body(), hazelcast_map, hazelcast_ip, hazelcast_port){
            
                let values: Vec<&str> = body.split(": ").take(2).collect();
                if values.len() == 2{
                    if Uuid::from_str(values[0]).is_ok(){
                        
                        let client = HazelcastRestClient::new(&hazelcast_ip, hazelcast_port);

                        let res = client.map_put(&hazelcast_map,&Into::<String>::into(values[0]), &vec![("Content-Type", "plain/text"), ("factoryId", "-1")], values[1]);
                        if show_debug {
                            println!("{res:?}");
                        }

                        match res{
                            Ok(response) => {
                                stream.write_all((format!("HTTP/1.1 200 OK\nContent-Length: {}\n\n{}",response.as_bytes().len(), response)).as_bytes()).unwrap();
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
            if let (Some(hazelcast_map), Some(hazelcast_ip), Some(hazelcast_port)) = (hazelcast_map, hazelcast_ip, hazelcast_port){
                
                let path = format!("{}:{}",&hazelcast_ip,  &hazelcast_port);


                let cluster_name =  consul.read_key(ReadKeyRequest{
                    key: "hazelcast_cluster_name",
                    namespace: "",
                    datacenter: "",
                    recurse: true,
                    separator: "",
                    consistency: ConsistencyMode::Default,
                    index: None,
                    wait: Duration::from_secs(5),
                }).await;
                if show_debug {
                    println!("hazelcast cluster name(s) found: {:?}", hazelcast_map);
                }
                let cluster_name = 
                match cluster_name{
                    Ok(cluster_name) => {
                        let vector:Vec<String> = cluster_name.response.iter().map(|response| response.value.clone()).filter(|val| val.is_some()).map(|val| val.unwrap()).collect();
                        vector.get(0).map(|val| val.to_owned())
                    },
                    Err(_) => None,
                };

                
                let output = Command::new("zsh")

                .args(["run_get_data.sh", "--ip", &path, "--cluster_name", &cluster_name.map_or("".to_owned(), |v| v), "--map_name", &hazelcast_map])
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
        }
        _ =>{
            
            let response = "HTTP/1.1 501 Not Implemented\r\n\r\n";
            stream.write_all(response.as_bytes()).unwrap();
        }
    }

}
