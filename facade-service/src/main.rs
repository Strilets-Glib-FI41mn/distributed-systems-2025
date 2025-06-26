use std::{
    collections::HashMap, io::{prelude::*, BufReader}, net::{TcpListener, TcpStream}, time::Duration
};

use http_reader::HttpReader;
//use reqwest::blocking::{Request, RequestBuilder};
use reqwest::blocking::Client;
use uuid::Uuid;

use clap::Parser;
use rs_consul::{types::*, Config, Consul};

#[derive(Parser, Default,Debug)]
struct Arguments {
    #[arg(short = 'p', long)]
    pub port : Option<u16>,
    
    #[arg(value_name = "IP of facade service")]
    pub ip: Option<String>,
    
    #[arg(long, short = 'd', action)]
    pub debug: bool,

    #[arg(long, value_name = "consul adress OPTIONAL")]
    pub consul_address: Option<String>,
    
}
#[tokio::main]
async fn main() {
    let args = Arguments::parse();
    let port = args.port.unwrap_or(7878);
    if args.debug{
        println!("using port {port}")
    }

    
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
    let service_name = "facade-service"; //service name
    let node = format!("facade-service:{id}"); //service name


    
    let consul = Consul::new(consul_config);
    let facade_service_port = args.port.unwrap_or(8362);
    let facade_service_ip = args.ip.as_ref().map_or("127.0.0.1", |v| v);

    let facade_address: String = format!("127.0.0.1:{}", &port);
    let listener = TcpListener::bind(facade_address).unwrap();


    //println!("http://{facade_service_ip}:{facade_service_port}/get/health");
    let payload = RegisterEntityPayload {
        ID: Some(id.to_string()),
        Node: node.to_string(),
        Address: facade_service_ip.to_owned(), //server address
        Datacenter: None,
        TaggedAddresses: Default::default(),
        NodeMeta: Default::default(),
        Service: Some(RegisterEntityService {
            ID: Some(facade_service_port.to_string()),
            Service: service_name.to_string(),
            Tags: vec![],
            TaggedAddresses: Default::default(),
            Meta: Default::default(),
            Port: Some(facade_service_port), 
            Namespace: None,
        }),
        Checks: vec![RegisterEntityCheck{ Node: Some(node), CheckID: Some(id.to_string()), Name: "still_here".to_owned(), 
        Notes: None,
        // Status: None,
        Status: Some("passing".to_owned()),
        ServiceID: None,
        Definition: HashMap::from([
            //("args".to_owned(), "curl localhost".to_owned()),
            ("http".to_owned(), format!("http://{facade_service_ip}:{facade_service_port}/get/health").to_owned()),
            ("name".to_owned(), "/health".to_owned()),
            ("interval".to_owned(), "10s".to_owned()),
            ("timeout".to_owned(), "4s".to_owned()),
            ("method".to_owned(), "GET".to_owned()), // Specify the HTTP method
            //,
        ])
        }],
        SkipNodeUpdate: None,
    };

    consul.register_entity(&payload).await.expect("messages service relies on consul agent registration");

    
    for stream in listener.incoming() {
        let stream = stream.unwrap();
        handle_connection(stream,  args.debug,&consul).await//&args.server_config, &args.kafka_topic);
    }
}

async fn handle_connection(mut stream: TcpStream, debug: bool, consul: &Consul){
    let mut buf_reader = BufReader::new(&stream);
    let mut line_consumer = HttpReader::new(&mut buf_reader);
    let request = line_consumer.make_request();
    if debug{println!("Request aquired:\n{:?}", request);}
    if request.method() == &http::Method::GET && request.uri().path().split("/").collect::<Vec<_>>().get(2) == Some(&"health"){
        //stream.write_all("HTTP/1.1 200 OK\r\n\r\n".to_owned().as_bytes()).unwrap();
        stream.write_all("HTTP/1.1 429\r\n\r\n".to_owned().as_bytes()).unwrap();
        return;
    }
    
    let mut response = "HTTP/1.1 401 Not Implemented\r\n\r\n".to_owned();

    let http_client = Client::new();
    let kafka_topic = 
    match request.method(){
        &http::Method::POST | &http::Method::GET =>{
            let kafka_topic_r =  consul.read_key(ReadKeyRequest{
                key: "kafka_topic",
                namespace: "",
                datacenter: "",
                recurse: true,
                separator: "",
                consistency: ConsistencyMode::Default,
                index: None,
                wait: Duration::from_secs(5),
            }).await;
            if debug {
                println!("kafka topic(s) found: {:?}", kafka_topic_r);
            }
            
            match kafka_topic_r{
                Ok(kafka_topic) => {
                    let vector:Vec<String> = kafka_topic.response.iter().map(|response| response.value.clone()).filter(|val| val.is_some()).map(|val| val.unwrap()).collect();
                    vector.get(0).map_or("", |v| v).to_owned()
                },
                Err(_) => "".to_owned()
            }
        }
        _ =>{
            stream.write_all(response.as_bytes()).unwrap();
            return;
        }
    };

    let logging_adresses: Vec<_> = {
        consul.get_service_nodes(GetServiceNodesRequest{
            service: "logging-service",
            near: None,
            passing: true,
            filter: None,
        },
            None
        ).await.iter().map(|response| response.response.clone())
        .fold(vec![], |mut acc:Vec<ServiceNode>, mut xs| {acc.append(&mut xs); return acc})
        .iter().map(|a| format!("{}:{}", a.node.address, a.service.port)).collect()
    };


    let message_adresses: Vec<_> = {
        consul.get_service_nodes(GetServiceNodesRequest{
            service: "messages-service",
            near: None,
            passing: true,
            filter: None,
        },
            None
        ).await.iter().map(|response| response.response.clone())
        .fold(vec![], |mut acc:Vec<ServiceNode>, mut xs| {acc.append(&mut xs); return acc})
        .iter().map(|a| format!("{}:{}", a.node.address, a.service.port)).collect()
    };

    let produce_targets =  consul.read_key(ReadKeyRequest{
        key: "kafka_address",
        namespace: "",
        datacenter: "",
        recurse: true,
        separator: "",
        consistency: ConsistencyMode::Default,
        index: None,
        wait: Duration::from_secs(5),
            }).await;
        if debug {
            println!("produce targets adress(es) found: {:?}", produce_targets);
        }
        let produce_targets = 
        match produce_targets{
            Ok(produce_targets) => {
                produce_targets.response.iter().map(|response| response.value.clone()).filter(|val| val.is_some()).map(|val| val.unwrap()).collect()
            },
            Err(_) => vec![],
        };
    
    //let logging_adresses = serde_json::from_str(&logging_adresses.unwrap_or("".to_owned())).unwrap_or(Vec::<String>::new());

    if debug{
        println!("Logging adresses:\n{:#?}", &logging_adresses);
        println!("Message adresses:\n{:#?}", &message_adresses);
        println!("Kafka adresses:\n{:#?}", &produce_targets);
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
                    {
                        //use std::fmt::Write;
                        use std::time::Duration;
                        use kafka::producer::{Producer, Record, RequiredAcks};
                        
                        for produce_to in produce_targets{
                            match Producer::from_hosts(vec!(produce_to.to_string()))
                                .with_ack_timeout(Duration::from_secs(1))
                                .with_required_acks(RequiredAcks::One)
                                .create(){
                                    Ok(mut producer) => {
                                        let res = producer.send(&Record::from_key_value(&kafka_topic, id.to_string(), body.clone()));
                                        
                                        //.send(&Record::from_value(kafka_topic, format!("{id}: {body}").as_bytes()));//.unwrap();
                                        match res{
                                            Ok(_) => {
                                                break},
                                            Err(err) => {
                                                if debug { println!("Failed to create a send a value: {:?}", err);}
                                            },
                                        }
                                    },
                                    Err(err) => {
                                        if debug{
                                            println!("Failed to create a producer {:?}", err);
                                        }
                                    },
                                }
                                //.unwrap();
                        }
                    }                    
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
            
            let mut http_result_message = None ;

            if debug{
                println!("{:#?}", &message_adresses);
            }
            for message_adress in message_adresses{
                
                http_result_message = Some(http_client
                .get(format!("http://{}/get", message_adress))
                .send());
            if debug{println!("Result of sending get request to message at {}\n{:?}", &message_adress, &http_result_logging);}
            if let Some(res) =  &http_result_message{
                if let Ok(res) = res{
                    if debug{
                        println!("Response of the message {}", &res.status());
                    }
                    if res.status().is_success(){
                        println!("Is success");
                        break
                    }
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
                        let both_text = format!("logging: {}\n message: {}\n",  &text_1, &text_2);
                        Some(format!("HTTP/1.1 200 OK\nContent-Length: {}\n\n{}", both_text.as_bytes().len(), both_text))
                    }else{
                        None
                    }
                },
                (Ok(text_1), Err(message_error)) => {
                    println!("Message error: {}", message_error);
                    let both_text = format!("logging: {:#?}\n message: {}\n",  &text_1, &message_error);
                    Some(format!("HTTP/1.1 200 OK\nContent-Length: {}\n\n{}", both_text.as_bytes().len(), both_text))                
                },
                (Err(logging_error), Ok(test_2)) => {
                    println!("Logging error: {}", logging_error);
                    let both_text = format!("logging: {}\n message: {:#?}\n",  &logging_error, &test_2);
                    Some(format!("HTTP/1.1 200 OK\nContent-Length: {}\n\n{}", both_text.as_bytes().len(), both_text))
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