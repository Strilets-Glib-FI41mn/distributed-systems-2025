use std::{collections::HashMap, io::{prelude::*, BufReader}, net::{TcpListener, TcpStream}, time::Duration};
use http_reader::HttpReader;
use clap::Parser;

use uuid::Uuid;

use rs_consul::{types::*, Config, Consul};
use rand::seq::SliceRandom;

#[derive(Parser,Default,Debug)]
struct Arguments {
    #[arg(short = 'p', long = "port", value_name = "PORT of messages service")]
    pub port : u16,
    #[arg(value_name = "IP of messages service")]
    pub ip: Option<String>,
    #[arg(long, short = 'd', action)]
    pub debug: bool,

    #[arg(long, value_name = "consul adress OPTIONAL")]
    pub consul_address: Option<String>,

    #[clap(long = "non_consume", action=clap::ArgAction::SetFalse, value_name = "Non consume flag, used to tell the Consumer to leave messages be")]
    pub consume: bool,
}

#[tokio::main]
async fn main() {
    let args = Arguments::parse();
    if args.debug{
        println!("{:?}",&args)
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
    let service_name = "messages-service"; //service name
    let node = format!("messages-service:{id}"); //service name


    
    let consul = Consul::new(consul_config);
    let message_service_port = args.port;
    let message_service_ip = args.ip.as_ref().map_or("127.0.0.1", |v| v);
    let message_adress: String = format!("{}:{}", message_service_ip, message_service_port);
    let listener = TcpListener::bind(message_adress).unwrap();


    let payload = RegisterEntityPayload {
        ID: Some(id.to_string()),
        Node: node.to_string(),
        Address: message_service_ip.to_string(), //server address
        Datacenter: None,
        TaggedAddresses: Default::default(),
        NodeMeta: Default::default(),
        Service: Some(RegisterEntityService {
            ID: Some(args.port.to_string()),
            Service: service_name.to_string(),
            Tags: vec![],
            TaggedAddresses: Default::default(),
            Meta: Default::default(),
            Port: Some(message_service_port), 
            Namespace: None,
        }),
        Checks: vec![RegisterEntityCheck{ Node: Some(node), CheckID: Some(id.to_string()), Name: "still_here".to_owned(), 
        Notes: None, Status: Some("passing".to_owned()),
        ServiceID: None, 
        Definition: HashMap::from([
            //("args".to_owned(), "curl localhost".to_owned()),
            ("http".to_owned(), format!("http://{message_service_ip}:{message_service_port}/get/health").to_owned()),
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
        handle_connection(stream, args.debug, &consul, args.consume).await;
    }
}

async fn handle_connection(mut stream: TcpStream, show_debug: bool, consul: &Consul, consume: bool){
    let mut buf_reader = BufReader::new(&stream);
    let mut line_consumer = HttpReader::new(&mut buf_reader);
    let request = line_consumer.make_request();
    if show_debug {
        println!("{:?}", request);
        //println!("{:#?}", consul.get_all_registered_service_names(None));
    }
    if request.method() == &http::Method::GET && request.uri().path().split("/").collect::<Vec<_>>().get(2) == Some(&"health"){
        stream.write_all("HTTP/1.1 200 OK\r\n\r\n".to_owned().as_bytes()).unwrap();
        return;
    }


    match *request.method(){
        http::Method::GET => {
            let mut response = "HTTP/1.1 501 Not Implemented\r\n\r\n".to_string();
            let mut messages_found = vec![];
            let consume_from_r =  consul.read_key(ReadKeyRequest{
                key: "kafka_address",
                namespace: "",
                datacenter: "",
                recurse: true,
                separator: "",
                consistency: ConsistencyMode::Default,
                index: None,
                wait: Duration::from_secs(5),
            }).await;
        if show_debug {
            println!("consume from adress(es) found: {:?}", consume_from_r);
        }
        let mut consume_from = 
        match consume_from_r{
            Ok(consume_from) => {
                consume_from.response.iter().map(|response| response.value.clone()).filter(|val| val.is_some()).map(|val| val.unwrap()).collect()
            },
            Err(_) => vec![],
        };
        if show_debug{
            println!("Consume from: {:?}", consume_from);
        }

        
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
        if show_debug {
            println!("kafka topic(s) found: {:?}", kafka_topic_r);
        }
        let kafka_topic = 
        match kafka_topic_r{
            Ok(kafka_topic) => {
                let vector:Vec<String> = kafka_topic.response.iter().map(|response| response.value.clone()).filter(|val| val.is_some()).map(|val| val.unwrap()).collect();
                vector.get(0).unwrap_or(&"".to_owned()).to_owned()
            },
            Err(_) => "".to_owned(),
        };

            consume_from.shuffle(&mut rand::rng());
            match consume_from.get(0){
                Some(address) => {

                //let mut consume_from = consume_from.clone();
                use kafka::consumer::{Consumer, FetchOffset, GroupOffsetStorage};
                match Consumer::from_hosts(vec!(address.to_string()))
                    .with_topic(kafka_topic.to_owned())
                    .with_fallback_offset(FetchOffset::Earliest)
                    .with_group("messages-service".to_owned())
                    .with_offset_storage(Some(GroupOffsetStorage::Kafka))
                    .create(){
                        Ok(mut consumer) => {

                            if let Some(mss) = consumer.poll().iter().next() {
                                if let Some(ms) = mss.iter().next() {
                                    let partition = ms.partition();
                                    //for message in 
                                    if let Some(message) = ms.messages().get(1){
                                        let key = match String::from_utf8(message.key.to_vec()){
                                            Ok(key) => key,
                                            Err(err) => format!("Instead of key found: {}", err.to_string()),
                                        };
    
                                        let value = match String::from_utf8(message.value.to_vec()){
                                            Ok(value) => value,
                                            Err(err) => format!("Instead of value found: {}", err.to_string()),
                                        };
                                        let res_message = format!("{}: {}", key, value);
                                        if show_debug{
                                            println!("{res_message}");
                                        }
                                        messages_found.push(res_message);
                                        if consume{
                                            consumer.consume_message(&kafka_topic, partition, message.offset).unwrap();
                                            consumer.commit_consumed().unwrap();
                                        }
                                    }
                                }
                            }
                        if show_debug{
                            println!("Found messages: {:#?}", &messages_found);
                        }
                        if messages_found.len() > 0{
                            let messages_sent = serde_json::to_string(&messages_found);
                            let string_used = match messages_sent{
                                Ok(good) => good,
                                Err(er) => er.to_string(),
                            };
                            response = format!("HTTP/1.1 200 OK\nContent-Length: {}\n\n{}", string_used.as_bytes().len(), string_used);
                        }
                        },
                        Err(err) => {
                            let err = err.to_string();
                            if show_debug{
                                println!("Error when trying to create consumer{:#?}", &err);
                            }
                            response = format!("HTTP/1.1 404 NOT FOUND\nContent-Length: {}\n\n{}", err.as_bytes().len(), err);
                        },
                    }

                    //.unwrap();
                },
                None => {
                    let not_found_kafka = "Not found kafka adresses";
                    response = format!("HTTP/1.1 404 NOT FOUND\nContent-Length: {}\n\n{}", not_found_kafka.as_bytes().len(), not_found_kafka);
                },
            }
            if consume_from.len() > 0{
            }

            
            stream.write_all(response.to_string().as_bytes());//.unwrap();
        }
        _ =>{
            
            let response = "HTTP/1.1 501 Not Implemented\r\n\r\n";
            stream.write_all(response.as_bytes()).unwrap();
        }
    }

}
