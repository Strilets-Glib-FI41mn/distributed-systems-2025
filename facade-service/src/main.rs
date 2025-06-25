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
    pub debug: bool,
    //#[arg(long, value_name = "kafka adress")]
    //pub produce_to: String,
    #[arg(long, value_name = "kafka topic used as queue")]
    pub kafka_topic: String,
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
        handle_connection(stream, &args.server_config,  args.debug, &args.kafka_topic);
    }
}

fn handle_connection(mut stream: TcpStream, server_config: &str, debug:bool, kafka_topic: &str){
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
                    {
                        //use std::fmt::Write;
                        use std::time::Duration;
                        use kafka::producer::{Producer, Record, RequiredAcks};
                        let produce_targets: Vec<String> = serde_json::from_str(&get_data_from_config(server_config, "queue", debug).unwrap_or_default()).unwrap_or(vec![]);
                        for produce_to in produce_targets{
                            match Producer::from_hosts(vec!(produce_to.to_string()))
                                .with_ack_timeout(Duration::from_secs(1))
                                .with_required_acks(RequiredAcks::One)
                                .create(){
                                    Ok(mut producer) => {
                                        let res = producer.send(&Record::from_key_value(kafka_topic, id.to_string(), body.clone()));
                                        
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