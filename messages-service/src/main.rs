use std::{io::{prelude::*, BufReader}, net::{TcpListener, TcpStream}};
use http_reader::HttpReader;
use clap::Parser;
#[derive(Parser,Default,Debug)]
struct Arguments {
    #[arg(short = 'p', long = "port", value_name = "PORT of messages service")]
    pub port : i32,
    #[arg(value_name = "IP of messages service")]
    pub ip: Option<String>,
    #[arg(long, short = 'd', action)]
    pub debug: bool,
    #[arg(long, value_name = "kafka adress")]
    pub consume_from: String,
    #[arg(long, value_name = "kafka topic used as queue")]
    pub kafka_topic: String,
    #[clap(long = "non_consume", action=clap::ArgAction::SetFalse, value_name = "Non consume flag, used to tell the Consumer to leave messages be")]
    pub consume: bool,
}

fn main() {
    let args = Arguments::parse();
    let message_service_port = args.port;
    let message_service_ip = args.ip.as_ref().map_or("127.0.0.1", |v| v);
    if args.debug{
        println!("{:?}",&args)
    }
    let message_adress: String = format!("{}:{}", message_service_ip, message_service_port);
    let listener = TcpListener::bind(message_adress).unwrap();

    
    for stream in listener.incoming() {
        let stream = stream.unwrap();
        handle_connection(stream, args.debug, &args.consume_from, &args.kafka_topic, args.consume);
    }
}

fn handle_connection(mut stream: TcpStream, show_debug: bool, consume_from: &str, kafka_topic: &str, consume: bool){
    let mut buf_reader = BufReader::new(&stream);
    let mut line_consumer = HttpReader::new(&mut buf_reader);
    let request = line_consumer.make_request();
    if show_debug {println!("{:?}", request);}


    match *request.method(){
        http::Method::GET => {
            let mut response = "HTTP/1.1 501 Not Implemented\r\n\r\n".to_string();
            let mut messages_found = vec![];
            {
                use kafka::consumer::{Consumer, FetchOffset, GroupOffsetStorage};
                match Consumer::from_hosts(vec!(consume_from.to_owned()))
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
                                            consumer.consume_message(kafka_topic, partition, message.offset).unwrap();
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
            }

            
            stream.write_all(response.to_string().as_bytes());//.unwrap();
        }
        _ =>{
            
            let response = "HTTP/1.1 501 Not Implemented\r\n\r\n";
            stream.write_all(response.as_bytes()).unwrap();
        }
    }

}
