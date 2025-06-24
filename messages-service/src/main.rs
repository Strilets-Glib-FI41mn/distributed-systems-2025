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
        handle_connection(stream, args.debug);
    }
}

fn handle_connection(mut stream: TcpStream, show_debug: bool){
    let mut buf_reader = BufReader::new(&stream);
    let mut line_consumer = HttpReader::new(&mut buf_reader);
    let request = line_consumer.make_request();
    if show_debug {println!("{:?}", request);}


    match *request.method(){
        http::Method::GET => {
            let response = "HTTP/1.1 501 Not Implemented\r\n\r\n".to_string();
            
            stream.write_all(response.to_string().as_bytes()).unwrap();
        }
        _ =>{
            
            let response = "HTTP/1.1 501 Not Implemented\r\n\r\n";
            stream.write_all(response.as_bytes()).unwrap();
        }
    }

}
