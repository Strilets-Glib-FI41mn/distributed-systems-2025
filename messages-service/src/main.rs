use std::{env, io::{prelude::*, BufReader}, net::{TcpListener, TcpStream}};
use http_reader::HttpReader;

fn main() {
    //let mut port = "7878".to_owned();
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
            "--port" | "-p" | "--message_service_port" => {
                if let Some(port_string) = &value{
                    if let Ok(val) = port_string.parse::<i32>(){
                        if val < 65535{
                            message_service_port = port_string.clone();
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
        println!("using port {message_service_port}");
    }
    let message_adress: String = "127.0.0.1:".to_owned() + &message_service_port;
    let listener = TcpListener::bind(message_adress).unwrap();

    
    for stream in listener.incoming() {
        let stream = stream.unwrap();
        handle_connection(stream, show_debug);
        //handle_connection(stream)
    }
}

fn handle_connection(mut stream: TcpStream, show_debug: bool){
    let mut buf_reader = BufReader::new(&stream);
    let mut line_consumer = HttpReader::new(&mut buf_reader);
    let request = line_consumer.make_request();
    if show_debug {println!("{:?}", request);}


    match *request.method(){
        http::Method::GET => {
            let body = "Not Implemented Yet".to_string();
            let length = body.as_bytes().len();
            
            let response = format!("HTTP/1.1 501 Not Implemented\nContent-Length: {length}\r\n\r\n{body}\r\n");
            
            stream.write_all(response.to_string().as_bytes()).unwrap();
        }
        _ =>{
            
            let response = "HTTP/1.1 501 Not Implemented\r\n\r\n";
            stream.write_all(response.as_bytes()).unwrap();
        }
    }

}
