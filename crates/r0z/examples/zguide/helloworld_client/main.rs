#![crate_name = "helloworld_client"]

//! Hello World client

fn main() {
    println!("Connecting to hello world server...\n");

    let context = r0z::Context::new();
    let requester = context.socket(r0z::REQ).unwrap();

    assert!(requester.connect("tcp://localhost:5555").is_ok());

    let mut msg = r0z::Message::new();

    for request_nbr in 0..10 {
        println!("Sending Hello {}...", request_nbr);
        requester.send("Hello", 0).unwrap();

        requester.recv(&mut msg, 0).unwrap();
        println!("Received World {}: {}", msg.as_str().unwrap(), request_nbr);
    }
}
