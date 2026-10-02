#![crate_name = "helloworld_server"]

//! Hello World server in Rust
//! Binds REP socket to tcp://*:5555
//! Expects "Hello" from client, replies with "World"

use std::thread;
use std::time::Duration;

fn main() {
    let context = r0rz::Context::new();
    let responder = context.socket(r0rz::REP).unwrap();

    assert!(responder.bind("tcp://*:5555").is_ok());

    let mut msg = r0rz::Message::new();
    loop {
        responder.recv(&mut msg, 0).unwrap();
        println!("Received {}", msg.as_str().unwrap());
        thread::sleep(Duration::from_millis(1000));
        responder.send("World", 0).unwrap();
    }
}
