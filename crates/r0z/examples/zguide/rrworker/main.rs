#![crate_name = "rrworker"]

use std::thread;
use std::time::Duration;

fn main() {
    let context = r0z::Context::new();
    let responder = context.socket(r0z::REP).unwrap();
    responder
        .connect("tcp://localhost:5560")
        .expect("failed connecting responder");

    loop {
        let string = responder.recv_string(0).unwrap().unwrap();
        println!("Received request:{}", string);
        thread::sleep(Duration::from_millis(1000));
        responder.send("World", 0).unwrap();
    }
}
