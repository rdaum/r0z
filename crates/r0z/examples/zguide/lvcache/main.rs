#![crate_name = "lvcache"]

use std::collections::HashMap;
use std::str::from_utf8;

fn main() {
    let context = r0z::Context::new();
    let frontend = context.socket(r0z::SUB).unwrap();
    frontend
        .connect("tcp://localhost:5557")
        .expect("could not connect to frontend");
    let backend = context.socket(r0z::XPUB).unwrap();
    backend
        .bind("tcp://*:5558")
        .expect("could not bind backend socket");

    //  Subscribe to every single topic from publisher
    frontend.set_subscribe(b"").unwrap();

    let mut cache = HashMap::new();

    loop {
        let mut items = [
            frontend.as_poll_item(r0z::POLLIN),
            backend.as_poll_item(r0z::POLLIN),
        ];
        if r0z::poll(&mut items, 1000).is_err() {
            break; //  Interrupted
        }
        if items[0].is_readable() {
            let topic = frontend.recv_msg(0).unwrap();
            let current = frontend.recv_msg(0).unwrap();
            cache.insert(topic.to_vec(), current.to_vec());
            backend.send(topic, r0z::SNDMORE).unwrap();
            backend.send(current, 0).unwrap();
        }
        if items[1].is_readable() {
            // Event is one byte 0=unsub or 1=sub, followed by topic
            let event = backend.recv_msg(0).unwrap();
            if event[0] == 1 {
                let topic = &event[1..];
                println!("Sending cached topic {}", from_utf8(topic).unwrap());
                if let Some(previous) = cache.get(topic) {
                    backend.send(topic, r0z::SNDMORE).unwrap();
                    backend.send(previous, 0).unwrap();
                }
            }
        }
    }
}
