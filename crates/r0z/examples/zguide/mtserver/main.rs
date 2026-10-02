#![crate_name = "mtserver"]

use std::thread;
use std::time::Duration;

fn worker_routine(context: &r0z::Context) {
    let receiver = context.socket(r0z::REP).unwrap();
    receiver
        .connect("inproc://workers")
        .expect("failed to connect worker");
    loop {
        receiver
            .recv_string(0)
            .expect("worker failed receiving")
            .unwrap();
        thread::sleep(Duration::from_millis(1000));
        receiver.send("World", 0).unwrap();
    }
}

fn main() {
    let context = r0z::Context::new();
    let clients = context.socket(r0z::ROUTER).unwrap();
    let workers = context.socket(r0z::DEALER).unwrap();

    clients
        .bind("tcp://*:5555")
        .expect("failed to bind client router");
    workers
        .bind("inproc://workers")
        .expect("failed to bind worker dealer");

    for _ in 0..5 {
        let ctx = context.clone();
        thread::spawn(move || worker_routine(&ctx));
    }
    r0z::proxy(&clients, &workers).expect("failed proxying");
}
