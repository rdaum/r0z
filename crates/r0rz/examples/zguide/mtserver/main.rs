#![crate_name = "mtserver"]

use std::thread;
use std::time::Duration;

fn worker_routine(context: &r0rz::Context) {
    let receiver = context.socket(r0rz::REP).unwrap();
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
    let context = r0rz::Context::new();
    let clients = context.socket(r0rz::ROUTER).unwrap();
    let workers = context.socket(r0rz::DEALER).unwrap();

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
    r0rz::proxy(&clients, &workers).expect("failed proxying");
}
