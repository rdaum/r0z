//  Asynchronous client-to-server (DEALER to ROUTER)
//
//  While this example runs in a single process, that is to make
//  it easier to start and stop the example. Each task has its own
//  context and conceptually acts as a separate process.
#![crate_name = "asyncsrv"]

use rand::{rng, RngExt};
use std::time::Duration;
use std::{str, thread};

fn client_task() {
    let context = r0rz::Context::new();
    let client = context.socket(r0rz::DEALER).unwrap();
    let mut rng = rng();
    let identity = format!("{:04X}-{:04X}", rng.random::<u16>(), rng.random::<u16>());
    client
        .set_identity(identity.as_bytes())
        .expect("failed setting client id");
    client
        .connect("tcp://localhost:5570")
        .expect("failed connecting client");
    let mut request_nbr = 0;
    loop {
        for _ in 0..100 {
            if client
                .poll(r0rz::POLLIN, 10)
                .expect("client failed polling")
                > 0
            {
                let msg = client
                    .recv_multipart(0)
                    .expect("client failed receivng response");
                println!("{}", str::from_utf8(&msg[msg.len() - 1]).unwrap());
            }
        }
        request_nbr += 1;
        let request = format!("request #{}", request_nbr);
        client
            .send(&request, 0)
            .expect("client failed sending request");
    }
}

fn server_task() {
    let context = r0rz::Context::new();
    let frontend = context.socket(r0rz::ROUTER).unwrap();
    frontend
        .bind("tcp://*:5570")
        .expect("server failed binding frontend");
    let backend = context.socket(r0rz::DEALER).unwrap();
    backend
        .bind("inproc://backend")
        .expect("server failed binding backend");
    for _ in 0..5 {
        let ctx = context.clone();
        thread::spawn(move || server_worker(&ctx));
    }
    r0rz::proxy(&frontend, &backend).expect("server failed proxying");
}

fn server_worker(context: &r0rz::Context) {
    let worker = context.socket(r0rz::DEALER).unwrap();
    worker
        .connect("inproc://backend")
        .expect("worker failed to connect to backend");
    let mut rng = rng();

    loop {
        let identity = worker
            .recv_string(0)
            .expect("worker failed receiving identity")
            .unwrap();
        let message = worker
            .recv_string(0)
            .expect("worker failed receiving message")
            .unwrap();
        let replies = rng.random_range(0..4);
        for _ in 0..replies {
            thread::sleep(Duration::from_millis(rng.random_range(0..1000) + 1));
            worker
                .send(&identity, r0rz::SNDMORE)
                .expect("worker failed sending identity");
            worker
                .send(&message, 0)
                .expect("worker failed sending message");
        }
    }
}

fn main() {
    thread::spawn(client_task);
    thread::spawn(client_task);
    thread::spawn(client_task);
    thread::spawn(server_task);
    thread::sleep(Duration::from_secs(5));
}
