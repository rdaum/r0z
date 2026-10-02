#![crate_name = "msreader"]

//! Reading from multiple sockets
//! This version uses a simple recv loop

use std::thread;
use std::time::Duration;

fn main() {
    let context = r0rz::Context::new();

    // Connect to task ventilator
    let receiver = context.socket(r0rz::PULL).unwrap();
    assert!(receiver.connect("tcp://localhost:5557").is_ok());

    // Connect to weather server
    let subscriber = context.socket(r0rz::SUB).unwrap();
    assert!(subscriber.connect("tcp://localhost:5556").is_ok());
    let filter = b"10001";
    assert!(subscriber.set_subscribe(filter).is_ok());

    // Process messages from both sockets
    // We prioritize traffic from the task ventilator
    let mut msg = r0rz::Message::new();
    loop {
        loop {
            if receiver.recv(&mut msg, r0rz::DONTWAIT).is_err() {
                break;
            }
            // Process task
        }

        loop {
            if subscriber.recv(&mut msg, r0rz::DONTWAIT).is_err() {
                break;
            }
            // Process weather update
        }

        // No activity, so sleep for 1 msec
        thread::sleep(Duration::from_millis(1))
    }
}
