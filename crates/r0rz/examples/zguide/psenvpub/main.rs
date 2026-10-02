#![crate_name = "psenvpub"]

use std::thread;
use std::time::Duration;

fn main() {
    //prepare context and publisher
    let context = r0rz::Context::new();
    let publisher = context.socket(r0rz::PUB).unwrap();
    publisher
        .bind("tcp://*:5563")
        .expect("failed binding publisher");

    loop {
        publisher
            .send("A", r0rz::SNDMORE)
            .expect("failed sending first envelope");
        publisher
            .send("We don't want to see this", 0)
            .expect("failed sending first message");
        publisher
            .send("B", r0rz::SNDMORE)
            .expect("failed sending second envelope");
        publisher
            .send("We would like to see this", 0)
            .expect("failed sending second message");
        thread::sleep(Duration::from_millis(1));
    }
}
