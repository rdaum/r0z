//  Reading from multiple sockets
//  This version uses r0rz::poll()

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
    let mut msg = r0rz::Message::new();
    loop {
        let mut items = [
            receiver.as_poll_item(r0rz::POLLIN),
            subscriber.as_poll_item(r0rz::POLLIN),
        ];
        r0rz::poll(&mut items, -1).unwrap();
        if items[0].is_readable() && receiver.recv(&mut msg, 0).is_ok() {
            //  Process task
        }
        if items[1].is_readable() && subscriber.recv(&mut msg, 0).is_ok() {
            // Process weather update
        }
    }
}
