//  Reading from multiple sockets
//  This version uses r0z::poll()

fn main() {
    let context = r0z::Context::new();

    // Connect to task ventilator
    let receiver = context.socket(r0z::PULL).unwrap();
    assert!(receiver.connect("tcp://localhost:5557").is_ok());

    // Connect to weather server
    let subscriber = context.socket(r0z::SUB).unwrap();
    assert!(subscriber.connect("tcp://localhost:5556").is_ok());
    let filter = b"10001";
    assert!(subscriber.set_subscribe(filter).is_ok());

    // Process messages from both sockets
    let mut msg = r0z::Message::new();
    loop {
        let mut items = [
            receiver.as_poll_item(r0z::POLLIN),
            subscriber.as_poll_item(r0z::POLLIN),
        ];
        r0z::poll(&mut items, -1).unwrap();
        if items[0].is_readable() && receiver.recv(&mut msg, 0).is_ok() {
            //  Process task
        }
        if items[1].is_readable() && subscriber.recv(&mut msg, 0).is_ok() {
            // Process weather update
        }
    }
}
