#![crate_name = "rrbroker"]

fn main() {
    let context = r0z::Context::new();
    let frontend = context.socket(r0z::ROUTER).unwrap();
    let backend = context.socket(r0z::DEALER).unwrap();

    frontend
        .bind("tcp://*:5559")
        .expect("failed binding frontend");
    backend
        .bind("tcp://*:5560")
        .expect("failed binding backend");

    loop {
        let mut items = [
            frontend.as_poll_item(r0z::POLLIN),
            backend.as_poll_item(r0z::POLLIN),
        ];
        r0z::poll(&mut items, -1).unwrap();

        if items[0].is_readable() {
            loop {
                let message = frontend.recv_msg(0).unwrap();
                let more = message.get_more();
                backend
                    .send(message, if more { r0z::SNDMORE } else { 0 })
                    .unwrap();
                if !more {
                    break;
                }
            }
        }
        if items[1].is_readable() {
            loop {
                let message = backend.recv_msg(0).unwrap();
                let more = message.get_more();
                frontend
                    .send(message, if more { r0z::SNDMORE } else { 0 })
                    .unwrap();
                if !more {
                    break;
                }
            }
        }
    }
}
