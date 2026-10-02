fn main() {
    let context = r0z::Context::new();
    let _poll_item = {
        let socket = context.socket(r0z::PAIR).unwrap();
        socket.as_poll_item(r0z::POLLIN)
    }; //~^ ERROR `socket` does not live long enough [E0597]
}
