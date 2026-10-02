fn main() {
    let context = r0rz::Context::new();
    let _poll_item = {
        let socket = context.socket(r0rz::PAIR).unwrap();
        socket.as_poll_item(r0rz::POLLIN)
    }; //~^ ERROR `socket` does not live long enough [E0597]
}
