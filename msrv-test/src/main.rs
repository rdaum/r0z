fn main() {
    let ctx = r0rz::Context::new();

    let socket = ctx.socket(r0rz::REQ).unwrap();
    socket.connect("tcp://127.0.0.1:1234").unwrap();
    socket.send("hello world!", 0).unwrap();
}
