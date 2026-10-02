#![crate_name = "msgqueue"]

fn main() {
    let context = r0rz::Context::new();
    let frontend = context.socket(r0rz::ROUTER).unwrap();
    let backend = context.socket(r0rz::DEALER).unwrap();

    frontend
        .bind("tcp://*:5559")
        .expect("failed binding frontend");
    backend
        .bind("tcp://*:5560")
        .expect("failed binding backend");

    r0rz::proxy(&frontend, &backend).expect("failed to proxy");
}
