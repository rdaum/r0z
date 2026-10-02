#![crate_name = "msgqueue"]

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

    r0z::proxy(&frontend, &backend).expect("failed to proxy");
}
