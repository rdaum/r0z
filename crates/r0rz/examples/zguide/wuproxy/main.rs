#![crate_name = "wuproxy"]

fn main() {
    let context = r0rz::Context::new();
    let frontend = context.socket(r0rz::XSUB).unwrap();
    let backend = context.socket(r0rz::XPUB).unwrap();

    frontend
        .connect("tcp://192.168.55.210:5556")
        .expect("failed connecting frontend");
    backend
        .bind("tcp://10.1.1.0:8100")
        .expect("failed binding backend");
    r0rz::proxy(&frontend, &backend).expect("failed proxying");
}
