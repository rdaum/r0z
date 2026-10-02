macro_rules! t {
    ($e:expr) => (
        $e.unwrap_or_else(|e| { panic!("{} failed with {:?}", stringify!($e), e) })
    )
}

fn assert_send<T: Send>(_: T) {}

fn main() {
    let mut context = zmq::Context::new();
    let socket = t!(context.socket(zmq::REP));
    let s = &socket;
    assert_send(s);
}
