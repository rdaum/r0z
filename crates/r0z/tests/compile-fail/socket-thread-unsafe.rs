macro_rules! t {
    ($e:expr) => (
        $e.unwrap_or_else(|e| { panic!("{} failed with {:?}", stringify!($e), e) })
    )
}

fn assert_send<T: Send>(_: T) {}

fn main() {
    let mut context = r0z::Context::new();
    let socket = t!(context.socket(r0z::REP));
    let s = &socket;
    assert_send(s);
}
