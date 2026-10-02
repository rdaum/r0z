#![cfg(all(unix, any(feature = "tokio", feature = "async-io")))]

use r0z_async::{Context, Result, SocketExt};

mod utils;

#[test]
fn async_builders_default_to_zero_linger_and_preserve_overrides() -> Result<()> {
    utils::run(|| async {
        let context = Context::new();
        for linger in [None, Some(0), Some(25), Some(-1)] {
            macro_rules! check {
                ($constructor:ident) => {{
                    let mut builder = utils::$constructor(&context);
                    if let Some(value) = linger {
                        builder = builder.set_linger(value);
                    }
                    let socket = builder.bind(&format!(
                        "inproc://linger-{}-{linger:?}",
                        stringify!($constructor)
                    ))?;
                    assert_eq!(socket.get_linger()?, linger.unwrap_or(0));
                }};
            }
            check!(dealer);
            check!(pair);
            check!(publish);
            check!(pull);
            check!(push);
            check!(reply);
            check!(request);
            check!(router);

            // SUB delays runtime registration until subscribe; preserve the option across it.
            let mut builder = utils::subscribe(&context);
            if let Some(value) = linger {
                builder = builder.set_linger(value);
            }
            let subscriber = builder
                .connect(&format!("inproc://linger-subscribe-{linger:?}"))?
                .subscribe(b"")?;
            assert_eq!(subscriber.get_linger()?, linger.unwrap_or(0));
        }
        // The async default must not affect synchronous sockets, even on the same context.
        assert_eq!(context.socket(r0z::REQ)?.get_linger()?, -1);
        Ok(())
    })
}
