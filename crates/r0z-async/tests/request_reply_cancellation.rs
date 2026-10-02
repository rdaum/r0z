#![cfg(all(unix, any(feature = "tokio", feature = "async-io")))]

use r0z_async::{
    request_reply::RequestReplyState as State, AsZmqSocket, Context, Multipart, Result, TmqError,
};
use std::time::Duration;

mod utils;

fn invalid(result: Result<impl Sized>, expected: State) {
    assert!(
        matches!(result, Err(TmqError::InvalidRequestReplyState { state, .. }) if state == expected)
    );
}

#[test]
fn unpolled_operations_leave_state_unchanged() -> Result<()> {
    utils::run(|| async {
        let context = Context::new();
        let mut client = utils::request(&context)
            .set_linger(0)
            .bind("inproc://unpolled")?;
        let mut server = utils::reply(&context)
            .set_linger(0)
            .connect("inproc://unpolled")?;
        drop(client.send(vec!["discarded"].into()));
        drop(server.recv());
        assert_eq!(client.state(), State::SendReady);
        assert_eq!(server.state(), State::ReceiveReady);
        client.send(vec!["request"].into()).await?;
        assert_eq!(server.recv().await?, vec!["request"].into());
        drop(server.send(vec!["discarded reply"].into()));
        assert_eq!(server.state(), State::SendReady);
        server.send(vec!["reply"].into()).await?;
        assert_eq!(client.recv().await?, vec!["reply"].into());
        Ok(())
    })
}

#[test]
fn cancelled_send_resumes_the_original_multipart() -> Result<()> {
    utils::run(|| async {
        let context = Context::new();
        let mut client = utils::request(&context)
            .set_linger(0)
            .bind("inproc://pending-request")?;
        {
            let send = client.send(vec!["first", "", "last"].into());
            futures::pin_mut!(send);
            assert!(futures::poll!(send).is_pending());
        }
        assert_eq!(client.state(), State::SendPending);
        invalid(
            client.send(vec!["replacement"].into()).await,
            State::SendPending,
        );
        invalid(client.recv().await, State::SendPending);
        // Cancelling flush also retains the original multipart.
        {
            let flush = client.flush();
            futures::pin_mut!(flush);
            assert!(futures::poll!(flush).is_pending());
        }
        let mut server = utils::reply(&context)
            .set_linger(0)
            .connect("inproc://pending-request")?;
        client.flush().await?;
        assert_eq!(client.state(), State::ReceiveReady);
        assert_eq!(server.recv().await?, vec!["first", "", "last"].into());
        server.send(vec!["ack"].into()).await?;
        assert_eq!(client.recv().await?, vec!["ack"].into());
        // No duplicate or replacement request was queued.
        assert!(utils::timeout(Duration::from_millis(20), server.recv())
            .await
            .is_err());
        client.send(vec!["next"].into()).await?;
        assert_eq!(server.recv().await?, vec!["next"].into());
        Ok(())
    })
}

#[test]
fn cancelled_exchange_retains_the_outstanding_reply() -> Result<()> {
    utils::run(|| async {
        // Compare native default linger with explicit zero linger. The peer delays receiving and
        // replying until after the timeout. The OS deadline also bounds final teardown.
        for linger in [-1, 0] {
            let context = Context::new();
            let mut client = utils::request(&context)
                .set_linger(linger)
                .bind("inproc://late-reply")?;
            let mut server = utils::reply(&context)
                .set_linger(linger)
                .connect("inproc://late-reply")?;
            let exchange = async {
                client.send(vec!["request"].into()).await?;
                client.recv().await
            };
            assert!(utils::timeout(Duration::from_millis(30), exchange)
                .await
                .is_err());
            assert_eq!(client.state(), State::ReceiveReady);
            invalid(client.send(vec!["retry"].into()).await, State::ReceiveReady);
            assert_eq!(server.recv().await?, vec!["request"].into());
            server.send(vec!["late", "reply"].into()).await?;
            assert_eq!(client.recv().await?, vec!["late", "reply"].into());
            assert_eq!(client.state(), State::SendReady);
            client.send(vec!["second"].into()).await?;
            assert_eq!(server.recv().await?, vec!["second"].into());
            server.send(vec!["second reply"].into()).await?;
            assert_eq!(client.recv().await?, vec!["second reply"].into());
        }
        Ok(())
    })
}

#[test]
fn cancelled_reply_receive_and_invalid_calls_preserve_state() -> Result<()> {
    utils::run(|| async {
        let context = Context::new();
        let mut server = utils::reply(&context)
            .set_linger(0)
            .bind("inproc://server-cancel")?;
        invalid(
            server.send(vec!["too early"].into()).await,
            State::ReceiveReady,
        );
        assert!(utils::timeout(Duration::from_millis(20), server.recv())
            .await
            .is_err());
        assert_eq!(server.state(), State::ReceiveReady);
        let mut client = utils::request(&context)
            .set_linger(0)
            .connect("inproc://server-cancel")?;
        invalid(client.recv().await, State::SendReady);
        assert!(matches!(
            client.send(Multipart::default()).await,
            Err(TmqError::Zmq(r0z::Error::EINVAL))
        ));
        assert_eq!(client.state(), State::SendReady);
        client.flush().await?;
        client.send(vec![""].into()).await?;
        assert_eq!(server.recv().await?, vec![""].into());
        invalid(server.recv().await, State::SendReady);
        server.send(vec!["reply"].into()).await?;
        invalid(
            server.send(vec!["extra reply"].into()).await,
            State::ReceiveReady,
        );
        assert_eq!(client.recv().await?, vec!["reply"].into());
        Ok(())
    })
}

#[test]
fn native_errors_retain_ownership_and_prevent_further_io() -> Result<()> {
    utils::run(|| async {
        for sending in [true, false] {
            let context = Context::new();
            let mut socket = if sending {
                utils::request(&context)
                    .set_linger(0)
                    .bind("inproc://terminate-send")?
            } else {
                utils::reply(&context)
                    .set_linger(0)
                    .bind("inproc://terminate-recv")?
            };
            if sending {
                let send = socket.send(vec!["pending"].into());
                futures::pin_mut!(send);
                assert!(futures::poll!(send).is_pending());
            } else {
                let recv = socket.recv();
                futures::pin_mut!(recv);
                assert!(futures::poll!(recv).is_pending());
            }
            // Termination wakes pending operations with ETERM, then waits for the owned socket.
            let mut terminating = context.clone();
            let terminate = std::thread::spawn(move || terminating.destroy());
            let error = if sending {
                socket.flush().await.unwrap_err()
            } else {
                socket.recv().await.unwrap_err()
            };
            assert!(
                matches!(error, TmqError::Zmq(r0z::Error::ETERM)),
                "{error:?}"
            );
            assert_eq!(socket.state(), State::Failed);
            // The wrapper and native socket are still owned by this scope.
            let _native = socket.get_socket();
            invalid(socket.flush().await, State::Failed);
            invalid(socket.recv().await, State::Failed);
            invalid(
                socket.send(vec!["cannot reuse"].into()).await,
                State::Failed,
            );
            drop(socket);
            terminate.join().unwrap()?;
        }
        Ok(())
    })
}
