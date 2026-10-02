#![deny(missing_docs)]

//! # r0z-async: native ZeroMQ bindings with runtime adapters
//!
//! This crate wraps `r0z` and native `libzmq`. It is derived from `tmq`.
//! The built-in adapters support Tokio (the default) and `async-io` on Unix.
//! Disable default features and enable `async-io` to build without Tokio.
//! Select an adapter per socket with [`SocketBuilder::with_runtime`]; see [`runtime`].
//! With neither feature, applications must supply their own [`runtime::Runtime`].
//! Socket builders default to zero linger: closing a socket discards queued outgoing messages.
//! Use [`SocketBuilder::set_linger`] to select a finite or infinite wait during context shutdown.
//!
//! ## Currently Implemented Sockets
//!
//! * Request/Reply
//! * Publish/Subscribe
//! * Dealer/Router
//! * Push/Pull
//! ## Usage
//!
//! See the [examples](https://github.com/rdaum/r0z/tree/main/crates/r0z-async/examples) for usage.
//!
//! ### Publish Example
//!
//! To publish messages to all connected subscribers, you can use the `publish` function:
//!
//! ```rust,no_run
//! # #[cfg(feature = "tokio")]
//! # mod example {
//! use r0z_async::{publish, Context, Result};
//!
//! use futures::SinkExt;
//! use log::info;
//! use std::env;
//! use std::time::Duration;
//! use tokio::time::sleep;
//!
//! #[tokio::main]
//! async fn main() -> Result<()> {
//!
//!     let mut socket = publish(&Context::new()).bind("tcp://127.0.0.1:7899")?;
//!
//!     let mut i = 0;
//!
//!     loop {
//!         i += 1;
//!
//!         socket
//!             .send(vec!["topic", &format!("Broadcast #{}", i)])
//!             .await?;
//!
//!         sleep(Duration::from_secs(1)).await;
//!     }
//! }
//! # }
//! # fn main() {}
//! ```

/// Shortcut for [`Result<T, r0z_async::TmqError>`].
pub type Result<T> = std::result::Result<T, TmqError>;

pub use r0z::{Context, Message};

/// Internal re-exports
pub use error::TmqError;
pub use message::Multipart;
pub use socket::{AsZmqSocket, SocketExt};
pub use socket_builder::SocketBuilder;
pub use socket_types::*;

/// Crate re-exports
pub(crate) use comm::*;

#[macro_use]
mod macros;

mod comm;
mod error;
mod message;
mod poll;
pub mod runtime;
mod socket;
mod socket_builder;
mod socket_types;
