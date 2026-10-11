//! Shared workload, validation, and timing for the comparison peers.

use serde::{Deserialize, Serialize};
use serde_json::json;
use std::time::Instant;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

pub use futures;
pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Pattern {
    Pushpull,
    Reqrep,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    Sender,
    Receiver,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum PayloadMode {
    Fresh,
    Prepared,
}

#[derive(Debug, Deserialize)]
pub struct Config {
    pub control: String,
    pub endpoint: String,
    pub role: Role,
    pub pattern: Pattern,
    pub payload_mode: PayloadMode,
    pub size: usize,
    pub count: usize,
    pub warmup: usize,
    pub hwm: i32,
    pub io_threads: usize,
    pub verify_full: bool,
}

// These futures run on a current-thread runtime and need no Send bound.
#[allow(async_fn_in_trait)]
pub trait Backend: Sized {
    type Message;
    async fn open(config: &Config) -> Result<(Self, String)>;
    fn message(payload: Vec<u8>) -> Self::Message;
    fn frame_count(message: &Self::Message) -> usize;
    fn payload(message: &Self::Message) -> &[u8];
    fn native_version() -> Option<(i32, i32, i32)>;
    async fn send(&mut self, message: Self::Message) -> Result<()>;
    async fn recv(&mut self) -> Result<Self::Message>;
}

struct Control(BufReader<TcpStream>);

impl Control {
    async fn report(&mut self, value: serde_json::Value) -> Result<()> {
        let mut bytes = serde_json::to_vec(&value)?;
        bytes.push(b'\n');
        self.0.get_mut().write_all(&bytes).await?;
        Ok(())
    }

    async fn expect(&mut self, expected: &str) -> Result<()> {
        let mut line = String::new();
        self.0.read_line(&mut line).await?;
        if line.trim() != expected {
            return Err(format!("expected control command {expected:?}, got {line:?}").into());
        }
        Ok(())
    }
}

fn payload(size: usize, sequence: usize) -> Vec<u8> {
    let mut data = vec![0x5a; size];
    data[..8].copy_from_slice(&(sequence as u64).to_le_bytes());
    data
}

fn validate<B: Backend>(message: &B::Message, size: usize, sequence: usize, full: bool) {
    validate_payload(
        B::frame_count(message),
        B::payload(message),
        size,
        sequence,
        full,
    );
}

fn validate_payload(frames: usize, data: &[u8], size: usize, sequence: usize, full: bool) {
    assert_eq!(frames, 1, "multipart boundary");
    assert_eq!(data.len(), size, "payload length");
    assert_eq!(
        u64::from_le_bytes(data[..8].try_into().unwrap()),
        sequence as u64,
        "message order"
    );
    if full {
        assert!(
            data[8..].iter().all(|&byte| byte == 0x5a),
            "payload corruption"
        );
    }
    std::hint::black_box(data);
}

fn cpu_seconds() -> Result<f64> {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    // getrusage writes this live, correctly sized object. RUSAGE_SELF includes
    // all native and runtime threads in this process, not the controller.
    if unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) } != 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    // A successful getrusage call initialised the complete object.
    let usage = unsafe { usage.assume_init() };
    Ok(usage.ru_utime.tv_sec as f64
        + usage.ru_utime.tv_usec as f64 / 1e6
        + usage.ru_stime.tv_sec as f64
        + usage.ru_stime.tv_usec as f64 / 1e6)
}

fn monotonic_ns() -> Result<u64> {
    let mut time = std::mem::MaybeUninit::<libc::timespec>::uninit();
    // clock_gettime writes this live timespec. CLOCK_MONOTONIC shares an epoch
    // across the two processes on the same Linux host.
    if unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, time.as_mut_ptr()) } != 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    // A successful clock_gettime call initialised the complete object.
    let time = unsafe { time.assume_init() };
    Ok(time.tv_sec as u64 * 1_000_000_000 + time.tv_nsec as u64)
}

#[derive(Serialize)]
struct Measurement {
    count: usize,
    start_ns: u64,
    end_ns: u64,
    cpu_seconds: f64,
    elapsed_seconds: f64,
    cpu_percent: f64,
    rtt_ns: Vec<u64>,
}

async fn warmup<B: Backend>(socket: &mut B, config: &Config) -> Result<()> {
    for sequence in 0..config.warmup {
        if config.role == Role::Sender {
            socket
                .send(B::message(payload(config.size, sequence)))
                .await?;
            if config.pattern == Pattern::Reqrep {
                let message = socket.recv().await?;
                validate::<B>(&message, config.size, sequence, true);
            }
        } else {
            let message = socket.recv().await?;
            validate::<B>(&message, config.size, sequence, true);
            if config.pattern == Pattern::Reqrep {
                socket.send(message).await?;
            }
        }
    }
    Ok(())
}

async fn measure<B: Backend>(
    socket: &mut B,
    config: &Config,
    prepared: Vec<B::Message>,
    control: &mut Control,
) -> Result<Measurement> {
    let mut prepared = prepared.into_iter();
    let latency = config.pattern == Pattern::Reqrep && config.role == Role::Sender;
    let mut rtt_ns = Vec::with_capacity(if latency { config.count } else { 0 });
    let cpu_before = cpu_seconds()?;
    let start_ns = monotonic_ns()?;
    if config.role == Role::Receiver {
        // The controller starts the sender only after the receiver is armed.
        control.report(json!({"event": "armed"})).await?;
    }
    for sequence in 0..config.count {
        // Only REQ/REP latency takes per-operation timestamps. Both throughput
        // peers run fixed counts without clocks, timers, or select in this loop.
        let start = latency.then(Instant::now);
        if config.role == Role::Sender {
            let message = match config.payload_mode {
                PayloadMode::Fresh => B::message(payload(config.size, sequence)),
                PayloadMode::Prepared => prepared.next().expect("prepared message count"),
            };
            socket.send(message).await?;
            if config.pattern == Pattern::Reqrep {
                let message = socket.recv().await?;
                rtt_ns.push(start.unwrap().elapsed().as_nanos() as u64);
                validate::<B>(&message, config.size, sequence, config.verify_full);
            }
        } else {
            let message = socket.recv().await?;
            validate::<B>(&message, config.size, sequence, config.verify_full);
            if config.pattern == Pattern::Reqrep {
                socket.send(message).await?;
            }
        }
    }
    if config.role == Role::Sender && config.pattern == Pattern::Pushpull {
        // Keep native IO work in the CPU interval until all messages arrive.
        // The receiver's end timestamp, not this acknowledgement, bounds throughput.
        control.expect("delivered").await?;
    }
    if config.role == Role::Receiver && config.pattern == Pattern::Reqrep {
        // An async send can return before the final echo reaches the requester.
        control.expect("delivered").await?;
    }
    let end_ns = monotonic_ns()?;
    let cpu_seconds = cpu_seconds()? - cpu_before;
    let elapsed_seconds = (end_ns - start_ns) as f64 / 1e9;
    Ok(Measurement {
        count: config.count,
        start_ns,
        end_ns,
        cpu_seconds,
        elapsed_seconds,
        cpu_percent: 100.0 * cpu_seconds / elapsed_seconds,
        rtt_ns,
    })
}

async fn run<B: Backend>(config: Config) -> Result<()> {
    if config.size < 8 || config.count == 0 || config.warmup == 0 || config.hwm < 1 {
        return Err("size >= 8, positive counts and HWM are required".into());
    }
    let stream = TcpStream::connect(&config.control).await?;
    stream.set_nodelay(true)?;
    let mut control = Control(BufReader::new(stream));
    let (mut socket, endpoint) = B::open(&config).await?;
    control
        .report(json!({"event": "ready", "endpoint": endpoint,
        "native_version": B::native_version(), "pid": std::process::id()}))
        .await?;
    control.expect("warmup").await?;
    warmup(&mut socket, &config).await?;
    control.report(json!({"event": "warm"})).await?;
    control.expect("prepare").await?;
    let prepared = if config.role == Role::Sender && config.payload_mode == PayloadMode::Prepared {
        (0..config.count)
            .map(|sequence| B::message(payload(config.size, sequence)))
            .collect()
    } else {
        Vec::new()
    };
    control.report(json!({"event": "prepared"})).await?;
    control.expect("run").await?;
    let measurement = measure(&mut socket, &config, prepared, &mut control).await?;
    control
        .report(json!({"event": "result", "measurement": measurement}))
        .await?;
    // Both processes stay alive until the controller has both results.
    control.expect("quit").await?;
    drop(socket);
    Ok(())
}

pub fn main<B: Backend>() -> Result<()> {
    let config = serde_json::from_str(
        &std::env::args()
            .nth(1)
            .ok_or("missing JSON configuration")?,
    )?;
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?
        .block_on(run::<B>(config))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checks_full_payload_and_sequence() {
        for size in [8, 16, 4096] {
            for sequence in [0, 1, 12345] {
                validate_payload(1, &payload(size, sequence), size, sequence, true);
            }
        }
    }

    #[test]
    #[should_panic(expected = "message order")]
    fn rejects_duplicate_or_reordered_message() {
        validate_payload(1, &payload(16, 0), 16, 1, false);
    }

    #[test]
    #[should_panic(expected = "multipart boundary")]
    fn rejects_extra_frames() {
        validate_payload(2, &payload(16, 0), 16, 0, false);
    }

    #[test]
    #[should_panic(expected = "payload corruption")]
    fn detects_body_corruption_in_full_validation() {
        let mut data = payload(16, 0);
        data[15] ^= 1;
        validate_payload(1, &data, 16, 0, true);
    }

    #[test]
    #[should_panic(expected = "payload length")]
    fn rejects_truncated_message() {
        validate_payload(1, &payload(8, 0), 16, 0, false);
    }
}
