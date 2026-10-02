// Test whether `r0z::poll()` works with `PollItem`s constructed from
// arbitrary FDs.

use nix::unistd;
use std::os::fd::AsRawFd;
use std::thread;

#[test]
fn test_pipe_poll() {
    let (pipe_read, pipe_write) = unistd::pipe().expect("pipe creation failed");
    let writer_thread = thread::spawn(move || {
        pipe_writer(pipe_write);
    });
    let pipe_item = r0z::PollItem::from_fd(pipe_read.as_raw_fd(), r0z::POLLIN);
    assert!(pipe_item.has_fd(pipe_read.as_raw_fd()));

    let mut poll_items = [pipe_item];
    assert_eq!(r0z::poll(&mut poll_items, 1000).unwrap(), 1);
    assert!(poll_items[0].get_revents().contains(r0z::POLLIN));

    let mut buf = vec![0];
    assert_eq!(unistd::read(&pipe_read, &mut buf).unwrap(), 1);
    assert_eq!(buf, b"X");

    writer_thread.join().unwrap();
}

fn pipe_writer(fd: std::os::fd::OwnedFd) {
    unistd::write(&fd, b"X").expect("pipe write failed");
}
