use r0z::*;
use r0z_sys::errno;

#[test]
fn from_raw_eintr() {
    let error = Error::from_raw(errno::EINTR);
    assert_eq!(error, Error::EINTR);
}
