#[test]
fn test_has() {
    // Until we can clean up the API `has` must return Some(_), not matter
    // wether the capability is actually supported or not.
    assert!(r0z::has("ipc").is_some());
}

#[test]
fn test_has_rejects_interior_nul() {
    assert_eq!(r0z::has("\0:"), None);
}

#[test]
fn curve_key_generation_matches_native_capability() {
    if r0z::has("curve").unwrap() {
        r0z::CurveKeyPair::new().unwrap();
    } else {
        assert_eq!(r0z::CurveKeyPair::new().unwrap_err(), r0z::Error::ENOTSUP);
    }
}
