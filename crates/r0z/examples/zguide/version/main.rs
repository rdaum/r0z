#![crate_name = "version"]

fn main() {
    let (major, minor, patch) = r0z::version();
    println!("Current 0MQ version is {}.{}.{}", major, minor, patch);
}
