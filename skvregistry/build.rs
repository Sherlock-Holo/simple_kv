use std::path::Path;

use prost_build::Config;

fn main() {
    println!("cargo:rerun-if-changed=proto");

    let builder = tonic_build::configure();

    let mut prost_config = Config::new();
    prost_config.bytes(&["."]);

    let proto_path = Path::new("../proto/register.proto");

    let proto_dir = proto_path
        .parent()
        .expect("proto file should reside in a directory");

    builder
        .compile_with_config(prost_config, &[proto_path], &[proto_dir])
        .unwrap();
}
