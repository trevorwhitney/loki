//! Generates Rust types for the scan protocol shared with the Go server. The
//! .proto file in pkg/dataobj/arrowflight/scanpb is the single source of
//! truth; this build script requires `protoc` on PATH.

fn main() -> std::io::Result<()> {
    let proto_dir = "../../pkg/dataobj/arrowflight/scanpb";
    let proto = format!("{proto_dir}/scan.proto");
    println!("cargo:rerun-if-changed={proto}");
    prost_build::compile_protos(&[proto.as_str()], &[proto_dir])
}
