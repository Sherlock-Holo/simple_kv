pub mod kv;
pub mod raft;

pub mod pb {
    tonic::include_proto!("simple_kv");
}
