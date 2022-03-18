pub mod connect;
pub mod kv;
pub mod raft;
pub mod register;

pub mod pb {
    pub mod simple_kv {
        tonic::include_proto!("simple_kv");
    }

    pub mod register {
        tonic::include_proto!("skv_register");
    }
}
