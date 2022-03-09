#[tokio::main]
async fn main() -> anyhow::Result<()> {
    simple_kv::run().await
}
