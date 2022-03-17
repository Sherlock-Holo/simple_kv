#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    skvregistry::run().await
}
