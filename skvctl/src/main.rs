#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    skvctl::run().await
}
