use bip300301::MainClient as _;
use clap::Parser;
use tokio::time::Duration;
use tracing_subscriber::{filter as tracing_filter, layer::SubscriberExt};

mod cli;
mod mempool;
mod zmq;

// Configure logger.
fn set_tracing_subscriber(log_level: tracing::Level) -> anyhow::Result<()> {
    let targets_filter = tracing_filter::Targets::new().with_default(log_level);
    let stdout_layer = tracing_subscriber::fmt::layer()
        .compact()
        .with_line_number(true);
    let tracing_subscriber = tracing_subscriber::registry()
        .with(targets_filter)
        .with(stdout_layer);
    tracing::subscriber::set_global_default(tracing_subscriber).map_err(|err| {
        let err = anyhow::Error::from(err);
        anyhow::anyhow!("setting default subscriber failed: {err:#}")
    })
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = cli::Cli::parse();
    set_tracing_subscriber(cli.log_level)?;
    let rpc_client = {
        const REQUEST_TIMEOUT: Duration = Duration::from_secs(120);
        let client = bip300301::client(
            cli.rpc_addr,
            &cli.rpc_pass,
            Some(REQUEST_TIMEOUT),
            &cli.rpc_user,
        )?;
        // get network info to check that RPC client is configured correctly
        let _network_info = client.get_network_info().await?;
        tracing::debug!("connected to RPC server");
        client
    };
    let mut sequence_stream =
        zmq::subscribe_sequence(&cli.zmq_addr_sequence).await?;
    let _mempool_txs =
        mempool::sync_mempool(&rpc_client, &mut sequence_stream).await?;
    Ok(())
}
