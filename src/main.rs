use std::{net::SocketAddr, sync::Arc};

use anyhow::Ok;
use bip300301::MainClient as _;
use bitcoin::{hashes::Hash, BlockHash};
use clap::Parser;
use jsonrpsee::server::ServerHandle;
use tokio::{sync::Mutex, time::Duration};
use tracing_subscriber::{filter as tracing_filter, layer::SubscriberExt};

mod cli;
mod cusf_enforcer;
mod mempool;
mod server;
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

async fn spawn_rpc_server(
    server: server::Server,
    serve_rpc_addr: SocketAddr,
) -> anyhow::Result<ServerHandle> {
    use server::RpcServer;
    let handle = jsonrpsee::server::Server::builder()
        .build(serve_rpc_addr)
        .await?
        .start(server.into_rpc());
    Ok(handle)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = cli::Cli::parse();
    set_tracing_subscriber(cli.log_level)?;
    let (rpc_client, network_info) = {
        const REQUEST_TIMEOUT: Duration = Duration::from_secs(120);
        let client = bip300301::client(
            cli.node_rpc_addr,
            &cli.node_rpc_pass,
            Some(REQUEST_TIMEOUT),
            &cli.node_rpc_user,
        )?;
        // get network info to check that RPC client is configured correctly
        let network_info = client.get_network_info().await?;
        tracing::debug!("connected to RPC server");
        (client, network_info)
    };
    let sample_block_template =
        rpc_client.get_block_template(Default::default()).await?;
    let mut sequence_stream =
        zmq::subscribe_sequence(&cli.node_zmq_addr_sequence).await?;
    let mempool = {
        let prev_blockhash = BlockHash::from_byte_array(
            sample_block_template.prev_blockhash.to_byte_array(),
        );
        mempool::init_sync_mempool(
            &rpc_client,
            &mut sequence_stream,
            prev_blockhash,
        )
        .await?
    };
    tracing::info!("Initial mempool sync complete");
    let mempool = Arc::new(Mutex::new(mempool));
    let server =
        server::Server::new(mempool, network_info, sample_block_template);
    let rpc_server_handle =
        spawn_rpc_server(server, cli.serve_rpc_addr).await?;
    let () = rpc_server_handle.stopped().await;
    Ok(())
}
