use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};

use clap::Parser;

const DEFAULT_SERVE_RPC_ADDR: SocketAddr =
    SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 21_000));

#[derive(Parser)]
pub struct Cli {
    /// Log level
    #[arg(default_value_t = tracing::Level::DEBUG, long)]
    pub log_level: tracing::Level,
    /// Bitcoin node RPC address
    #[arg(long)]
    pub node_rpc_addr: SocketAddr,
    /// Bitcoin node RPC pass
    #[arg(long)]
    pub node_rpc_pass: String,
    /// Bitcoin node RPC user
    #[arg(long)]
    pub node_rpc_user: String,
    /// Bitcoin node ZMQ endpoint for `sequence`
    #[arg(long)]
    pub node_zmq_addr_sequence: String,
    /// Serve `getblocktemplate` RPC from this address
    #[arg(default_value_t = DEFAULT_SERVE_RPC_ADDR, long)]
    pub serve_rpc_addr: SocketAddr,
}
