use std::net::SocketAddr;

use clap::Parser;

#[derive(Parser)]
pub struct Cli {
    /// Log level
    #[arg(default_value_t = tracing::Level::DEBUG, long)]
    pub log_level: tracing::Level,
    /// Bitcoin node RPC address
    #[arg(long)]
    pub rpc_addr: SocketAddr,
    /// Bitcoin node RPC pass
    #[arg(long)]
    pub rpc_pass: String,
    /// Bitcoin node RPC user
    #[arg(long)]
    pub rpc_user: String,
    /// Bitcoin node ZMQ endpoint for `sequence`
    #[arg(long)]
    pub zmq_addr_sequence: String,
}
