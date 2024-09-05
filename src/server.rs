use std::sync::Arc;

use async_trait::async_trait;
use bip300301::{
    bitcoin::hashes::Hash as _,
    client::{BlockTemplate, BlockTemplateRequest, NetworkInfo},
};
use bitcoin::hashes::Hash as _;
use chrono::Utc;
use jsonrpsee::{core::RpcResult, proc_macros::rpc, types::ErrorCode};
use tokio::sync::Mutex;

use crate::mempool::Mempool;

#[rpc(server)]
pub trait Rpc {
    #[method(name = "getblocktemplate")]
    async fn get_block_template(
        &self,
        _request: BlockTemplateRequest,
    ) -> RpcResult<BlockTemplate>;
}

pub struct Server {
    mempool: Arc<Mutex<Mempool>>,
    network_info: NetworkInfo,
    sample_block_template: BlockTemplate,
}

impl Server {
    pub fn new(
        mempool: Arc<Mutex<Mempool>>,
        network_info: NetworkInfo,
        sample_block_template: BlockTemplate,
    ) -> Self {
        Self {
            mempool,
            network_info,
            sample_block_template,
        }
    }
}

fn log_error<Err>(err: Err)
where
    anyhow::Error: From<Err>,
{
    let err = anyhow::Error::from(err);
    tracing::error!("{err:#}");
}

#[async_trait]
impl RpcServer for Server {
    async fn get_block_template(
        &self,
        _request: BlockTemplateRequest,
    ) -> RpcResult<BlockTemplate> {
        const NONCE_RANGE: [u8; 8] = [0, 0, 0, 0, 0xFF, 0xFF, 0xFF, 0xFF];

        let now = Utc::now();
        let BlockTemplate {
            version,
            ref rules,
            ref version_bits_available,
            version_bits_required,
            ref coinbase_aux,
            // FIXME: compute this
            coinbase_value,
            ref mutable,
            sigop_limit,
            size_limit,
            weight_limit,
            ..
        } = self.sample_block_template;
        let mempool_locked = self.mempool.lock().await;
        let target = mempool_locked.next_target();
        let transactions = mempool_locked.propose_txs().map_err(|err| {
            log_error(err);
            jsonrpsee::types::ErrorObject::from(ErrorCode::InternalError)
        })?;
        let tip_block = mempool_locked.tip();
        let prev_blockhash = tip_block.hash;
        let current_time_adjusted =
            (now.timestamp() + self.network_info.time_offset_s) as u64;
        let mintime = std::cmp::max(
            tip_block.mediantime as u64 + 1,
            current_time_adjusted,
        );
        let height = tip_block.height as u32 + 1;
        let res = BlockTemplate {
            version,
            rules: rules.clone(),
            version_bits_available: version_bits_available.clone(),
            version_bits_required,
            prev_blockhash,
            transactions,
            coinbase_aux: coinbase_aux.clone(),
            coinbase_value,
            long_poll_id: None,
            target: target.to_le_bytes(),
            mintime,
            mutable: mutable.clone(),
            nonce_range: NONCE_RANGE,
            sigop_limit,
            size_limit,
            weight_limit,
            current_time: current_time_adjusted,
            compact_target: target.to_compact_lossy(),
            height,
            // FIXME: set this
            default_witness_commitment: None,
        };
        Ok(res)
    }
}
