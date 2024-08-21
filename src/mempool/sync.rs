//! Initial mempool sync

use bip300301::{
    bitcoin::hashes::Hash as _,
    client::{
        BlockTemplateTransaction, GetRawMempoolClient as _, GetRawMempoolVerbose, GetRawTransactionClient as _, GetRawTransactionVerbose, MainClient as _, RawMempoolTxFees, RawMempoolTxInfo, RawMempoolVerbose
    },
    jsonrpsee::{core::ClientError as JsonRpcError, http_client::HttpClient},
};
use bitcoin::{BlockHash, Transaction, Txid};
use futures::{channel::mpsc, future::{try_join, Either}, stream, StreamExt as _};
use hashlink::{LinkedHashMap, LinkedHashSet};
use thiserror::Error;

use crate::zmq::{SequenceMessage, SequenceStream, SequenceStreamError};
use super::Mempool;

#[derive(Debug)]
struct MempoolSyncing {
    blocks_needed: LinkedHashSet<BlockHash>,
    mempool: Mempool,
    txs_needed: LinkedHashSet<Txid>,
}

impl MempoolSyncing {
    fn is_synced(&self) -> bool {
        self.blocks_needed.is_empty() && self.txs_needed.is_empty()
    }

    fn next_rpc_request(
        &self,
    ) -> Option<Either<bip300301::bitcoin::BlockHash, bip300301::bitcoin::Txid>>
    {
        if let Some(block_hash) = self.blocks_needed.front() {
            Some(Either::Left(*block_hash))
        } else if let Some(txid) = self.txs_needed.front() {
            Some(Either::Right(*txid))
        } else {
            None
        }
    }
}

#[derive(Debug, Error)]
pub enum SyncMempoolError {
    #[error("Error deserializing tx")]
    DeserializeTx(#[from] bitcoin::consensus::encode::FromHexError),
    #[error("RPC error")]
    JsonRpc(#[from] JsonRpcError),
    #[error("Sequence stream error")]
    SequenceStream(#[from] SequenceStreamError),
    #[error("Sequence stream ended unexpectedly")]
    SequenceStreamEnded,
}

fn txs_needed(raw_mempool_verbose: &RawMempoolVerbose) -> LinkedHashSet<Txid> {
    fn insert_ancestors_first(
        txid: &bip300301::bitcoin::Txid,
        mempool_tx_infos: &LinkedHashMap<
            bip300301::bitcoin::Txid,
            &RawMempoolTxInfo,
        >,
        txs_needed: &mut LinkedHashSet<Txid>,
    ) {
        let tx_info = mempool_tx_infos[txid];
        for ancestor in &tx_info.depends {
            insert_ancestors_first(ancestor, mempool_tx_infos, txs_needed);
            let _: Option<Txid> = txs_needed.replace(*txid);
        }
    }
    let mempool_tx_infos: LinkedHashMap<bip300301::bitcoin::Txid, _> =
        raw_mempool_verbose
            .entries
            .iter()
            .map(|(txid, tx_info)| (*txid, tx_info))
            .collect();
    let mut res = LinkedHashSet::new();
    for txid in mempool_tx_infos.keys() {
        insert_ancestors_first(txid, &mempool_tx_infos, &mut res);
    }
    res
}

/// Items requested while syncing
#[derive(Clone, Debug)]
enum RequestItem {
    Block(BlockHash),
    Tx(Txid),
}

/// Items processed while syncing
enum StreamItem {
    ZmqSeq(SequenceMessage),
}

pub async fn sync_mempool(
    rpc_client: &HttpClient,
    sequence_stream: &mut SequenceStream<'_>,
    prev_blockhash: BlockHash,
) -> Result<Mempool, SyncMempoolError> {
    let raw_mempool_verbose: RawMempoolVerbose = rpc_client
        .get_raw_mempool(GetRawMempoolVerbose::<true>)
        .await?;
    let mut sync_state = MempoolSyncing {
        blocks_needed: LinkedHashSet::from_iter([prev_blockhash]),
        mempool: Mempool::new(prev_blockhash),
        txs_needed: txs_needed(&raw_mempool_verbose),
    };
    let (rpc_res_tx, rpc_res_rx) = mpsc::unbounded();
    let mut pending_rpc_request = false;
    let mut combined_stream = stream::select(
        sequence_stream.map(Either::Left),
        rpc_res_rx.map(Either::Right),
    );
    while !sync_state.is_synced() {
        // FIXME: handle txs dropped from mempool after requesting and before
        // receiving notification message
        // FIXME: this condition looks sus
        if !pending_rpc_request {
            if let Some(next_rpc_request) = sync_state.next_rpc_request() {
                tokio::spawn({
                    let rpc_client = rpc_client.clone();
                    let rpc_res_tx = rpc_res_tx.clone();
                    async move {
                        let res = match next_rpc_request {
                            Either::Left(block_hash) => rpc_client
                                .getblock(block_hash, Some(1))
                                .await
                                .map(Either::Left),
                            Either::Right(txid) => try_join(
                                rpc_client.get_raw_transaction(
                                    txid,
                                    GetRawTransactionVerbose::<false>,
                                    None,
                                ),
                                rpc_client.get_mempool_entry(txid),
                            )
                            .await
                            .map(Either::Right),
                        };
                        let () = rpc_res_tx.unbounded_send(res).unwrap_or_else(|err| {
                            let err = anyhow::Error::from(err);
                            tracing::error!(
                                "Failed to push `getrawtransaction` response: {err:#}"
                            );
                        });
                    }
                });
                pending_rpc_request = true;
            }
        }
        match combined_stream
            .next()
            .await
            .ok_or(SyncMempoolError::SequenceStreamEnded)?
        {
            Either::Left(sequence_msg) => match sequence_msg? {
                SequenceMessage::BlockHashConnected(block_hash) => {
                    let _: Option<BlockHash> =
                        sync_state.blocks_needed.replace(block_hash);
                }
                SequenceMessage::BlockHashDisconnected(_) => {
                    // FIXME: should use a work queue
                },
                SequenceMessage::TxHashAdded {
                    txid,
                    mempool_seq: _,
                } => {
                    let _: Option<Txid> = sync_state.txs_needed.replace(txid);
                }
                SequenceMessage::TxHashRemoved {
                    txid,
                    mempool_seq: _,
                } => {
                    sync_state.mempool.remove(&txid);
                }
            },
            Either::Right(rpc_res) => {
                pending_rpc_request = false;
                match rpc_res? {
                    Either::Left(block) => {
                        // Remove txs from connected block
                        for txid in &block.tx {
                            sync_state.mempool.remove(txid);
                            sync_state.txs_needed.remove(txid);
                        }
                        sync_state.mempool.chain.tip = block.hash;
                        sync_state.mempool.chain.blocks.insert(block.hash, block);
                    }
                    Either::Right((tx_hex, tx_info)) => {
                        let tx: Transaction =
                            bitcoin::consensus::encode::deserialize_hex(
                                &tx_hex,
                            )?;
                        let txid = tx.compute_txid();
                        // tx may have already been removed from mempool
                        // FIXME
                        /*
                        if sync_state.txs_needed.remove(&txid) {
                            let _: Option<_> = sync_state
                                .mempool
                                .insert_with_info(tx, tx_info.into());
                        }
                        */
                    }
                }
            }
        }
    }
    Ok(sync_state.mempool)
}