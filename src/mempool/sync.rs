//! Initial mempool sync

use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
};

use bip300301::{
    bitcoin::hashes::Hash as _,
    client::{
        BlockTemplateTransaction, BoolWitness, GetRawMempoolClient as _,
        GetRawTransactionClient as _, GetRawTransactionVerbose,
        MainClient as _, RawMempoolTxFees, RawMempoolTxInfo, RawMempoolVerbose,
        RawMempoolWithSequence,
    },
    jsonrpsee::{core::ClientError as JsonRpcError, http_client::HttpClient},
};
use bitcoin::{Amount, BlockHash, OutPoint, Transaction, Txid};
use futures::{
    channel::mpsc,
    future::{try_join, Either},
    stream, StreamExt as _,
};
use hashlink::{LinkedHashMap, LinkedHashSet};
use parking_lot::Mutex;
use thiserror::Error;

use super::{Mempool, MempoolRemoveError};
use crate::zmq::{SequenceMessage, SequenceStream, SequenceStreamError};

/// Items requested while syncing
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum RequestItem {
    Block(BlockHash),
    Tx(Txid),
}

#[derive(Debug)]
struct MempoolSyncing {
    blocks_needed: LinkedHashSet<BlockHash>,
    mempool: Mempool,
    next_sequence: u64,
    request_queue: Arc<Mutex<LinkedHashSet<RequestItem>>>,
    seq_message_queue: VecDeque<SequenceMessage>,
    /// Txs not needed in mempool, but requested in order to determine fees
    tx_cache: HashMap<Txid, Transaction>,
    txs_needed: LinkedHashSet<Txid>,
}

impl MempoolSyncing {
    fn is_synced(&self) -> bool {
        self.blocks_needed.is_empty()
            && self.txs_needed.is_empty()
            && self.seq_message_queue.is_empty()
    }
}

#[derive(Debug, Error)]
pub enum SyncMempoolError {
    #[error("Error deserializing tx")]
    DeserializeTx(#[from] bitcoin::consensus::encode::FromHexError),
    #[error("RPC error")]
    JsonRpc(#[from] JsonRpcError),
    #[error(transparent)]
    MempoolRemove(#[from] MempoolRemoveError),
    #[error("Sequence message missing: {0}")]
    SequenceMessageMissing(u64),
    #[error("Sequence stream error")]
    SequenceStream(#[from] SequenceStreamError),
    #[error("Sequence stream ended unexpectedly")]
    SequenceStreamEnded,
}

fn handle_seq_message(
    sync_state: &mut MempoolSyncing,
    seq_msg: SequenceMessage,
) -> Result<(), SyncMempoolError> {
    sync_state.seq_message_queue.push_back(seq_msg);
    match seq_msg {
        SequenceMessage::BlockHashConnected(block_hash) => {
            sync_state.blocks_needed.replace(block_hash);
            sync_state
                .request_queue
                .lock()
                .replace(RequestItem::Block(block_hash));
            Ok(())
        }
        SequenceMessage::BlockHashDisconnected(block_hash) => {
            if !sync_state.mempool.chain.blocks.contains_key(&block_hash) {
                sync_state.blocks_needed.replace(block_hash);
                sync_state
                    .request_queue
                    .lock()
                    .replace(RequestItem::Block(block_hash));
            }
            Ok(())
        }
        SequenceMessage::TxHashAdded { txid, mempool_seq } => {
            if mempool_seq != sync_state.next_sequence {
                Err(SyncMempoolError::SequenceMessageMissing(
                    sync_state.next_sequence,
                ))
            } else {
                sync_state.next_sequence += 1;
                sync_state.txs_needed.replace(txid);
                sync_state
                    .request_queue
                    .lock()
                    .replace(RequestItem::Tx(txid));
                Ok(())
            }
        }
        SequenceMessage::TxHashRemoved {
            txid: _,
            mempool_seq,
        } => {
            if mempool_seq != sync_state.next_sequence {
                return Err(SyncMempoolError::SequenceMessageMissing(
                    sync_state.next_sequence,
                ));
            } else {
                sync_state.next_sequence += 1;
                Ok(())
            }
        }
    }
}

#[derive(Debug)]
enum ResponseItem {
    Block(bip300301::client::Block),
    Tx(Transaction),
}

fn handle_resp_block(
    sync_state: &mut MempoolSyncing,
    block: bip300301::client::Block,
) {
    sync_state.blocks_needed.remove(&block.hash);
    match sync_state.seq_message_queue.front() {
        Some(SequenceMessage::BlockHashConnected(block_hash))
            if *block_hash == block.hash =>
        {
            for txid in &block.tx {
                sync_state.mempool.remove(txid);
            }
            sync_state.mempool.chain.tip = block.hash;
            sync_state.seq_message_queue.pop_front();
        }
        Some(SequenceMessage::BlockHashDisconnected(block_hash))
            if *block_hash == block.hash
                && sync_state.mempool.chain.tip == block.hash =>
        {
            for txid in &block.tx {
                // FIXME: insert without info
                let () = todo!();
            }
            sync_state.mempool.chain.tip = block
                .previousblockhash
                .unwrap_or_else(|| BlockHash::all_zeros());
            sync_state.seq_message_queue.pop_front();
        }
        Some(_) | None => (),
    }
    sync_state.mempool.chain.blocks.insert(block.hash, block);
}

fn handle_resp_tx(sync_state: &mut MempoolSyncing, tx: Transaction) {
    let txid = tx.compute_txid();
    sync_state.txs_needed.remove(&txid);
    sync_state.tx_cache.insert(txid, tx);
}

// returns `true` if the tx was added successfully
fn try_add_tx_from_cache(sync_state: &mut MempoolSyncing, txid: &Txid) -> bool {
    let Some(tx) = sync_state.tx_cache.get(txid) else {
        return false;
    };
    let (mut value_in, value_out) = (Some(Amount::ZERO), Amount::ZERO);
    for input in &tx.input {
        let OutPoint {
            txid: input_txid,
            vout,
        } = input.previous_output;
        let input_tx =
            if let Some(input_tx) = sync_state.tx_cache.get(&input_txid) {
                input_tx
            } else if let Some((input_tx, _)) =
                sync_state.mempool.txs.0.get(&input_txid)
            {
                input_tx
            } else {
                value_in = None;
                sync_state.txs_needed.replace(input_txid);
                sync_state.txs_needed.to_front(&input_txid);
                let request = RequestItem::Tx(input_txid);
                let mut request_queue_lock = sync_state.request_queue.lock();
                request_queue_lock.replace(request);
                request_queue_lock.to_front(&request);
                continue;
            };
        let value = input_tx.output[vout as usize].value;
        value_in = value_in.map(|value_in| value_in + value);
    }
    let Some(value_in) = value_in else {
        return false;
    };
    let _fee_delta = value_in.checked_sub(value_out);
    // FIXME: insert without info
    let () = todo!();
    true
}

// returns `true` if an item was applied successfully
fn try_apply_next_seq_message(
    sync_state: &mut MempoolSyncing,
) -> Result<bool, SyncMempoolError> {
    let res = 'res: {
        match sync_state.seq_message_queue.front() {
            Some(SequenceMessage::BlockHashDisconnected(block_hash)) => {
                if sync_state.mempool.chain.tip != *block_hash {
                    break 'res false;
                };
                let Some(block) =
                    sync_state.mempool.chain.blocks.get(block_hash)
                else {
                    break 'res false;
                };
                for txid in &block.tx {
                    // FIXME: insert without info
                    let () = todo!();
                }
                sync_state.mempool.chain.tip = block
                    .previousblockhash
                    .unwrap_or_else(|| BlockHash::all_zeros());
                true
            }
            Some(SequenceMessage::TxHashAdded {
                txid,
                mempool_seq: _,
            }) => {
                let txid = *txid;
                try_add_tx_from_cache(sync_state, &txid)
            }
            Some(SequenceMessage::TxHashRemoved {
                txid,
                mempool_seq: _,
            }) => sync_state.mempool.remove(txid)?.is_some(),
            Some(SequenceMessage::BlockHashConnected(_)) | None => false,
        }
    };
    if res {
        sync_state.seq_message_queue.pop_front();
    }
    Ok(res)
}

fn handle_resp(
    sync_state: &mut MempoolSyncing,
    resp: ResponseItem,
) -> Result<(), SyncMempoolError> {
    match resp {
        ResponseItem::Block(block) => {
            let () = handle_resp_block(sync_state, block);
        }
        ResponseItem::Tx(tx) => {
            let () = handle_resp_tx(sync_state, tx);
        }
    }
    while try_apply_next_seq_message(sync_state)? {}
    Ok(())
}

/// Items processed while syncing
#[derive(Debug)]
enum CombinedStreamItem {
    ZmqSeq(Result<SequenceMessage, SequenceStreamError>),
    Response(Result<ResponseItem, SyncMempoolError>),
}

pub async fn sync_mempool(
    rpc_client: &HttpClient,
    sequence_stream: &mut SequenceStream<'_>,
    prev_blockhash: BlockHash,
) -> Result<Mempool, SyncMempoolError> {
    let RawMempoolWithSequence {
        txids,
        mempool_sequence,
    } = rpc_client
        .get_raw_mempool(BoolWitness::<false>, BoolWitness::<true>)
        .await?;
    let mut sync_state = {
        let request_queue = LinkedHashSet::from_iter(
            txids.iter().copied().map(RequestItem::Tx), // FIXME: request prev block(s)
        );
        MempoolSyncing {
            blocks_needed: LinkedHashSet::from_iter([prev_blockhash]),
            mempool: Mempool::new(prev_blockhash),
            next_sequence: mempool_sequence + 1,
            request_queue: Arc::new(Mutex::new(request_queue)),
            seq_message_queue: VecDeque::new(),
            tx_cache: HashMap::new(),
            txs_needed: LinkedHashSet::from_iter(txids),
        }
    };

    let request_stream = stream::try_unfold((), {
        let request_queue = sync_state.request_queue.clone();
        move |()| {
            let mut request_queue_lock = request_queue.lock();
            let next_req = request_queue_lock.pop_front();
            drop(request_queue_lock);
            async move {
                let Some(next_req) = next_req else {
                    return Result::<_, SyncMempoolError>::Ok(None);
                };
                match next_req {
                    RequestItem::Block(block_hash) => {
                        let block =
                            rpc_client.getblock(block_hash, Some(1)).await?;
                        let item = ResponseItem::Block(block);
                        Ok(Some((item, ())))
                    }
                    RequestItem::Tx(txid) => {
                        let tx_hex = rpc_client
                            .get_raw_transaction(
                                txid,
                                GetRawTransactionVerbose::<false>,
                                None,
                            )
                            .await?;
                        let tx: Transaction =
                            bitcoin::consensus::encode::deserialize_hex(
                                &tx_hex,
                            )?;
                        let item = ResponseItem::Tx(tx);
                        Ok(Some((item, ())))
                    }
                }
            }
        }
    })
    .boxed();

    let mut combined_stream = stream::select(
        sequence_stream.map(CombinedStreamItem::ZmqSeq),
        request_stream.map(CombinedStreamItem::Response),
    );
    while !sync_state.is_synced() {
        match combined_stream.next().await.unwrap() {
            CombinedStreamItem::ZmqSeq(seq_msg) => {
                let () = handle_seq_message(&mut sync_state, seq_msg?)?;
            }
            CombinedStreamItem::Response(resp) => {
                let () = handle_resp(&mut sync_state, resp?)?;
            }
        }
    }
    Ok(sync_state.mempool)
}

/*
pub async fn sync_mempool(
    rpc_client: &HttpClient,
    sequence_stream: &mut SequenceStream<'_>,
    prev_blockhash: BlockHash,
) -> Result<Mempool, SyncMempoolError> {
    let RawMempoolWithSequence { txids, mempool_sequence } = rpc_client
        .get_raw_mempool(BoolWitness::<false>, BoolWitness::<true>)
        .await?;
    let mut sync_state = MempoolSyncing {
        blocks_needed: LinkedHashSet::from_iter([prev_blockhash]),
        mempool: Mempool::new(prev_blockhash),
        txs_needed: LinkedHashSet::from_iter(txids),
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
*/
