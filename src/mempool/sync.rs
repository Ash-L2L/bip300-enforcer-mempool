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
    jsonrpsee::{
        core::{
            client::ClientT as _,
            params::{ArrayParams, BatchRequestBuilder},
            ClientError as JsonRpcError,
        },
        http_client::HttpClient,
    },
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

use super::{Mempool, MempoolInsertError, MempoolRemoveError};
use crate::zmq::{SequenceMessage, SequenceStream, SequenceStreamError};

/// Items requested while syncing
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
enum RequestItem {
    /// Bool indicating if the tx is a mempool tx.
    /// `false` if the tx is needed as a dependency for a mempool tx
    BatchTx(Vec<(Txid, bool)>),
    Block(BlockHash),
    /// Bool indicating if the tx is a mempool tx.
    /// `false` if the tx is needed as a dependency for a mempool tx
    Tx(Txid, bool),
}

#[derive(Debug)]
struct MempoolSyncing {
    blocks_needed: LinkedHashSet<BlockHash>,
    /// Drop messages with lower mempool sequences.
    /// Set to None after encountering this mempool sequence ID.
    /// Return an error if higher sequence is encountered.
    first_mempool_sequence: Option<u64>,
    mempool: Mempool,
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
    #[error("Combined stream ended unexpectedly")]
    CombinedStreamEnded,
    #[error("Error deserializing tx")]
    DeserializeTx(#[from] bitcoin::consensus::encode::FromHexError),
    #[error("Fee overflow")]
    FeeOverflow,
    #[error("Missing first message with mempool sequence: {0}")]
    FirstMempoolSequence(u64),
    #[error("RPC error")]
    JsonRpc(#[from] JsonRpcError),
    #[error(transparent)]
    MempoolInsert(#[from] MempoolInsertError),
    #[error(transparent)]
    MempoolRemove(#[from] MempoolRemoveError),
    #[error("Sequence stream error")]
    SequenceStream(#[from] SequenceStreamError),
    #[error("Sequence stream ended unexpectedly")]
    SequenceStreamEnded,
}

fn handle_seq_message(
    sync_state: &mut MempoolSyncing,
    seq_msg: SequenceMessage,
) -> Result<(), SyncMempoolError> {
    match seq_msg {
        SequenceMessage::BlockHashConnected(block_hash, _) => {
            // FIXME: remove
            tracing::debug!("Adding block {block_hash} to req queue");
            sync_state.blocks_needed.replace(block_hash);
            sync_state
                .request_queue
                .lock()
                .replace(RequestItem::Block(block_hash));
        }
        SequenceMessage::BlockHashDisconnected(block_hash, _) => {
            // FIXME: remove
            tracing::debug!("Adding block {block_hash} to req queue");
            if !sync_state.mempool.chain.blocks.contains_key(&block_hash) {
                sync_state.blocks_needed.replace(block_hash);
                sync_state
                    .request_queue
                    .lock()
                    .replace(RequestItem::Block(block_hash));
            }
        }
        SequenceMessage::TxHashAdded {
            txid,
            mempool_seq,
            zmq_seq: _,
        } => {
            if let Some(first_mempool_seq) = sync_state.first_mempool_sequence {
                if mempool_seq < first_mempool_seq {
                    // Ignore
                    return Ok(());
                } else if mempool_seq == first_mempool_seq {
                    sync_state.first_mempool_sequence = None;
                } else {
                    return Err(SyncMempoolError::FirstMempoolSequence(
                        first_mempool_seq,
                    ));
                }
            }
            // FIXME: remove
            tracing::debug!("Added {txid} to req queue");
            sync_state.txs_needed.replace(txid);
            sync_state
                .request_queue
                .lock()
                .replace(RequestItem::Tx(txid, true));
        }
        SequenceMessage::TxHashRemoved {
            txid,
            mempool_seq,
            zmq_seq: _,
        } => {
            if let Some(first_mempool_sequence) =
                sync_state.first_mempool_sequence
            {
                if mempool_seq < first_mempool_sequence {
                    // Ignore
                    return Ok(());
                } else if mempool_seq == first_mempool_sequence {
                    sync_state.first_mempool_sequence = None;
                } else {
                    return Err(SyncMempoolError::FirstMempoolSequence(
                        first_mempool_sequence,
                    ));
                }
            }
            tracing::debug!("Remove tx {txid} from req queue");
            sync_state.txs_needed.remove(&txid);
            sync_state
                .request_queue
                .lock()
                .remove(&RequestItem::Tx(txid, true));
        }
    }
    sync_state.seq_message_queue.push_back(seq_msg);
    Ok(())
}

#[derive(Debug)]
enum ResponseItem {
    /// Bool indicating if the tx is a mempool tx.
    /// `false` if the tx is needed as a dependency for a mempool tx
    BatchTx(Vec<(Transaction, bool)>),
    Block(bip300301::client::Block),
    /// Bool indicating if the tx is a mempool tx.
    /// `false` if the tx is needed as a dependency for a mempool tx
    Tx(Transaction, bool),
}

fn handle_resp_block(
    sync_state: &mut MempoolSyncing,
    block: bip300301::client::Block,
) -> Result<(), MempoolRemoveError> {
    sync_state.blocks_needed.remove(&block.hash);
    match sync_state.seq_message_queue.front() {
        Some(SequenceMessage::BlockHashConnected(block_hash, _))
            if *block_hash == block.hash =>
        {
            let mut request_queue_lock = sync_state.request_queue.lock();
            for txid in &block.tx {
                let _removed: Option<_> = sync_state.mempool.remove(txid)?;
                sync_state.txs_needed.remove(txid);
                request_queue_lock.remove(&RequestItem::Tx(*txid, true));
            }
            drop(request_queue_lock);
            sync_state.mempool.chain.tip = block.hash;
            sync_state.seq_message_queue.pop_front();
        }
        Some(SequenceMessage::BlockHashDisconnected(block_hash, _))
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
    Ok(())
}

fn handle_resp_tx(sync_state: &mut MempoolSyncing, tx: Transaction) {
    let txid = tx.compute_txid();
    sync_state.txs_needed.remove(&txid);
    sync_state.tx_cache.insert(txid, tx);
}

// returns `true` if the tx was added successfully
fn try_add_tx_from_cache(
    sync_state: &mut MempoolSyncing,
    txid: &Txid,
) -> Result<bool, SyncMempoolError> {
    let Some(tx) = sync_state.tx_cache.get(txid) else {
        return Ok(false);
    };
    let (mut value_in, value_out) = (Some(Amount::ZERO), Amount::ZERO);
    let mut input_txs_needed = Vec::new();
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
                tracing::trace!("Need {input_txid} for {txid}");
                value_in = None;
                input_txs_needed.push(input_txid);
                continue;
            };
        let value = input_tx.output[vout as usize].value;
        value_in = value_in.map(|value_in| value_in + value);
    }
    for input_txid in input_txs_needed.iter().rev() {
        sync_state.txs_needed.replace(*input_txid);
        sync_state.txs_needed.to_front(input_txid);
    }
    let request = match input_txs_needed.len() {
        0 => None,
        1 => Some(RequestItem::Tx(input_txs_needed[0], false)),
        (2..) => Some(RequestItem::BatchTx(
            input_txs_needed
                .into_iter()
                .map(|input_txid| (input_txid, false))
                .collect(),
        )),
    };
    if let Some(request) = request {
        let mut request_queue_lock = sync_state.request_queue.lock();
        request_queue_lock.replace(request.clone());
        request_queue_lock.to_front(&request);
        drop(request_queue_lock);
    }
    let Some(value_in) = value_in else {
        return Ok(false);
    };
    let Some(fee_delta) = value_in.checked_sub(value_out) else {
        return Err(SyncMempoolError::FeeOverflow);
    };
    sync_state.mempool.insert(tx.clone(), fee_delta.to_sat())?;
    tracing::trace!("added {txid} to mempool");
    let mempool_txs = sync_state.mempool.txs.0.len();
    tracing::debug!(%mempool_txs, "Syncing...");
    Ok(true)
}

// returns `true` if an item was applied successfully
fn try_apply_next_seq_message(
    sync_state: &mut MempoolSyncing,
) -> Result<bool, SyncMempoolError> {
    let res = 'res: {
        match sync_state.seq_message_queue.front() {
            Some(SequenceMessage::BlockHashDisconnected(block_hash, _)) => {
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
                zmq_seq: _,
            }) => {
                let txid = *txid;
                try_add_tx_from_cache(sync_state, &txid)?
            }
            Some(SequenceMessage::TxHashRemoved {
                txid,
                mempool_seq: _,
                zmq_seq: _,
            }) => sync_state.mempool.remove(txid)?.is_some(),
            Some(SequenceMessage::BlockHashConnected(_, _)) | None => false,
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
        ResponseItem::BatchTx(txs) => {
            let mut input_txs_needed = LinkedHashSet::new();
            for (tx, in_mempool) in txs {
                if in_mempool {
                    for input_txid in
                        tx.input.iter().map(|input| input.previous_output.txid)
                    {
                        if !sync_state.tx_cache.contains_key(&input_txid) {
                            input_txs_needed.replace(input_txid);
                        }
                    }
                }
                let () = handle_resp_tx(sync_state, tx);
            }
            sync_state
                .txs_needed
                .extend(input_txs_needed.iter().copied());
            let request = match input_txs_needed.len() {
                0 => None,
                1 => Some(RequestItem::Tx(
                    input_txs_needed.pop_front().unwrap(),
                    false,
                )),
                2.. => Some(RequestItem::BatchTx(
                    input_txs_needed
                        .into_iter()
                        .map(|input_txid| (input_txid, false))
                        .collect(),
                )),
            };
            if let Some(request) = request {
                let mut request_queue_lock = sync_state.request_queue.lock();
                request_queue_lock.replace(request.clone());
                request_queue_lock.to_front(&request);
                drop(request_queue_lock);
            }
        }
        ResponseItem::Block(block) => {
            // FIXME: remove
            tracing::debug!("Handling block {}", block.hash);
            let () = handle_resp_block(sync_state, block)?;
        }
        ResponseItem::Tx(tx, in_mempool) => {
            let mut input_txs_needed = LinkedHashSet::new();
            if in_mempool {
                for input_txid in
                    tx.input.iter().map(|input| input.previous_output.txid)
                {
                    if !sync_state.tx_cache.contains_key(&input_txid) {
                        input_txs_needed.replace(input_txid);
                    }
                }
            }
            let () = handle_resp_tx(sync_state, tx);
            sync_state
                .txs_needed
                .extend(input_txs_needed.iter().copied());
            let request = match input_txs_needed.len() {
                0 => None,
                1 => Some(RequestItem::Tx(
                    input_txs_needed.pop_front().unwrap(),
                    false,
                )),
                2.. => Some(RequestItem::BatchTx(
                    input_txs_needed
                        .into_iter()
                        .map(|input_txid| (input_txid, false))
                        .collect(),
                )),
            };
            if let Some(request) = request {
                let mut request_queue_lock = sync_state.request_queue.lock();
                request_queue_lock.replace(request.clone());
                request_queue_lock.to_front(&request);
                drop(request_queue_lock);
            }
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
        let request_queue = LinkedHashSet::from_iter([
            RequestItem::Block(prev_blockhash),
            RequestItem::BatchTx(
                txids.iter().map(|txid| (*txid, true)).collect(),
            ),
        ]);
        let seq_message_queue = VecDeque::from_iter(txids.iter().map(|txid| {
            SequenceMessage::TxHashAdded {
                txid: *txid,
                mempool_seq: mempool_sequence,
                zmq_seq: 0,
            }
        }));
        MempoolSyncing {
            blocks_needed: LinkedHashSet::from_iter([prev_blockhash]),
            first_mempool_sequence: Some(mempool_sequence + 1),
            mempool: Mempool::new(prev_blockhash),
            request_queue: Arc::new(Mutex::new(request_queue)),
            seq_message_queue,
            tx_cache: HashMap::new(),
            txs_needed: LinkedHashSet::from_iter(txids),
        }
    };

    let request_stream = stream::try_unfold((), {
        let request_queue = sync_state.request_queue.clone();
        move |()| {
            let mut request_queue_lock = request_queue.lock();
            let requests_remaining = request_queue_lock.len();
            let mut next_req = request_queue_lock.pop_front();
            if let Some(RequestItem::Tx(txid, in_mempool)) = next_req {
                let mut txids = vec![(txid, in_mempool)];
                loop {
                    match request_queue_lock.front() {
                        Some(RequestItem::Tx(txid, in_mempool)) => {
                            txids.push((*txid, *in_mempool));
                            request_queue_lock.pop_front();
                        }
                        Some(RequestItem::BatchTx(batch_txids)) => {
                            txids.extend(batch_txids);
                            request_queue_lock.pop_front();
                        }
                        Some(RequestItem::Block(_)) | None => {
                            break;
                        }
                    }
                }
                if txids.len() > 1 {
                    next_req = Some(RequestItem::BatchTx(txids));
                }
            }
            drop(request_queue_lock);
            tracing::debug!(%requests_remaining, "Syncing...");
            async move {
                let Some(next_req) = next_req else {
                    tracing::debug!("Request stream done");
                    return Result::<_, SyncMempoolError>::Ok(None);
                };
                match next_req {
                    RequestItem::BatchTx(txs) => {
                        let in_mempool =
                            HashMap::<_, _>::from_iter(txs.iter().copied());
                        let mut request = BatchRequestBuilder::new();
                        for (txid, _) in txs {
                            let mut params = ArrayParams::new();
                            params.insert(txid).unwrap();
                            params.insert(false).unwrap();
                            request
                                .insert("getrawtransaction", params)
                                .unwrap();
                        }
                        let txs: Vec<(Transaction, bool)> = rpc_client
                            .batch_request(request)
                            .await?
                            .into_ok()
                            .map_err(|mut errs| {
                                JsonRpcError::from(errs.next().unwrap())
                            })?
                            .map(|tx_hex: String| {
                                bitcoin::consensus::encode::deserialize_hex(
                                    &tx_hex,
                                )
                                .map(
                                    |tx: Transaction| {
                                        let txid = tx.compute_txid();
                                        (tx, in_mempool[&txid])
                                    },
                                )
                            })
                            .collect::<Result<_, _>>()?;
                        let item = ResponseItem::BatchTx(txs);
                        Ok(Some((item, ())))
                    }
                    RequestItem::Block(block_hash) => {
                        let block =
                            rpc_client.getblock(block_hash, Some(1)).await?;
                        let item = ResponseItem::Block(block);
                        Ok(Some((item, ())))
                    }
                    RequestItem::Tx(txid, in_mempool) => {
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
                        let item = ResponseItem::Tx(tx, in_mempool);
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
        // FIXME: remove
        tracing::debug!(
            "sync state needed: {} blocks, {} txs",
            sync_state.blocks_needed.len(),
            sync_state.txs_needed.len()
        );
        match combined_stream
            .next()
            .await
            .ok_or(SyncMempoolError::CombinedStreamEnded)?
        {
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
