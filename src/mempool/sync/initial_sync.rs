//! Initial mempool sync

use std::collections::{HashMap, VecDeque};

use bip300301::{
    bitcoin::hashes::Hash as _,
    client::{BoolWitness, GetRawMempoolClient as _, RawMempoolWithSequence},
    jsonrpsee::{core::ClientError as JsonRpcError, http_client::HttpClient},
};
use bitcoin::{Amount, BlockHash, OutPoint, Transaction, Txid};
use futures::{stream, StreamExt as _};
use hashlink::LinkedHashSet;
use thiserror::Error;

use super::{
    super::{Mempool, MempoolInsertError, MempoolRemoveError},
    batched_request, BatchedResponseItem, CombinedStreamItem, RequestError,
    RequestItem, RequestQueue, ResponseItem,
};
use crate::zmq::{SequenceMessage, SequenceStream, SequenceStreamError};

#[derive(Debug)]
struct MempoolSyncing {
    blocks_needed: LinkedHashSet<BlockHash>,
    /// Drop messages with lower mempool sequences.
    /// Set to None after encountering this mempool sequence ID.
    /// Return an error if higher sequence is encountered.
    first_mempool_sequence: Option<u64>,
    mempool: Mempool,
    request_queue: RequestQueue,
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
    #[error("Request error")]
    Request(#[from] RequestError),
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
                .push_back(RequestItem::Block(block_hash));
        }
        SequenceMessage::BlockHashDisconnected(block_hash, _) => {
            // FIXME: remove
            tracing::debug!("Adding block {block_hash} to req queue");
            if !sync_state.mempool.chain.blocks.contains_key(&block_hash) {
                sync_state.blocks_needed.replace(block_hash);
                sync_state
                    .request_queue
                    .push_back(RequestItem::Block(block_hash));
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
                .push_back(RequestItem::Tx(txid, true));
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
                .remove(&RequestItem::Tx(txid, true));
        }
    }
    sync_state.seq_message_queue.push_back(seq_msg);
    Ok(())
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
            for txid in &block.tx {
                let _removed: Option<_> = sync_state.mempool.remove(txid)?;
                sync_state.txs_needed.remove(txid);
                sync_state
                    .request_queue
                    .remove(&RequestItem::Tx(*txid, true));
            }
            sync_state.mempool.chain.tip = block.hash;
            sync_state.seq_message_queue.pop_front();
        }
        Some(SequenceMessage::BlockHashDisconnected(block_hash, _))
            if *block_hash == block.hash
                && sync_state.mempool.chain.tip == block.hash =>
        {
            for _txid in &block.tx {
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
    for input_txid in input_txs_needed.into_iter().rev() {
        sync_state.txs_needed.replace(input_txid);
        sync_state.txs_needed.to_front(&input_txid);
        sync_state
            .request_queue
            .push_front(RequestItem::Tx(input_txid, false))
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
                for _txid in &block.tx {
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
            }) => {
                // FIXME: review -- looks sus
                sync_state.mempool.remove(txid)?.is_some()
            }
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
    resp: BatchedResponseItem,
) -> Result<(), SyncMempoolError> {
    match resp {
        BatchedResponseItem::BatchTx(txs) => {
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
            for input_txid in input_txs_needed.into_iter().rev() {
                sync_state
                    .request_queue
                    .push_front(RequestItem::Tx(input_txid, false))
            }
        }
        BatchedResponseItem::Single(ResponseItem::Block(block)) => {
            // FIXME: remove
            tracing::debug!("Handling block {}", block.hash);
            let () = handle_resp_block(sync_state, block)?;
        }
        BatchedResponseItem::Single(ResponseItem::Tx(tx, in_mempool)) => {
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
            for input_txid in input_txs_needed.into_iter().rev() {
                sync_state
                    .request_queue
                    .push_front(RequestItem::Tx(input_txid, false))
            }
        }
        BatchedResponseItem::BatchRejectTx
        | BatchedResponseItem::Single(ResponseItem::RejectTx) => {}
    }
    while try_apply_next_seq_message(sync_state)? {}
    Ok(())
}

pub async fn init_sync_mempool(
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
        let request_queue = RequestQueue::default();
        request_queue.push_back(RequestItem::Block(prev_blockhash));
        for txid in &txids {
            request_queue.push_back(RequestItem::Tx(*txid, true));
        }
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
            request_queue,
            seq_message_queue,
            tx_cache: HashMap::new(),
            txs_needed: LinkedHashSet::from_iter(txids),
        }
    };

    let response_stream = sync_state
        .request_queue
        .clone()
        .then(|request| batched_request(rpc_client, request))
        .boxed();

    let mut combined_stream = stream::select(
        sequence_stream.map(CombinedStreamItem::ZmqSeq),
        response_stream.map(CombinedStreamItem::Response),
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
