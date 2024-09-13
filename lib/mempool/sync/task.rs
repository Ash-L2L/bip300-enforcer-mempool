use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
};

use bip300301::jsonrpsee::http_client::HttpClient;
use bitcoin::{
    hashes::Hash as _, Amount, BlockHash, OutPoint, Transaction, Txid,
};
use educe::Educe;
use futures::{stream, StreamExt as _, TryFutureExt as _};
use hashlink::LinkedHashSet;
use imbl::HashSet;
use thiserror::Error;
use tokio::{spawn, sync::RwLock, task::JoinHandle};

use super::{
    super::Mempool, batched_request, BatchedResponseItem, CombinedStreamItem,
    RequestError, RequestQueue, ResponseItem,
};
use crate::{
    cusf_enforcer::CusfEnforcer,
    mempool::{sync::RequestItem, MempoolInsertError, MempoolRemoveError},
    zmq::{SequenceMessage, SequenceStream, SequenceStreamError},
};

#[derive(Debug)]
pub struct SyncState {
    /// Txs rejected by the CUSF enforcer
    rejected_txs: HashSet<Txid>,
    request_queue: RequestQueue,
    seq_message_queue: VecDeque<SequenceMessage>,
    /// Txs not needed in mempool, but requested in order to determine fees
    tx_cache: HashMap<Txid, Transaction>,
}

#[derive(Educe)]
#[educe(Debug(bound(Enforcer::AcceptTxError: std::fmt::Debug)))]
#[derive(Error)]
pub enum SyncTaskError<Enforcer>
where
    Enforcer: CusfEnforcer,
{
    #[error("Combined stream ended unexpectedly")]
    CombinedStreamEnded,
    #[error("CUSF enforcer error")]
    CusfEnforcer(#[source] Enforcer::AcceptTxError),
    #[error("Fee overflow")]
    FeeOverflow,
    #[error(transparent)]
    MempoolInsert(#[from] MempoolInsertError),
    #[error(transparent)]
    MempoolRemove(#[from] MempoolRemoveError),
    #[error("Request error")]
    Request(#[from] RequestError),
    #[error("Sequence stream error")]
    SequenceStream(#[from] SequenceStreamError),
}

async fn handle_seq_message(
    mempool: &RwLock<Mempool>,
    sync_state: &mut SyncState,
    seq_msg: SequenceMessage,
) {
    match seq_msg {
        SequenceMessage::BlockHashConnected(block_hash, _) => {
            // FIXME: remove
            tracing::debug!("Adding block {block_hash} to req queue");
            sync_state
                .request_queue
                .push_back(RequestItem::Block(block_hash));
        }
        SequenceMessage::BlockHashDisconnected(block_hash, _) => {
            // FIXME: remove
            tracing::debug!("Adding block {block_hash} to req queue");
            if !mempool.read().await.chain.blocks.contains_key(&block_hash) {
                sync_state
                    .request_queue
                    .push_back(RequestItem::Block(block_hash));
            }
        }
        SequenceMessage::TxHashAdded {
            txid,
            mempool_seq: _,
            zmq_seq: _,
        } => {
            // FIXME: remove
            tracing::debug!("Added {txid} to req queue");
            sync_state
                .request_queue
                .push_back(RequestItem::Tx(txid, true));
        }
        SequenceMessage::TxHashRemoved {
            txid,
            mempool_seq: _,
            zmq_seq: _,
        } => {
            tracing::debug!("Remove tx {txid} from req queue");
            sync_state
                .request_queue
                .remove(&RequestItem::Tx(txid, true));
        }
    }
    sync_state.seq_message_queue.push_back(seq_msg);
}

fn handle_resp_tx(sync_state: &mut SyncState, tx: Transaction) {
    let txid = tx.compute_txid();
    sync_state.tx_cache.insert(txid, tx);
}

fn handle_resp_block(
    mempool: &mut Mempool,
    sync_state: &mut SyncState,
    block: bip300301::client::Block,
) -> Result<(), MempoolRemoveError> {
    match sync_state.seq_message_queue.front() {
        Some(SequenceMessage::BlockHashConnected(block_hash, _))
            if *block_hash == block.hash =>
        {
            for txid in &block.tx {
                let _removed: Option<_> = mempool.remove(txid)?;
                sync_state
                    .request_queue
                    .remove(&RequestItem::Tx(*txid, true));
            }
            mempool.chain.tip = block.hash;
            sync_state.seq_message_queue.pop_front();
        }
        Some(SequenceMessage::BlockHashDisconnected(block_hash, _))
            if *block_hash == block.hash && mempool.chain.tip == block.hash =>
        {
            for _txid in &block.tx {
                // FIXME: insert without info
                let () = todo!();
            }
            mempool.chain.tip =
                block.previousblockhash.unwrap_or_else(BlockHash::all_zeros);
            sync_state.seq_message_queue.pop_front();
        }
        Some(_) | None => (),
    }
    mempool.chain.blocks.insert(block.hash, block);
    Ok(())
}

// returns `true` if the tx was added or rejected successfully
fn try_add_tx_from_cache<Enforcer>(
    enforcer: &mut Enforcer,
    mempool: &mut Mempool,
    sync_state: &mut SyncState,
    txid: &Txid,
) -> Result<bool, SyncTaskError<Enforcer>>
where
    Enforcer: CusfEnforcer,
{
    let Some(tx) = sync_state.tx_cache.get(txid) else {
        return Ok(false);
    };
    let (mut value_in, value_out) = (Some(Amount::ZERO), Amount::ZERO);
    let mut input_txs_needed = LinkedHashSet::new();
    let mut input_txs = HashMap::<Txid, &Transaction>::new();
    for input in &tx.input {
        let OutPoint {
            txid: input_txid,
            vout,
        } = input.previous_output;
        let input_tx = if sync_state.rejected_txs.contains(&input_txid) {
            // Reject tx
            tracing::trace!("rejecting {txid}: rejected ancestor");
            sync_state.rejected_txs.insert(*txid);
            sync_state
                .request_queue
                .push_front(RequestItem::RejectTx(*txid));
            return Ok(true);
        } else if let Some(input_tx) = sync_state.tx_cache.get(&input_txid) {
            input_tx
        } else if let Some((input_tx, _)) = mempool.txs.0.get(&input_txid) {
            input_tx
        } else {
            tracing::trace!("Need {input_txid} for {txid}");
            value_in = None;
            input_txs_needed.replace(input_txid);
            continue;
        };
        let value = input_tx.output[vout as usize].value;
        value_in = value_in.map(|value_in| value_in + value);
        input_txs.insert(input_txid, input_tx);
    }
    for input_txid in input_txs_needed.into_iter().rev() {
        sync_state
            .request_queue
            .push_front(RequestItem::Tx(input_txid, false))
    }
    let Some(value_in) = value_in else {
        return Ok(false);
    };
    let Some(fee_delta) = value_in.checked_sub(value_out) else {
        return Err(SyncTaskError::FeeOverflow);
    };
    if enforcer
        .accept_tx(tx, &input_txs)
        .map_err(SyncTaskError::CusfEnforcer)?
    {
        mempool.insert(tx.clone(), fee_delta.to_sat())?;
        tracing::trace!("added {txid} to mempool");
    } else {
        tracing::trace!("rejecting {txid}");
        sync_state.rejected_txs.insert(*txid);
        sync_state
            .request_queue
            .push_front(RequestItem::RejectTx(*txid));
    }
    let mempool_txs = mempool.txs.0.len();
    tracing::debug!(%mempool_txs, "Syncing...");
    Ok(true)
}

// returns `true` if an item was applied successfully
fn try_apply_next_seq_message<Enforcer>(
    enforcer: &mut Enforcer,
    mempool: &mut Mempool,
    sync_state: &mut SyncState,
) -> Result<bool, SyncTaskError<Enforcer>>
where
    Enforcer: CusfEnforcer,
{
    let res = 'res: {
        match sync_state.seq_message_queue.front() {
            Some(SequenceMessage::BlockHashDisconnected(block_hash, _)) => {
                if mempool.chain.tip != *block_hash {
                    break 'res false;
                };
                let Some(block) = mempool.chain.blocks.get(block_hash) else {
                    break 'res false;
                };
                for _txid in &block.tx {
                    // FIXME: insert without info
                    let () = todo!();
                }
                mempool.chain.tip = block
                    .previousblockhash
                    .unwrap_or_else(BlockHash::all_zeros);
                true
            }
            Some(SequenceMessage::TxHashAdded {
                txid,
                mempool_seq: _,
                zmq_seq: _,
            }) => {
                let txid = *txid;
                try_add_tx_from_cache(enforcer, mempool, sync_state, &txid)?
            }
            Some(SequenceMessage::TxHashRemoved {
                txid,
                mempool_seq: _,
                zmq_seq: _,
            }) => {
                // FIXME: review -- looks sus
                mempool.remove(txid)?.is_some()
            }
            Some(SequenceMessage::BlockHashConnected(_, _)) | None => false,
        }
    };
    if res {
        sync_state.seq_message_queue.pop_front();
    }
    Ok(res)
}

async fn handle_resp<Enforcer>(
    enforcer: &mut Enforcer,
    mempool: &RwLock<Mempool>,
    sync_state: &mut SyncState,
    resp: BatchedResponseItem,
) -> Result<(), SyncTaskError<Enforcer>>
where
    Enforcer: CusfEnforcer,
{
    let mut mempool_write = mempool.write().await;
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
            for input_txid in input_txs_needed.into_iter().rev() {
                sync_state
                    .request_queue
                    .push_front(RequestItem::Tx(input_txid, false))
            }
        }
        BatchedResponseItem::Single(ResponseItem::Block(block)) => {
            // FIXME: remove
            tracing::debug!("Handling block {}", block.hash);
            let () = handle_resp_block(&mut mempool_write, sync_state, *block)?;
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
            let () = handle_resp_tx(sync_state, *tx);
            for input_txid in input_txs_needed.into_iter().rev() {
                sync_state
                    .request_queue
                    .push_front(RequestItem::Tx(input_txid, false))
            }
        }
        BatchedResponseItem::BatchRejectTx
        | BatchedResponseItem::Single(ResponseItem::RejectTx) => {}
    }
    while try_apply_next_seq_message(enforcer, &mut mempool_write, sync_state)?
    {
    }
    Ok(())
}

async fn task<Enforcer>(
    mut enforcer: Enforcer,
    mempool: Arc<RwLock<Mempool>>,
    tx_cache: HashMap<Txid, Transaction>,
    rpc_client: HttpClient,
    sequence_stream: SequenceStream<'static>,
) -> Result<(), SyncTaskError<Enforcer>>
where
    Enforcer: CusfEnforcer,
{
    // Filter mempool with enforcer
    let rejected_txs: LinkedHashSet<Txid> = {
        let mut mempool_write = mempool.write().await;
        let rejected_txs = mempool_write
            .try_filter(|tx, mempool_inputs| {
                let mut tx_inputs = mempool_inputs.clone();
                for tx_in in &tx.input {
                    let input_txid = tx_in.previous_output.txid;
                    if tx_inputs.contains_key(&input_txid) {
                        continue;
                    }
                    let input_tx = &tx_cache[&input_txid];
                    tx_inputs.insert(input_txid, input_tx);
                }
                enforcer.accept_tx(tx, &tx_inputs)
            })
            .map_err(|err| match err {
                either::Either::Left(mempool_remove_err) => {
                    SyncTaskError::MempoolRemove(mempool_remove_err)
                }
                either::Either::Right(enforcer_err) => {
                    SyncTaskError::CusfEnforcer(enforcer_err)
                }
            })?
            .keys()
            .copied()
            .collect();
        drop(mempool_write);
        rejected_txs
    };
    let request_queue = RequestQueue::default();
    let rejected_txs: HashSet<Txid> = rejected_txs
        .into_iter()
        .inspect(|rejected_txid| {
            request_queue.push_back(RequestItem::RejectTx(*rejected_txid));
        })
        .collect();
    let mut sync_state = SyncState {
        rejected_txs,
        request_queue,
        seq_message_queue: VecDeque::new(),
        tx_cache,
    };
    let response_stream = sync_state
        .request_queue
        .clone()
        .then(|request| batched_request(&rpc_client, request))
        .boxed();
    let mut combined_stream = stream::select(
        sequence_stream.map(CombinedStreamItem::ZmqSeq),
        response_stream.map(CombinedStreamItem::Response),
    );
    loop {
        match combined_stream
            .next()
            .await
            .ok_or(SyncTaskError::CombinedStreamEnded)?
        {
            CombinedStreamItem::ZmqSeq(seq_msg) => {
                let () =
                    handle_seq_message(&mempool, &mut sync_state, seq_msg?)
                        .await;
            }
            CombinedStreamItem::Response(resp) => {
                let () = handle_resp(
                    &mut enforcer,
                    &mempool,
                    &mut sync_state,
                    resp?,
                )
                .await?;
            }
        }
    }
}

pub struct MempoolSync {
    mempool: Arc<RwLock<Mempool>>,
    task: JoinHandle<()>,
}

impl MempoolSync {
    pub fn new<Enforcer>(
        enforcer: Enforcer,
        mempool: Mempool,
        tx_cache: HashMap<Txid, Transaction>,
        rpc_client: &HttpClient,
        sequence_stream: SequenceStream<'static>,
    ) -> Self
    where
        Enforcer: CusfEnforcer + Send + 'static,
    {
        let mempool = Arc::new(RwLock::new(mempool));
        let task = spawn(
            task(
                enforcer,
                mempool.clone(),
                tx_cache,
                rpc_client.clone(),
                sequence_stream,
            )
            .unwrap_or_else(|err| {
                let err = anyhow::Error::from(err);
                tracing::error!("{err:#}");
            }),
        );
        Self { mempool, task }
    }

    pub async fn with_mempool<F, Output>(&self, f: F) -> Output
    where
        F: FnOnce(&Mempool) -> Output,
    {
        let mempool_read = self.mempool.read().await;
        f(&mempool_read)
    }
}

impl Drop for MempoolSync {
    fn drop(&mut self) {
        self.task.abort()
    }
}
