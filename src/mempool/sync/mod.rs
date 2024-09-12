use std::{
    collections::HashMap,
    sync::Arc,
    task::{Poll, Waker},
};

use bip300301::{
    client::{
        GetRawTransactionClient as _, GetRawTransactionVerbose, MainClient as _,
    },
    jsonrpsee::{
        core::{
            client::ClientT as _,
            params::{ArrayParams, BatchRequestBuilder, ObjectParams},
            ClientError as JsonRpcError,
        },
        http_client::HttpClient,
    },
};
use bitcoin::{Amount, BlockHash, Transaction, Txid};

use futures::Stream;
use hashlink::LinkedHashSet;
use nonempty::NonEmpty;
use parking_lot::Mutex;
use thiserror::Error;

use crate::zmq::{SequenceMessage, SequenceStreamError};

mod initial_sync;
mod sync;

pub use initial_sync::init_sync_mempool;
pub use sync::MempoolSync;

/// Items requested while syncing
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum RequestItem {
    Block(BlockHash),
    /// Reject a tx
    RejectTx(Txid),
    /// Bool indicating if the tx is a mempool tx.
    /// `false` if the tx is needed as a dependency for a mempool tx
    Tx(Txid, bool),
}

/// Batched items requested while syncing
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
enum BatchedRequestItem {
    BatchRejectTx(NonEmpty<Txid>),
    /// Bool indicating if the tx is a mempool tx.
    /// `false` if the tx is needed as a dependency for a mempool tx
    BatchTx(NonEmpty<(Txid, bool)>),
    Single(RequestItem),
}

#[derive(Debug, Default)]
struct RequestQueueInner {
    queue: Mutex<LinkedHashSet<RequestItem>>,
    waker: Mutex<Option<Waker>>,
}

#[derive(Clone, Debug, Default)]
#[repr(transparent)]
struct RequestQueue {
    inner: Arc<RequestQueueInner>,
}

impl RequestQueue {
    /// Remove the request from the queue, if it exists
    fn remove(&self, request: &RequestItem) {
        self.inner.queue.lock().remove(request);
    }

    /// Push the request to the back, if it does not already exist
    fn push_back(&self, request: RequestItem) {
        self.inner.queue.lock().replace(request);
        if let Some(waker) = self.inner.waker.lock().take() {
            waker.wake()
        }
    }

    /// Push the request to the front, if it does not already exist
    fn push_front(&self, request: RequestItem) {
        let mut queue_lock = self.inner.queue.lock();
        queue_lock.replace(request);
        queue_lock.to_front(&request);
        if let Some(waker) = self.inner.waker.lock().take() {
            waker.wake()
        }
    }
}

impl Stream for RequestQueue {
    type Item = BatchedRequestItem;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let mut queue_lock = self.inner.queue.lock();
        *self.inner.waker.lock() = Some(cx.waker().clone());
        match queue_lock.pop_front() {
            Some(request @ RequestItem::Block(_)) => {
                Poll::Ready(Some(BatchedRequestItem::Single(request)))
            }
            Some(RequestItem::RejectTx(txid)) => {
                let mut txids = NonEmpty::new(txid);
                while let Some(&RequestItem::RejectTx(txid)) =
                    queue_lock.front()
                {
                    queue_lock.pop_front();
                    txids.push(txid);
                }
                let batched_request = if txids.tail.is_empty() {
                    BatchedRequestItem::Single(RequestItem::RejectTx(
                        txids.head,
                    ))
                } else {
                    BatchedRequestItem::BatchRejectTx(txids)
                };
                Poll::Ready(Some(batched_request))
            }
            Some(RequestItem::Tx(txid, in_mempool)) => {
                let mut txids = NonEmpty::new((txid, in_mempool));
                while let Some(&RequestItem::Tx(txid, in_mempool)) =
                    queue_lock.front()
                {
                    queue_lock.pop_front();
                    txids.push((txid, in_mempool));
                }
                let batched_request = if txids.tail.is_empty() {
                    let (txid, in_mempool) = txids.head;
                    BatchedRequestItem::Single(RequestItem::Tx(
                        txid, in_mempool,
                    ))
                } else {
                    BatchedRequestItem::BatchTx(txids)
                };
                Poll::Ready(Some(batched_request))
            }
            None => Poll::Pending,
        }
    }
}

/// Responses received while syncing
#[derive(Clone, Debug)]
enum ResponseItem {
    Block(bip300301::client::Block),
    RejectTx,
    /// Bool indicating if the tx is a mempool tx.
    /// `false` if the tx is needed as a dependency for a mempool tx
    Tx(Transaction, bool),
}

/// Responses received while syncing
#[derive(Clone, Debug)]
enum BatchedResponseItem {
    BatchRejectTx,
    /// Bool indicating if the tx is a mempool tx.
    /// `false` if the tx is needed as a dependency for a mempool tx
    BatchTx(Vec<(Transaction, bool)>),
    Single(ResponseItem),
}

#[derive(Debug, Error)]
pub enum RequestError {
    #[error("Error deserializing tx")]
    DeserializeTx(#[from] bitcoin::consensus::encode::FromHexError),
    #[error("RPC error")]
    JsonRpc(#[from] JsonRpcError),
}

async fn batched_request(
    rpc_client: &HttpClient,
    request: BatchedRequestItem,
) -> Result<BatchedResponseItem, RequestError> {
    const NEGATIVE_MAX_SATS: i64 = -(21_000_000 * 100_000_000);
    match request {
        BatchedRequestItem::BatchRejectTx(txs) => {
            let mut request = BatchRequestBuilder::new();
            for txid in txs {
                let mut params = ObjectParams::new();
                params.insert("txid", txid).unwrap();
                // set priority fee to extremely negative so that it is cleared
                // from mempool as soon as possible
                params.insert("fee_delta", NEGATIVE_MAX_SATS).unwrap();
                request.insert("prioritisetransaction", params).unwrap();
            }
            let _resp: Vec<bool> = rpc_client
                .batch_request(request)
                .await?
                .into_ok()
                .map_err(|mut errs| JsonRpcError::from(errs.next().unwrap()))?
                .collect();
            Ok(BatchedResponseItem::BatchRejectTx)
        }
        BatchedRequestItem::BatchTx(txs) => {
            let in_mempool = HashMap::<_, _>::from_iter(txs.iter().copied());
            let mut request = BatchRequestBuilder::new();
            for (txid, _) in txs {
                let mut params = ArrayParams::new();
                params.insert(txid).unwrap();
                params.insert(false).unwrap();
                request.insert("getrawtransaction", params).unwrap();
            }
            let txs: Vec<(Transaction, bool)> = rpc_client
                .batch_request(request)
                .await?
                .into_ok()
                .map_err(|mut errs| JsonRpcError::from(errs.next().unwrap()))?
                .map(|tx_hex: String| {
                    bitcoin::consensus::encode::deserialize_hex(&tx_hex).map(
                        |tx: Transaction| {
                            let txid = tx.compute_txid();
                            (tx, in_mempool[&txid])
                        },
                    )
                })
                .collect::<Result<_, _>>()?;
            Ok(BatchedResponseItem::BatchTx(txs))
        }
        BatchedRequestItem::Single(RequestItem::Block(block_hash)) => {
            let block = rpc_client.getblock(block_hash, Some(1)).await?;
            let resp = ResponseItem::Block(block);
            Ok(BatchedResponseItem::Single(resp))
        }
        BatchedRequestItem::Single(RequestItem::RejectTx(txid)) => {
            // set priority fee to extremely negative so that it is cleared
            // from mempool as soon as possible
            let _: bool = rpc_client
                .prioritize_transaction(txid, NEGATIVE_MAX_SATS)
                .await?;
            let resp = ResponseItem::RejectTx;
            Ok(BatchedResponseItem::Single(resp))
        }
        BatchedRequestItem::Single(RequestItem::Tx(txid, in_mempool)) => {
            let tx_hex = rpc_client
                .get_raw_transaction(
                    txid,
                    GetRawTransactionVerbose::<false>,
                    None,
                )
                .await?;
            let tx: Transaction =
                bitcoin::consensus::encode::deserialize_hex(&tx_hex)?;
            let resp = ResponseItem::Tx(tx, in_mempool);
            Ok(BatchedResponseItem::Single(resp))
        }
    }
}

/// Items processed while syncing
#[derive(Debug)]
enum CombinedStreamItem {
    ZmqSeq(Result<SequenceMessage, SequenceStreamError>),
    Response(Result<BatchedResponseItem, RequestError>),
}
