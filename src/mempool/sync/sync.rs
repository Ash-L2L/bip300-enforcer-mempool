use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
};

use bip300301::jsonrpsee::http_client::HttpClient;
use bitcoin::{Transaction, Txid};
use futures::{stream, StreamExt as _, TryFutureExt as _};
use thiserror::Error;
use tokio::{spawn, sync::RwLock, task::JoinHandle};

use super::{
    super::Mempool, batched_request, BatchedResponseItem, CombinedStreamItem,
    RequestError, RequestQueue,
};
use crate::{
    cusf_enforcer::CusfEnforcer,
    zmq::{SequenceMessage, SequenceStream, SequenceStreamError},
};

#[derive(Debug, Default)]
pub struct SyncState {
    request_queue: RequestQueue,
    seq_message_queue: VecDeque<SequenceMessage>,
    /// Txs not needed in mempool, but requested in order to determine fees
    tx_cache: HashMap<Txid, Transaction>,
}

#[derive(Debug, Error)]
pub enum SyncTaskError {
    #[error("Combined stream ended unexpectedly")]
    CombinedStreamEnded,
    #[error("Request error")]
    Request(#[from] RequestError),
    #[error("Sequence stream error")]
    SequenceStream(#[from] SequenceStreamError),
}

fn handle_seq_message(
    sync_state: &mut SyncState,
    seq_msg: SequenceMessage,
) -> Result<(), SyncTaskError> {
    // FIXME
    todo!()
}

fn handle_resp(
    sync_state: &mut SyncState,
    resp: BatchedResponseItem,
) -> Result<(), SyncTaskError> {
    // FIXME
    todo!()
}

async fn task<Enforcer>(
    _enforcer: Enforcer,
    rpc_client: HttpClient,
    sequence_stream: SequenceStream<'static>,
) -> Result<(), SyncTaskError>
where
    Enforcer: CusfEnforcer,
{
    let mut sync_state = SyncState::default();
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
                let () = handle_seq_message(&mut sync_state, seq_msg?)?;
            }
            CombinedStreamItem::Response(resp) => {
                let () = handle_resp(&mut sync_state, resp?)?;
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
        rpc_client: &HttpClient,
        sequence_stream: SequenceStream<'static>,
    ) -> Self
    where
        Enforcer: CusfEnforcer + Send + 'static,
    {
        let mempool = Arc::new(RwLock::new(mempool));
        let task = spawn(
            task(enforcer, rpc_client.clone(), sequence_stream).unwrap_or_else(
                |err| {
                    let err = anyhow::Error::from(err);
                    tracing::error!("{err:#}");
                },
            ),
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
