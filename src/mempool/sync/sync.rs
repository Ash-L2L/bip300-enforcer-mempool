use std::{collections::VecDeque, sync::Arc};

use tokio::{spawn, sync::RwLock, task::JoinHandle};

use super::super::Mempool;
use crate::{cusf_enforcer::CusfEnforcer, zmq::SequenceMessage};

pub struct TaskState<Enforcer> {
    enforcer: Enforcer,
    seq_message_queue: VecDeque<SequenceMessage>,
}

async fn task<Enforcer>(_enforcer: Enforcer)
where
    Enforcer: CusfEnforcer,
{
}

pub struct MempoolSync {
    mempool: Arc<RwLock<Mempool>>,
    task: JoinHandle<()>,
}

impl MempoolSync {
    fn new<Enforcer>(mempool: Mempool, enforcer: Enforcer) -> Self
    where
        Enforcer: CusfEnforcer + Send + 'static,
    {
        let mempool = Arc::new(RwLock::new(mempool));
        let task = spawn(task(enforcer));
        Self { mempool, task }
    }

    async fn with_mempool<F, Output>(&self, f: F) -> Output
    where
        F: FnOnce(&Mempool) -> Output,
    {
        let mempool_read = self.mempool.read().await;
        f(&mempool_read)
    }
}
