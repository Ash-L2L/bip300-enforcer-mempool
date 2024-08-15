use std::collections::{hash_map, BTreeMap, HashMap};

use bip300301::{
    bitcoin::hashes::Hash as _,
    client::{
        GetRawMempoolClient as _, GetRawMempoolVerbose,
        GetRawTransactionClient as _, GetRawTransactionVerbose,
        MainClient as _, RawMempoolTxFees, RawMempoolTxInfo, RawMempoolVerbose,
    },
    jsonrpsee::{core::ClientError as JsonRpcError, http_client::HttpClient},
};
use bitcoin::{hashes::Hash as _, BlockHash, Transaction, Txid};
use fallible_iterator::FallibleIterator;
use futures::{
    channel::mpsc,
    future::{try_join, Either},
    stream, StreamExt,
};
use hashlink::{LinkedHashMap, LinkedHashSet};
use thiserror::Error;

use crate::zmq::{SequenceMessage, SequenceStream, SequenceStreamError};

pub mod iter;

#[derive(Debug)]
pub struct TxInfo {
    pub ancestor_size: u64,
    pub bip125_replaceable: bool,
    /// Map of ancestors to digests of their tx info
    pub depends: BTreeMap<Txid, blake3::Hash>,
    pub descendant_size: u64,
    pub fees: RawMempoolTxFees,
    /// Map of descendants to digests of their tx info
    pub spent_by: BTreeMap<Txid, blake3::Hash>,
    pub unbroadcast: bool,
}

impl TxInfo {
    // Digest when used as an ancestor. Does not commit to descendants
    pub fn digest_as_ancestor(&self) -> blake3::Hash {
        let Self {
            ancestor_size,
            bip125_replaceable,
            depends,
            descendant_size: _,
            fees,
            spent_by: _,
            unbroadcast,
        } = self;
        let RawMempoolTxFees {
            base: base_fee,
            modified: modified_fee,
            ancestor: ancestor_fees,
            descendant: _,
        } = fees;
        (|| {
            let mut hasher = blake3::Hasher::new();
            borsh::to_writer(&mut hasher, ancestor_size)?;
            borsh::to_writer(&mut hasher, bip125_replaceable)?;
            for (txid, digest) in depends {
                borsh::to_writer(
                    &mut hasher,
                    &(txid.as_byte_array(), digest.as_bytes()),
                )?;
            }
            borsh::to_writer(&mut hasher, base_fee)?;
            borsh::to_writer(&mut hasher, modified_fee)?;
            borsh::to_writer(&mut hasher, ancestor_fees)?;
            borsh::to_writer(&mut hasher, unbroadcast)?;
            borsh::io::Result::Ok(hasher.finalize())
        })()
        .unwrap()
    }

    // Digest when used as a descendant. Does not commit to ancestors
    pub fn digest_as_descendant(&self) -> blake3::Hash {
        let Self {
            ancestor_size: _,
            bip125_replaceable,
            depends: _,
            descendant_size,
            fees,
            spent_by,
            unbroadcast,
        } = self;
        let RawMempoolTxFees {
            base: base_fee,
            modified: modified_fee,
            ancestor: _,
            descendant: descendant_fees,
        } = fees;
        (|| {
            let mut hasher = blake3::Hasher::new();
            borsh::to_writer(&mut hasher, bip125_replaceable)?;
            borsh::to_writer(&mut hasher, descendant_size)?;
            borsh::to_writer(&mut hasher, base_fee)?;
            borsh::to_writer(&mut hasher, modified_fee)?;
            borsh::to_writer(&mut hasher, descendant_fees)?;
            for (txid, digest) in spent_by {
                borsh::to_writer(
                    &mut hasher,
                    &(txid.as_byte_array(), digest.as_bytes()),
                )?;
            }
            borsh::to_writer(&mut hasher, unbroadcast)?;
            borsh::io::Result::Ok(hasher.finalize())
        })()
        .unwrap()
    }
}

#[derive(Debug, Error)]
#[error("Missing ancestor for {tx}: {missing}")]
pub struct MissingAncestorError {
    pub tx: Txid,
    pub missing: Txid,
}

#[derive(Debug, Default)]
pub struct Mempool {
    txs: HashMap<Txid, (Transaction, TxInfo)>,
}

impl Mempool {
    /// Insert with info already computed. For use during initial sync only.
    /// All ancestors must exist in the mempool.
    /// Descendants are ignored.
    fn insert_with_info(
        &mut self,
        tx: Transaction,
        info: RawMempoolTxInfo,
    ) -> Result<Option<TxInfo>, MissingAncestorError> {
        let txid = tx.compute_txid();
        let RawMempoolTxInfo {
            vsize,
            weight: _,
            descendant_count: _,
            descendant_size: _,
            ancestor_count: _,
            ancestor_size,
            wtxid: _,
            fees,
            depends,
            spent_by: _,
            bip125_replaceable,
            unbroadcast } = info;
        let depends = depends
            .into_iter()
            .map(|dep_txid| {
                let dep_txid = Txid::from_byte_array(dep_txid.to_byte_array());
                let (_, dep_info) = self.txs.get(&dep_txid).ok_or_else(|| MissingAncestorError {
                    tx: txid,
                    missing: dep_txid
                })?;
                let dep_digest = dep_info.digest_as_ancestor();
                Ok((dep_txid, dep_digest))
            })
            .collect::<Result<_, _>>()?;
        let info = TxInfo {
            ancestor_size,
            bip125_replaceable,
            depends,
            descendant_size: vsize,
            fees: RawMempoolTxFees { descendant: fees.modified, ..fees },
            spent_by: BTreeMap::new(),
            unbroadcast
        };
        let res = self.txs.insert(txid, (tx, info)).map(|(_, info)| info);
        let (mut ancestor_size, mut ancestor_fees) = (0, 0);
        self.ancestors(txid).for_each(|(ancestor_tx, ancestor_info)| {
            ancestor_size += ancestor_tx.vsize() as u64;
            ancestor_fees += ancestor_info.fees.modified;
            Ok(())
        })?;
        let (_, info) = self.txs.get_mut(&txid).unwrap();
        if ancestor_size != info.ancestor_size {
            tracing::warn!(
                %txid,
                calculated = %ancestor_size,
                provided = %info.ancestor_size,
                "Calculated ancestor size does not match provided size"
            );
            info.ancestor_size = ancestor_size;
        }
        if ancestor_fees != info.fees.ancestor {
            tracing::warn!(
                %txid,
                calculated = %ancestor_fees,
                provided = %info.fees.ancestor,
                "Calculated ancestor fees do not match provided fees"
            );
            info.fees.ancestor = ancestor_fees;
        }
        Ok(res)
    }

    fn remove(&mut self, txid: &Txid) -> Option<(Transaction, TxInfo)> {
        let (tx, info) = self.txs.remove(txid)?;
        // FIXME
        Some((tx, info))
    }
}

#[derive(Debug, Default)]
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
            let block_hash = bip300301::bitcoin::BlockHash::from_byte_array(
                *block_hash.as_byte_array(),
            );
            Some(Either::Left(block_hash))
        } else if let Some(txid) = self.txs_needed.front() {
            let txid = bip300301::bitcoin::Txid::from_byte_array(
                *txid.as_byte_array(),
            );
            Some(Either::Right(txid))
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
            let txid = Txid::from_byte_array(*txid.as_byte_array());
            let _: Option<Txid> = txs_needed.replace(txid);
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

pub async fn sync_mempool(
    rpc_client: &HttpClient,
    sequence_stream: &mut SequenceStream<'_>,
) -> Result<Mempool, SyncMempoolError> {
    let raw_mempool_verbose: RawMempoolVerbose = rpc_client
        .get_raw_mempool(GetRawMempoolVerbose::<true>)
        .await?;
    let mut sync_state = MempoolSyncing {
        txs_needed: txs_needed(&raw_mempool_verbose),
        ..Default::default()
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
                SequenceMessage::BlockHashDisconnected(_) => (),
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
                        for txid in block.tx {
                            let txid =
                                Txid::from_byte_array(txid.to_byte_array());
                            sync_state.mempool.remove(&txid);
                            sync_state.txs_needed.remove(&txid);
                        }
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
