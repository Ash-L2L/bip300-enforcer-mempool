use std::collections::{
    btree_map, hash_map, BTreeMap, BTreeSet, HashMap, HashSet,
};

use bip300301::{
    bitcoin::hashes::Hash as _,
    client::{
        BlockTemplateTransaction, GetRawMempoolClient as _,
        GetRawTransactionClient as _, GetRawTransactionVerbose,
        MainClient as _, RawMempoolTxFees, RawMempoolTxInfo, RawMempoolVerbose,
    },
    jsonrpsee::{core::ClientError as JsonRpcError, http_client::HttpClient},
};
use bitcoin::{
    absolute::Height, hashes::Hash as _, Block, BlockHash, Target, Transaction,
    Txid, Weight,
};
use futures::{
    channel::mpsc,
    future::{try_join, Either},
    stream, StreamExt,
};
use hashlink::{LinkedHashMap, LinkedHashSet};
use imbl::{ordmap, OrdMap, OrdSet};
use indexmap::{IndexMap, IndexSet};
use lending_iterator::LendingIterator;
use thiserror::Error;

use crate::zmq::{SequenceMessage, SequenceStream, SequenceStreamError};

pub mod iter;
pub mod iter_mut;
mod sync;

pub use sync::sync_mempool;

#[derive(Clone, Copy, Debug, Eq)]
pub struct FeeRate {
    fee: u64,
    size: u64,
}

impl Ord for FeeRate {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // (self.fee / self.size) > (other.fee / other.size) ==>
        // (self.fee * other.size) > (other.fee * self.size)
        let lhs = self.fee as u128 * other.size as u128;
        let rhs = other.fee as u128 * self.size as u128;
        lhs.cmp(&rhs)
    }
}

impl PartialEq for FeeRate {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other).is_eq()
    }
}

impl PartialOrd for FeeRate {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Clone, Debug)]
pub struct TxInfo {
    pub ancestor_size: u64,
    pub bip125_replaceable: bool,
    pub depends: OrdSet<Txid>,
    pub descendant_size: u64,
    pub fees: RawMempoolTxFees,
    pub spent_by: OrdSet<Txid>,
    pub unbroadcast: bool,
}

#[derive(Debug, Error)]
#[error("Missing ancestor for {tx}: {missing}")]
pub struct MissingAncestorError {
    pub tx: Txid,
    pub missing: Txid,
}

#[derive(Debug, Error)]
#[error("Missing descendant for {tx}: {missing}")]
pub struct MissingDescendantError {
    pub tx: Txid,
    pub missing: Txid,
}

#[derive(Debug, Error)]
#[error("Missing descendants key: {0}")]
pub struct MissingDescendantsKeyError(Txid);

#[derive(Debug, Error)]
pub enum MempoolInsertError {
    #[error(transparent)]
    MissingAncestor(#[from] MissingAncestorError),
    #[error(transparent)]
    MissingDescendantsKey(#[from] MissingDescendantsKeyError),
}

#[derive(Debug, Error)]
#[error("Missing by_ancestor_fee_rate key: {0:?}")]
pub struct MissingByAncestorFeeRateKeyError(FeeRate);

#[derive(Debug, Error)]
pub enum MempoolRemoveError {
    #[error(transparent)]
    MissingAncestor(#[from] MissingAncestorError),
    #[error(transparent)]
    MissingByAncestorFeeRateKey(#[from] MissingByAncestorFeeRateKeyError),
    #[error(transparent)]
    MissingDescendant(#[from] MissingDescendantError),
    #[error(transparent)]
    MissingDescendantsKey(#[from] MissingDescendantsKeyError),
}

#[derive(Clone, Debug, Default)]
struct ByAncestorFeeRate(OrdMap<FeeRate, LinkedHashSet<Txid>>);

impl ByAncestorFeeRate {
    fn insert(&mut self, fee_rate: FeeRate, txid: Txid) {
        self.0.entry(fee_rate).or_default().insert(txid);
    }

    /// returns `true` if removed successfully, or `false` if not found
    fn remove(&mut self, fee_rate: FeeRate, txid: Txid) -> bool {
        match self.0.entry(fee_rate) {
            ordmap::Entry::Occupied(mut entry) => {
                let txs = entry.get_mut();
                txs.remove(&txid);
                if txs.is_empty() {
                    entry.remove();
                }
                true
            }
            ordmap::Entry::Vacant(_) => false,
        }
    }

    // Iterate from low-to-high fee rate, in insertion order
    fn iter(&self) -> impl DoubleEndedIterator<Item = (FeeRate, Txid)> + '_ {
        self.0.iter().flat_map(|(fee_rate, txids)| {
            txids.iter().map(|txid| (*fee_rate, *txid))
        })
    }

    // Iterate from high-to-low fee rate, in insertion order
    fn iter_rev(
        &self,
    ) -> impl DoubleEndedIterator<Item = (FeeRate, Txid)> + '_ {
        self.0.iter().rev().flat_map(|(fee_rate, txids)| {
            txids.iter().map(|txid| (*fee_rate, *txid))
        })
    }
}

#[derive(Clone, Debug)]
struct Chain {
    tip: BlockHash,
    blocks: imbl::HashMap<BlockHash, bip300301::client::Block>,
}

impl Chain {
    // Iterate over blocks from tip towards genesis.
    // Not all history is guaranteed to exist, so this iterator might return
    // `None` before the genesis block.
    fn iter(&self) -> impl Iterator<Item = &bip300301::client::Block> {
        let mut next = Some(self.tip);
        std::iter::from_fn(move || {
            if let Some(block) = self.blocks.get(&next?) {
                next = block.previousblockhash;
                Some(block)
            } else {
                next = None;
                None
            }
        })
    }
}

#[derive(Clone, Debug, Default)]
struct MempoolTxs(imbl::HashMap<Txid, (Transaction, TxInfo)>);

// MUST be cheap to clone so that constructing block templates is cheap
#[derive(Clone, Debug)]
pub struct Mempool {
    by_ancestor_fee_rate: ByAncestorFeeRate,
    chain: Chain,
    txs: MempoolTxs,
}

impl Mempool {
    fn new(prev_blockhash: BlockHash) -> Self {
        let chain = Chain {
            tip: prev_blockhash,
            blocks: imbl::HashMap::new(),
        };
        Self {
            by_ancestor_fee_rate: ByAncestorFeeRate::default(),
            chain,
            txs: MempoolTxs::default(),
        }
    }

    pub fn tip(&self) -> &bip300301::client::Block {
        &self.chain.blocks[&self.chain.tip]
    }

    pub fn tip_hash(&self) -> BlockHash {
        self.chain.tip
    }

    pub fn tip_height(&self) -> u64 {
        self.chain.blocks[&self.chain.tip].height as u64
    }

    pub fn next_target(&self) -> Target {
        // FIXME: calculate this properly
        self.chain.blocks[&self.chain.tip].compact_target.into()
    }

    /// Insert with info already computed. For use during initial sync only.
    /// All ancestors must exist in the mempool.
    /// Descendants must not exist.
    fn insert_with_info(
        &mut self,
        tx: Transaction,
        info: RawMempoolTxInfo,
    ) -> Result<Option<TxInfo>, MempoolInsertError> {
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
            unbroadcast,
        } = info;
        let depends = depends.into_iter().collect();
        for dep in &depends {
            self.txs
                .0
                .get_mut(dep)
                .ok_or(MissingAncestorError {
                    tx: txid,
                    missing: *dep,
                })?
                .1
                .spent_by
                .insert(txid);
        }
        let info = TxInfo {
            ancestor_size,
            bip125_replaceable,
            depends,
            descendant_size: vsize,
            fees: RawMempoolTxFees {
                descendant: fees.modified,
                ..fees
            },
            spent_by: OrdSet::new(),
            unbroadcast,
        };
        let modified_fees = info.fees.modified;
        let res = self.txs.0.insert(txid, (tx, info)).map(|(_, info)| info);
        let (mut ancestor_size, mut ancestor_fees) = (0, 0);
        self.txs.ancestors_mut(txid).try_for_each(|ancestor_info| {
            let (ancestor_tx, ancestor_info) = ancestor_info?;
            ancestor_size += ancestor_tx.vsize() as u64;
            ancestor_fees += ancestor_info.fees.modified;
            ancestor_info.descendant_size += vsize;
            ancestor_info.fees.descendant += modified_fees;
            Result::<_, MempoolInsertError>::Ok(())
        })?;
        let (_, info) = self.txs.0.get_mut(&txid).unwrap();
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
        let ancestor_fee_rate = FeeRate {
            fee: ancestor_fees,
            size: ancestor_size,
        };
        self.by_ancestor_fee_rate.insert(ancestor_fee_rate, txid);
        Ok(res)
    }

    /// Remove a tx from the mempool. Descendants are updated but not removed.
    fn remove(
        &mut self,
        txid: &Txid,
    ) -> Result<Option<(Transaction, TxInfo)>, MempoolRemoveError> {
        let (tx, info) = self
            .txs
            .0
            .get(txid)
            .ok_or(MissingDescendantsKeyError(*txid))?;
        let ancestor_size = info.ancestor_size;
        let vsize = tx.vsize() as u64;
        let fees = RawMempoolTxFees { ..info.fees };
        let mut descendants = self.txs.descendants_mut(*txid);
        // Skip first element
        let _: Option<_> = descendants.next().transpose()?;
        let () = descendants.try_for_each(|desc| {
            let (desc_tx, desc_info) = desc?;
            let ancestor_fee_rate = FeeRate {
                fee: desc_info.fees.ancestor,
                size: desc_info.ancestor_size,
            };
            let desc_txid = desc_tx.compute_txid();
            if !self
                .by_ancestor_fee_rate
                .remove(ancestor_fee_rate, desc_txid)
            {
                let err = MissingByAncestorFeeRateKeyError(ancestor_fee_rate);
                return Err(err.into());
            };
            desc_info.ancestor_size -= vsize;
            desc_info.fees.ancestor -= fees.modified;
            let ancestor_fee_rate = FeeRate {
                fee: desc_info.fees.ancestor,
                size: desc_info.ancestor_size,
            };
            self.by_ancestor_fee_rate
                .insert(ancestor_fee_rate, desc_txid);
            desc_info.depends.remove(txid);
            Result::<_, MempoolRemoveError>::Ok(())
        })?;
        // Update all ancestors
        let () = self.txs.ancestors_mut(*txid).try_for_each(|anc| {
            let (_anc_tx, anc_info) = anc?;
            anc_info.descendant_size -= vsize;
            anc_info.fees.descendant -= fees.modified;
            anc_info.spent_by.remove(txid);
            Result::<_, MempoolRemoveError>::Ok(())
        })?;
        let ancestor_fee_rate = FeeRate {
            fee: fees.ancestor,
            size: ancestor_size,
        };
        // Update `self.by_ancestor_fee_rate`
        if !self.by_ancestor_fee_rate.remove(ancestor_fee_rate, *txid) {
            let err = MissingByAncestorFeeRateKeyError(ancestor_fee_rate);
            return Err(err.into());
        };
        Ok(self.txs.0.remove(txid))
    }

    /// choose txs for a block proposal, mutating the underlying mempool
    fn propose_txs_mut(
        &mut self,
    ) -> Result<IndexSet<Txid>, MempoolRemoveError> {
        let mut res = IndexSet::new();
        let mut total_size = 0;
        loop {
            let Some((ancestor_fee_rate, txid)) = self
                .by_ancestor_fee_rate
                .iter_rev()
                .find(|(ancestor_fee_rate, _txid)| {
                    let total_weight =
                        Weight::from_vb(total_size + ancestor_fee_rate.size);
                    total_weight
                        .is_some_and(|weight| weight < Weight::MAX_BLOCK)
                })
            else {
                break;
            };
            let mut to_add = vec![(txid, false)];
            while let Some((txid, parents_visited)) = to_add.pop() {
                if parents_visited {
                    let (_tx, _info) = self
                        .remove(&txid)?
                        .expect("missing tx in mempool when proposing txs");
                    res.insert(txid);
                } else {
                    let (_tx, info) = &self.txs.0[&txid];
                    to_add.extend(info.depends.iter().map(|dep| (*dep, false)));
                    to_add.push((txid, true))
                }
            }
            total_size += ancestor_fee_rate.size;
        }
        Ok(res)
    }

    pub fn propose_txs(
        &self,
    ) -> Result<Vec<BlockTemplateTransaction>, MempoolRemoveError> {
        let mut txs = self.clone().propose_txs_mut()?;
        let mut res = Vec::new();
        // build result in reverse order
        while let Some(txid) = txs.pop() {
            let mut depends = Vec::new();
            let mut ancestors = self.txs.ancestors(txid);
            while let Some((anc_txid, _, _)) = ancestors.next().transpose()? {
                let anc_idx = txs
                    .get_index_of(&anc_txid)
                    .expect("missing dependency in proposal txs");
                depends.push(anc_idx as u32);
            }
            depends.sort();
            let (tx, info) = &self.txs.0[&txid];
            let block_template_tx = BlockTemplateTransaction {
                data: bitcoin::consensus::serialize(tx),
                txid,
                hash: tx.compute_wtxid(),
                depends,
                fee: info.fees.base as i64,
                // FIXME: compute this
                sigops: None,
                weight: tx.weight().to_wu(),
            };
            res.push(block_template_tx);
        }
        res.reverse();
        Ok(res)
    }
}
