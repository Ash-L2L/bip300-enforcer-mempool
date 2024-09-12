use std::collections::VecDeque;

use bip300301::client::{BlockTemplateTransaction, RawMempoolTxFees};
use bitcoin::{BlockHash, Target, Transaction, Txid, Weight};
use hashlink::{LinkedHashMap, LinkedHashSet};
use imbl::{ordmap, OrdMap, OrdSet};
use indexmap::IndexSet;
use lending_iterator::LendingIterator as _;
use thiserror::Error;

pub mod iter;
pub mod iter_mut;
mod sync;

pub use sync::{init_sync_mempool, MempoolSync};

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

#[allow(clippy::enum_variant_names)]
#[derive(Debug, Error)]
pub enum MempoolInsertError {
    #[error(transparent)]
    MissingAncestor(#[from] MissingAncestorError),
    #[error(transparent)]
    MissingDescendant(#[from] MissingDescendantError),
    #[error(transparent)]
    MissingDescendantsKey(#[from] MissingDescendantsKeyError),
}

#[derive(Debug, Error)]
#[error("Missing by_ancestor_fee_rate key: {0:?}")]
pub struct MissingByAncestorFeeRateKeyError(FeeRate);

#[allow(clippy::enum_variant_names)]
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

/// Map of txs (which may not be in the mempool) to their direct child txs,
/// which MUST be in the mempool
#[derive(Clone, Debug, Default)]
struct TxChilds(imbl::HashMap<Txid, imbl::HashSet<Txid>>);

impl TxChilds {
    fn insert(&mut self, txid: Txid, child: Txid) -> bool {
        self.0.entry(txid).or_default().insert(child).is_some()
    }

    fn remove(&mut self, txid: Txid, child: Txid) -> bool {
        match self.0.entry(txid) {
            imbl::hashmap::Entry::Occupied(mut entry) => {
                let res = entry.get_mut().remove(&child).is_some();
                if entry.get().is_empty() {
                    entry.remove();
                }
                res
            }
            imbl::hashmap::Entry::Vacant(_) => false,
        }
    }
}

#[derive(Clone, Debug, Default)]
struct MempoolTxs(imbl::HashMap<Txid, (Transaction, TxInfo)>);

// MUST be cheap to clone so that constructing block templates is cheap
#[derive(Clone, Debug)]
pub struct Mempool {
    by_ancestor_fee_rate: ByAncestorFeeRate,
    chain: Chain,
    /// Map of txs (which may not be in the mempool) to their direct child txs,
    /// which MUST be in the mempool
    tx_childs: TxChilds,
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
            tx_childs: TxChilds::default(),
            txs: MempoolTxs::default(),
        }
    }

    pub fn tip(&self) -> &bip300301::client::Block {
        &self.chain.blocks[&self.chain.tip]
    }

    pub fn next_target(&self) -> Target {
        // FIXME: calculate this properly
        self.chain.blocks[&self.chain.tip].compact_target.into()
    }

    /// Insert a tx into the mempool
    fn insert(
        &mut self,
        tx: Transaction,
        fee: u64,
    ) -> Result<Option<TxInfo>, MempoolInsertError> {
        let txid = tx.compute_txid();
        // initially incorrect, must be computed after insertion
        let mut ancestor_fees = fee;
        // initially incorrect, must be computed after insertion
        let mut descendant_fees = fee;
        let modified_fee = fee;
        let vsize = tx.vsize() as u64;
        // initially incorrect, must be computed after insertion
        let mut ancestor_size = vsize;
        // initially incorrect, must be computed after insertion
        let mut descendant_size = vsize;
        let depends = tx
            .input
            .iter()
            .map(|input| {
                let input_txid = input.previous_output.txid;
                self.tx_childs.insert(input_txid, txid);
                input_txid
            })
            .filter(|input_txid| self.txs.0.contains_key(input_txid))
            .collect();
        let spent_by = if let Some(childs) = self.tx_childs.0.get(&txid) {
            OrdSet::from_iter(childs.iter().copied())
        } else {
            OrdSet::new()
        };
        let info = TxInfo {
            ancestor_size,
            bip125_replaceable: tx.is_explicitly_rbf(),
            depends,
            descendant_size,
            fees: RawMempoolTxFees {
                ancestor: ancestor_fees,
                base: fee,
                descendant: descendant_fees,
                modified: modified_fee,
            },
            spent_by,
        };
        let (ndeps, nspenders) = (info.depends.len(), info.spent_by.len());
        let res = self.txs.0.insert(txid, (tx, info)).map(|(_, info)| info);
        // FIXME: remove
        tracing::debug!("Inserted {txid} to mempool with {ndeps} deps and {nspenders} spenders");
        self.txs.ancestors_mut(txid).try_for_each(|ancestor_info| {
            let (ancestor_tx, ancestor_info) = ancestor_info?;
            ancestor_size += ancestor_tx.vsize() as u64;
            ancestor_fees += ancestor_info.fees.modified;
            ancestor_info.descendant_size += vsize;
            ancestor_info.fees.descendant += modified_fee;
            Result::<_, MempoolInsertError>::Ok(())
        })?;
        self.txs.descendants_mut(txid).skip(1).try_for_each(
            |descendant_info| {
                let (descendant_tx, descendant_info) = descendant_info?;
                descendant_size += descendant_tx.vsize() as u64;
                descendant_fees += descendant_info.fees.modified;
                descendant_info.ancestor_size += vsize;
                descendant_info.fees.ancestor += modified_fee;
                Result::<_, MempoolInsertError>::Ok(())
            },
        )?;
        let (_, info) = self.txs.0.get_mut(&txid).unwrap();
        info.fees.ancestor = ancestor_fees;
        info.fees.descendant = descendant_fees;
        info.ancestor_size = ancestor_size;
        info.descendant_size = descendant_size;
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
        let Some((tx, info)) = self.txs.0.get(txid) else {
            return Ok(None);
        };
        let ancestor_size = info.ancestor_size;
        let vsize = tx.vsize() as u64;
        let fees = RawMempoolTxFees { ..info.fees };
        for spent_tx in tx.input.iter().map(|input| input.previous_output.txid)
        {
            self.tx_childs.remove(spent_tx, *txid);
        }
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
            // FIXME: remove
            tracing::debug!("removing {txid} as a dep of {desc_txid}");
            desc_info.depends.remove(txid);
            Result::<_, MempoolRemoveError>::Ok(())
        })?;
        // Update all ancestors
        let () = self.txs.ancestors_mut(*txid).try_for_each(|anc| {
            let (anc_tx, anc_info) = anc?;
            anc_info.descendant_size -= vsize;
            anc_info.fees.descendant -= fees.modified;
            let anc_txid = anc_tx.compute_txid();
            // FIXME: remove
            tracing::debug!("removing {txid} as a spender of {anc_txid}");
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
        let res = self.txs.0.remove(txid);
        // FIXME: remove
        tracing::debug!("Removed {txid} from mempool");
        Ok(res)
    }

    /// Remove a tx from mempool, and all descendants.
    /// Returns the removed tx and descendants.
    fn remove_with_descendants(
        &mut self,
        txid: &Txid,
    ) -> Result<LinkedHashMap<Txid, Transaction>, MempoolRemoveError> {
        let mut res = LinkedHashMap::new();
        let mut remove_stack = VecDeque::from_iter([*txid]);
        while let Some(next) = remove_stack.pop_front() {
            let Some((tx, tx_info)) = self.remove(&next)? else {
                continue;
            };
            remove_stack.extend(tx_info.spent_by);
            res.replace(next, tx);
        }
        Ok(res)
    }

    /// Retain txs for which the provided closure returns `true`.
    /// This function also deletes descendants of any deleted tx.
    /// Returns the removed txs.
    fn try_filter<F, E>(
        &mut self,
        mut f: F,
    ) -> Result<
        LinkedHashMap<Txid, Transaction>,
        either::Either<MempoolRemoveError, E>,
    >
    where
        F: FnMut(&Transaction) -> Result<bool, E>,
    {
        let no_ancestors_txids: Vec<Txid> = self
            .txs
            .0
            .iter()
            .filter_map(|(txid, (_tx, tx_info))| {
                if tx_info.depends.is_empty() {
                    Some(*txid)
                } else {
                    None
                }
            })
            .collect();
        let mut res = LinkedHashMap::new();
        for txid in no_ancestors_txids {
            let mut descendants = Vec::<Txid>::new();
            let () = self
                .txs
                .descendants_mut(txid)
                .try_for_each(|item| {
                    let (tx, _info) = item?;
                    let descendant_txid = tx.compute_txid();
                    descendants.push(descendant_txid);
                    Result::<_, MempoolRemoveError>::Ok(())
                })
                .map_err(either::Either::Left)?;
            'descs: for descendant_txid in descendants {
                let Some((tx, _info)) = self.txs.0.get(&descendant_txid) else {
                    continue 'descs;
                };
                if !f(tx).map_err(either::Either::Right)? {
                    res.extend(
                        self.remove_with_descendants(&descendant_txid)
                            .map_err(either::Either::Left)?,
                    );
                }
            }
        }
        Ok(res)
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
