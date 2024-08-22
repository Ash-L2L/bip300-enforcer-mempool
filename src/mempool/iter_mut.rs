use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};

use bitcoin::{Transaction, Txid};
use lending_iterator::prelude::*;

use super::{MempoolTxs, MissingAncestorError, MissingDescendantError, TxInfo};

type AncestorsItem<'a> = (&'a Transaction, &'a mut TxInfo);

/// Iterator over ancestors, NOT including the specified txid, where ancestors
/// occur before descendants
pub struct AncestorsMut<'mempool_txs> {
    mempool_txs: &'mempool_txs mut MempoolTxs,
    /// `bool` indicates whether all of the tx's parents have been visited
    to_visit: Vec<(Txid, bool)>,
    visited: HashSet<Txid>,
}

impl<'mempool_txs> AncestorsMut<'mempool_txs> {
    fn next(
        &mut self,
    ) -> Result<Option<AncestorsItem<'_>>, MissingAncestorError> {
        let Some((txid, parents_visited)) = self.to_visit.pop() else {
            return Ok(None);
        };
        if parents_visited {
            // If this is the last item, ignore it
            if self.to_visit.is_empty() {
                return Ok(None);
            }
            let (tx, info) = self.mempool_txs.0.get_mut(&txid).ok_or(
                MissingAncestorError {
                    tx: txid,
                    missing: txid,
                },
            )?;
            Ok(Some((tx, info)))
        } else {
            let (_, info) =
                self.mempool_txs.0.get(&txid).ok_or(MissingAncestorError {
                    tx: txid,
                    missing: txid,
                })?;
            self.to_visit.push((txid, true));
            self.to_visit
                .extend(info.depends.iter().copied().filter_map(|dep| {
                    if self.visited.insert(dep) {
                        Some((dep, false))
                    } else {
                        None
                    }
                }));
            self.next()
        }
    }
}

#[gat]
impl<'mempool_txs> LendingIterator for AncestorsMut<'mempool_txs> {
    type Item<'next>
    where
        Self: 'next,
    = Result<AncestorsItem<'next>, MissingAncestorError>;

    fn next<'next>(
        self: &'next mut AncestorsMut<'mempool_txs>,
    ) -> Option<Result<AncestorsItem<'next>, MissingAncestorError>> {
        Self::next(self).transpose()
    }
}

type DescendantsItem<'a> = (&'a Transaction, &'a mut TxInfo);

/// Iterator over descendants, including the specified txid, where ancestors
/// occur before descendants
pub struct DescendantsMut<'mempool_txs> {
    mempool_txs: &'mempool_txs mut MempoolTxs,
    to_visit: VecDeque<Txid>,
    visited: HashSet<Txid>,
}

impl<'mempool_txs> DescendantsMut<'mempool_txs> {
    fn next(
        &mut self,
    ) -> Result<Option<DescendantsItem<'_>>, MissingDescendantError> {
        let Some(txid) = self.to_visit.pop_front() else {
            return Ok(None);
        };
        let (tx, info) = self.mempool_txs.0.get_mut(&txid).ok_or(
            MissingDescendantError {
                tx: txid,
                missing: txid,
            },
        )?;
        self.to_visit.extend(
            info.spent_by
                .iter()
                .copied()
                .filter(|spender| self.visited.insert(*spender)),
        );
        Ok(Some((tx, info)))
    }
}

#[gat]
impl<'mempool_txs> LendingIterator for DescendantsMut<'mempool_txs> {
    type Item<'next>
    where
        Self: 'next,
    = Result<DescendantsItem<'next>, MissingDescendantError>;

    fn next<'next>(
        self: &'next mut DescendantsMut<'mempool_txs>,
    ) -> Option<Result<DescendantsItem<'next>, MissingDescendantError>> {
        Self::next(self).transpose()
    }
}

impl MempoolTxs {
    /// Iterator over ancestors, NOT including the specified txid, where ancestors
    /// occur before descendants
    pub fn ancestors_mut(&mut self, txid: Txid) -> AncestorsMut<'_> {
        AncestorsMut {
            mempool_txs: self,
            to_visit: vec![(txid, false)],
            visited: HashSet::new(),
        }
    }

    /// Iterator over descendants, including the specified txid, where ancestors
    /// occur before descendants
    pub fn descendants_mut(&mut self, txid: Txid) -> DescendantsMut<'_> {
        DescendantsMut {
            mempool_txs: self,
            to_visit: VecDeque::from([txid]),
            visited: HashSet::new(),
        }
    }
}
