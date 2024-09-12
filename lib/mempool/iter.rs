use std::collections::HashSet;

use bitcoin::{Transaction, Txid};
use lending_iterator::prelude::*;

use super::{MempoolTxs, MissingAncestorError, TxInfo};

type AncestorsItem<'a> = (Txid, &'a Transaction, &'a TxInfo);

/// Iterator over ancestors, NOT including the specified txid, where ancestors
/// occur before descendants
pub struct Ancestors<'mempool_txs> {
    mempool_txs: &'mempool_txs MempoolTxs,
    /// `bool` indicates whether all of the tx's parents have been visited
    to_visit: Vec<(Txid, bool)>,
    visited: HashSet<Txid>,
}

impl<'mempool_txs> Ancestors<'mempool_txs> {
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
            let (tx, info) =
                self.mempool_txs.0.get(&txid).ok_or(MissingAncestorError {
                    tx: txid,
                    missing: txid,
                })?;
            Ok(Some((txid, tx, info)))
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
impl<'mempool_txs> LendingIterator for Ancestors<'mempool_txs> {
    type Item<'next>
    where
        Self: 'next,
    = Result<AncestorsItem<'next>, MissingAncestorError>;

    fn next<'next>(
        self: &'next mut Ancestors<'mempool_txs>,
    ) -> Option<Result<AncestorsItem<'next>, MissingAncestorError>> {
        Self::next(self).transpose()
    }
}

impl MempoolTxs {
    /// Iterator over ancestors, NOT including the specified txid, where ancestors
    /// occur before descendants
    pub fn ancestors(&self, txid: Txid) -> Ancestors<'_> {
        Ancestors {
            mempool_txs: self,
            to_visit: vec![(txid, false)],
            visited: HashSet::new(),
        }
    }
}
