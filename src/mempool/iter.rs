use std::{collections::{btree_map, HashSet}, ops::ControlFlow};

use bitcoin::{Transaction, Txid};
use fallible_iterator::FallibleIterator;
use ouroboros::self_referencing;
use thiserror::Error;

use super::{Mempool, MissingAncestorError, TxInfo};

type AncestorsItem<'a> = (&'a Transaction, &'a TxInfo);

#[self_referencing]
#[derive(Debug)]
struct AncestorsInner<'mempool> {
    inner: Option<Box<AncestorsInner<'mempool>>>,
    tx: &'mempool Transaction,
    info: &'mempool TxInfo,
    #[borrows(info)]
    #[covariant]
    depends: btree_map::Keys<'this, Txid, blake3::Hash>,
}

impl<'mempool> AncestorsInner<'mempool> {
    fn next(
        &mut self,
        mempool: &'mempool Mempool,
        ignore: &mut HashSet<Txid>,
    ) -> Result<Option<AncestorsItem<'mempool>>, MissingAncestorError> {
        if let ControlFlow::Break(res) = self.with_inner_mut(|inner_opt| {
            if let Some(ref mut inner) = *inner_opt {
                if let Some(res) = Self::next(inner, mempool, ignore)? {
                    Ok(ControlFlow::Break(res))
                } else {
                    let inner = inner_opt.take().unwrap().into_heads();
                    let res = (inner.tx, inner.info);
                    Ok(ControlFlow::Break(res))
                }
            } else {
                Ok(ControlFlow::Continue(()))
            }
        })? {
            return Ok(Some(res));
        };
        if let Some(next) =
            self.with_depends_mut(|depends| {
                depends.find(|dep| ignore.insert(**dep))
            }).copied()
        {
            let (tx, info) = &mempool.txs.get(&next).ok_or_else(|| MissingAncestorError{
                tx: self.with_tx(|tx| tx.compute_txid()),
                missing: next,
            })?;
            let inner = Self::new(None, tx, info, |info| info.depends.keys());
            self.with_inner_mut(|inner_opt| *inner_opt = Some(Box::new(inner)));
            self.next(mempool, ignore)
        } else {
            Ok(None)
        }
    }
}

/// Internal state of the `Ancestors` iterator
#[derive(Debug)]
enum AncestorsState<'mempool> {
    /// Newly initialized, not yet started
    NotStarted(Txid),
    Started {
        ignore: HashSet<Txid>,
        inner: AncestorsInner<'mempool>,
    },
    /// Completed
    Done,
}

/// Iterator over ancestors of a tx, including the specified tx last.
/// Descendant txs in the result occur after their ancestors.
pub struct Ancestors<'mempool> {
    mempool: &'mempool Mempool,
    state: AncestorsState<'mempool>
}

impl<'mempool> FallibleIterator for Ancestors<'mempool> {
    type Item = AncestorsItem<'mempool>;
    type Error = MissingAncestorError;

    fn next(&mut self) -> Result<Option<Self::Item>, Self::Error> {
        match self.state {
            AncestorsState::NotStarted(ref txid) => {
                let (tx, info) = &self.mempool.txs.get(txid).ok_or_else(|| MissingAncestorError {
                    tx: *txid,
                    missing: *txid
                })?;
                let inner =
                    AncestorsInner::new(None, tx, info, |info| info.depends.keys());
                self.state = AncestorsState::Started {
                    ignore: HashSet::new(),
                    inner,
                };
                self.next()
            }
            AncestorsState::Started{ ref mut ignore, ref mut inner } => {
                match inner.next(self.mempool, ignore)? {
                    Some(res) => Ok(Some(res)),
                    None => {
                        let res = inner.with(|fields| (*fields.tx, *fields.info));
                        self.state = AncestorsState::Done;
                        Ok(Some(res))
                    }
                }
            }
            AncestorsState::Done => {
                Ok(None)
            }
        }
    }
}

impl Mempool {
    pub fn ancestors(&self, txid: Txid) -> Ancestors {
        Ancestors {
            mempool: self,
            state: AncestorsState::NotStarted(txid),
        }
    }
}
