use std::{borrow::Borrow, collections::HashMap, convert::Infallible};

use bitcoin::{Transaction, Txid};
use either::Either;

pub trait CusfEnforcer {
    type AcceptTxError: std::error::Error + Send + Sync + 'static;

    /// Return `true` to accept the tx, or `false` to reject it.
    /// Inputs to a tx are always available.
    fn accept_tx<TxRef>(
        &mut self,
        tx: &Transaction,
        tx_inputs: &HashMap<Txid, TxRef>,
    ) -> Result<bool, Self::AcceptTxError>
    where
        TxRef: Borrow<Transaction>;
}

/// Compose two [`CusfEnforcer`]s, left-before-right
#[derive(Debug, Default)]
pub struct Compose<C0, C1>(C0, C1);

impl<C0, C1> CusfEnforcer for Compose<C0, C1>
where
    C0: CusfEnforcer,
    C1: CusfEnforcer,
{
    type AcceptTxError = Either<C0::AcceptTxError, C1::AcceptTxError>;

    fn accept_tx<TxRef>(
        &mut self,
        tx: &Transaction,
        tx_inputs: &HashMap<Txid, TxRef>,
    ) -> Result<bool, Self::AcceptTxError>
    where
        TxRef: Borrow<Transaction>,
    {
        if self.0.accept_tx(tx, tx_inputs).map_err(Either::Left)? {
            self.1.accept_tx(tx, tx_inputs).map_err(Either::Right)
        } else {
            Ok(false)
        }
    }
}

#[derive(Clone, Copy, Debug)]

pub struct DefaultEnforcer;

impl CusfEnforcer for DefaultEnforcer {
    type AcceptTxError = Infallible;

    fn accept_tx<TxRef>(
        &mut self,
        _tx: &Transaction,
        _tx_inputs: &HashMap<Txid, TxRef>,
    ) -> Result<bool, Self::AcceptTxError>
    where
        TxRef: Borrow<Transaction>,
    {
        Ok(true)
    }
}
