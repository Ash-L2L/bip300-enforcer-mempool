use bitcoin::Transaction;
use either::Either;

pub trait CusfEnforcer {
    type AcceptTxError: std::error::Error;

    /// Return `true` to accept the tx, or `false` to reject it
    fn accept_tx(
        &mut self,
        tx: &Transaction,
    ) -> Result<bool, Self::AcceptTxError>;
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

    fn accept_tx(
        &mut self,
        tx: &Transaction,
    ) -> Result<bool, Self::AcceptTxError> {
        if self.0.accept_tx(tx).map_err(Either::Left)? {
            self.1.accept_tx(tx).map_err(Either::Right)
        } else {
            Ok(false)
        }
    }
}
