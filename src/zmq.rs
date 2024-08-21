use std::cmp::Ordering;

use bitcoin::{hashes::Hash as _, BlockHash, Txid};
use futures::{
    stream::{self, BoxStream},
    Stream, StreamExt, TryStreamExt as _,
};
use thiserror::Error;
use zeromq::{Socket as _, SocketRecv as _, ZmqError, ZmqMessage};

#[derive(Clone, Copy, Debug)]
pub enum SequenceMessage {
    BlockHashConnected(BlockHash),
    BlockHashDisconnected(BlockHash),
    /// Tx hash added to mempool
    TxHashAdded {
        txid: Txid,
        mempool_seq: u64,
    },
    /// Tx hash removed from mempool for non-block inclusion reason
    TxHashRemoved {
        txid: Txid,
        mempool_seq: u64,
    },
}

impl SequenceMessage {
    fn mempool_seq(&self) -> Option<u64> {
        match self {
            Self::TxHashAdded {
                txid: _,
                mempool_seq,
            }
            | Self::TxHashRemoved {
                txid: _,
                mempool_seq,
            } => Some(*mempool_seq),
            Self::BlockHashConnected(_) | Self::BlockHashDisconnected(_) => {
                None
            }
        }
    }
}

#[derive(Debug, Error)]
pub enum DeserializeSequenceMessageError {
    #[error("Missing hash (first 32 bytes)")]
    MissingHash,
    #[error("Missing mempool sequence (bytes [#34 - #40])")]
    MissingMempoolSequence,
    #[error("Missing message type (byte #33)")]
    MissingMessageType,
    #[error("Unknown message type: {0:x}")]
    UnknownMessageType(u8),
}

impl TryFrom<ZmqMessage> for SequenceMessage {
    type Error = DeserializeSequenceMessageError;

    fn try_from(msg: ZmqMessage) -> Result<Self, Self::Error> {
        let msg = &msg.into_vec()[0];
        let Some((hash, rest)) = msg.split_first_chunk() else {
            return Err(Self::Error::MissingHash);
        };
        let Some(([message_type], rest)) = rest.split_first_chunk() else {
            return Err(Self::Error::MissingMessageType);
        };
        match *message_type {
            b'C' => {
                Ok(Self::BlockHashConnected(BlockHash::from_byte_array(*hash)))
            }
            b'D' => Ok(Self::BlockHashDisconnected(
                BlockHash::from_byte_array(*hash),
            )),
            b'A' => {
                let Some((mempool_seq, _rest)) = rest.split_first_chunk()
                else {
                    return Err(Self::Error::MissingMempoolSequence);
                };
                Ok(Self::TxHashAdded {
                    txid: Txid::from_byte_array(*hash),
                    mempool_seq: u64::from_le_bytes(*mempool_seq),
                })
            }
            b'R' => {
                let Some((mempool_seq, _rest)) = rest.split_first_chunk()
                else {
                    return Err(Self::Error::MissingMempoolSequence);
                };
                Ok(Self::TxHashRemoved {
                    txid: Txid::from_byte_array(*hash),
                    mempool_seq: u64::from_le_bytes(*mempool_seq),
                })
            }
            message_type => Err(Self::Error::UnknownMessageType(message_type)),
        }
    }
}

#[derive(Debug, Error)]
pub enum SequenceStreamError {
    #[error("Error deserializing message")]
    Deserialize(#[from] DeserializeSequenceMessageError),
    #[error("Missing message with mempool sequence {0}")]
    MissingMessage(u64),
    #[error("ZMQ error")]
    Zmq(#[from] ZmqError),
}

pub struct SequenceStream<'a>(
    BoxStream<'a, Result<SequenceMessage, SequenceStreamError>>,
);

impl<'a> Stream for SequenceStream<'a> {
    type Item = Result<SequenceMessage, SequenceStreamError>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.get_mut().0.poll_next_unpin(cx)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.0.size_hint()
    }
}

#[tracing::instrument]
pub async fn subscribe_sequence(
    zmq_addr_rawblock: &str,
) -> Result<SequenceStream, ZmqError> {
    tracing::debug!("Attempting to connect to ZMQ server...");
    let mut socket = zeromq::SubSocket::new();
    socket.connect(zmq_addr_rawblock).await?;
    tracing::info!("Connected to ZMQ server");
    tracing::debug!("Attempting to subscribe to `rawblock` topic...");
    socket.subscribe("rawblock").await?;
    tracing::info!("Subscribed to `rawblock`");
    let inner = stream::try_unfold(socket, |mut socket| async {
        let msg: SequenceMessage = socket.recv().await?.try_into()?;
        Ok(Some((msg, socket)))
    })
    .try_filter_map({
        let mut next_mempool_seq: Option<u64> = None;
        move |sequence_msg| {
            let res = match (next_mempool_seq, sequence_msg.mempool_seq()) {
                (_, None) => Ok(Some(sequence_msg)),
                (None, Some(mempool_seq)) => {
                    next_mempool_seq = Some(mempool_seq + 1);
                    Ok(Some(sequence_msg))
                }
                (Some(ref mut next_mempool_seq), Some(mempool_seq)) => {
                    match mempool_seq.cmp(next_mempool_seq) {
                        Ordering::Less => Ok(None),
                        Ordering::Equal => {
                            *next_mempool_seq = mempool_seq + 1;
                            Ok(Some(sequence_msg))
                        }
                        Ordering::Greater => {
                            Err(SequenceStreamError::MissingMessage(
                                *next_mempool_seq,
                            ))
                        }
                    }
                }
            };
            async { res }
        }
    })
    .boxed();
    Ok(SequenceStream(inner))
}
