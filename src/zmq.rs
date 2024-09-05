use std::cmp::Ordering;

use bitcoin::{hashes::Hash as _, hex::DisplayHex as _, BlockHash, Txid};
use futures::{
    stream::{self, BoxStream},
    Stream, StreamExt, TryStreamExt as _,
};
use thiserror::Error;
use zeromq::{Socket as _, SocketRecv as _, ZmqError, ZmqMessage};

#[derive(Clone, Copy, Debug)]
pub enum SequenceMessage {
    BlockHashConnected(BlockHash, u32),
    BlockHashDisconnected(BlockHash, u32),
    /// Tx hash added to mempool
    TxHashAdded {
        txid: Txid,
        mempool_seq: u64,
        zmq_seq: u32,
    },
    /// Tx hash removed from mempool for non-block inclusion reason
    TxHashRemoved {
        txid: Txid,
        mempool_seq: u64,
        zmq_seq: u32,
    },
}

impl SequenceMessage {
    fn mempool_seq(&self) -> Option<u64> {
        match self {
            Self::TxHashAdded { mempool_seq, .. }
            | Self::TxHashRemoved { mempool_seq, .. } => Some(*mempool_seq),
            Self::BlockHashConnected(_, _)
            | Self::BlockHashDisconnected(_, _) => None,
        }
    }

    fn zmq_seq(&self) -> u32 {
        match self {
            Self::TxHashAdded { zmq_seq, .. }
            | Self::TxHashRemoved { zmq_seq, .. }
            | Self::BlockHashConnected(_, zmq_seq)
            | Self::BlockHashDisconnected(_, zmq_seq) => *zmq_seq,
        }
    }
}

#[derive(Debug, Error)]
pub enum DeserializeSequenceMessageError {
    #[error("Missing hash (frame 1 bytes at index [0-31])")]
    MissingHash,
    #[error("Missing mempool sequence (frame 1 bytes at index [#33 - #40])")]
    MissingMempoolSequence,
    #[error("Missing message type (frame 1 index 32)")]
    MissingMessageType,
    #[error("Missing `sequence` prefix (frame 0 first 8 bytes)")]
    MissingPrefix,
    #[error("Missing ZMQ sequence (frame 2 first 4 bytes)")]
    MissingZmqSequence,
    #[error("Unknown message type: {0:x}")]
    UnknownMessageType(u8),
}

impl TryFrom<ZmqMessage> for SequenceMessage {
    type Error = DeserializeSequenceMessageError;

    fn try_from(msg: ZmqMessage) -> Result<Self, Self::Error> {
        let msgs = &msg.into_vec();
        let Some(b"sequence") = msgs.get(0).map(|msg| &**msg) else {
            return Err(Self::Error::MissingPrefix);
        };
        let Some((hash, rest)) =
            msgs.get(1).and_then(|msg| msg.split_first_chunk())
        else {
            return Err(Self::Error::MissingHash);
        };
        let mut hash = *hash;
        hash.reverse();
        let Some(([message_type], rest)) = rest.split_first_chunk() else {
            return Err(Self::Error::MissingMessageType);
        };
        let Some((zmq_seq, _rest)) =
            msgs.get(2).and_then(|msg| msg.split_first_chunk())
        else {
            return Err(Self::Error::MissingZmqSequence);
        };
        let zmq_seq = u32::from_le_bytes(*zmq_seq);
        let res = match *message_type {
            b'C' => Self::BlockHashConnected(
                BlockHash::from_byte_array(hash),
                zmq_seq,
            ),
            b'D' => Self::BlockHashDisconnected(
                BlockHash::from_byte_array(hash),
                zmq_seq,
            ),
            b'A' => {
                let Some((mempool_seq, _rest)) = rest.split_first_chunk()
                else {
                    return Err(Self::Error::MissingMempoolSequence);
                };
                Self::TxHashAdded {
                    txid: Txid::from_byte_array(hash),
                    mempool_seq: u64::from_le_bytes(*mempool_seq),
                    zmq_seq,
                }
            }
            b'R' => {
                let Some((mempool_seq, _rest)) = rest.split_first_chunk()
                else {
                    return Err(Self::Error::MissingMempoolSequence);
                };
                SequenceMessage::TxHashRemoved {
                    txid: Txid::from_byte_array(hash),
                    mempool_seq: u64::from_le_bytes(*mempool_seq),
                    zmq_seq,
                }
            }
            message_type => {
                return Err(Self::Error::UnknownMessageType(message_type))
            }
        };
        Ok(res)
    }
}

#[derive(Debug, Error)]
pub enum SequenceStreamError {
    #[error("Error deserializing message")]
    Deserialize(#[from] DeserializeSequenceMessageError),
    #[error("Missing message with mempool sequence {0}")]
    MissingMempoolSequence(u64),
    #[error("Missing message with zmq sequence {0}")]
    MissingZmqSequence(u32),
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
    tracing::debug!("Attempting to subscribe to `sequence` topic...");
    socket.subscribe("sequence").await?;
    tracing::info!("Subscribed to `sequence`");
    let inner = stream::try_unfold(socket, |mut socket| async {
        let msg: SequenceMessage = socket.recv().await?.try_into()?;
        Ok(Some((msg, socket)))
    })
    .try_filter_map({
        let mut next_mempool_seq: Option<u64> = None;
        let mut next_zmq_seq: Option<u32> = None;
        move |sequence_msg| {
            let (mempool_seq, zmq_seq) = match sequence_msg {
                SequenceMessage::BlockHashConnected(_, zmq_seq)
                | SequenceMessage::BlockHashDisconnected(_, zmq_seq) => {
                    next_mempool_seq = None;
                    (None, zmq_seq)
                }
                SequenceMessage::TxHashAdded {
                    txid: _,
                    mempool_seq,
                    zmq_seq,
                }
                | SequenceMessage::TxHashRemoved {
                    txid: _,
                    mempool_seq,
                    zmq_seq,
                } => (Some(mempool_seq), zmq_seq),
            };
            let res = 'res: {
                match &mut next_zmq_seq {
                    None => {
                        next_zmq_seq = Some(zmq_seq + 1);
                    }
                    Some(next_zmq_seq) => {
                        if zmq_seq + 1 == *next_zmq_seq {
                            // Ignore duplicates
                            break 'res Ok(None);
                        } else if zmq_seq != *next_zmq_seq {
                            break 'res Err(
                                SequenceStreamError::MissingZmqSequence(
                                    *next_zmq_seq,
                                ),
                            );
                        } else {
                            *next_zmq_seq = zmq_seq + 1;
                        }
                    }
                }
                let Some(mempool_seq) = mempool_seq else {
                    break 'res Ok(Some(sequence_msg));
                };
                match &mut next_mempool_seq {
                    None => {
                        next_mempool_seq = Some(mempool_seq + 1);
                    }
                    Some(next_mempool_seq) => {
                        if mempool_seq + 1 == *next_mempool_seq {
                            // Ignore duplicates
                            break 'res Ok(None);
                        } else if mempool_seq != *next_mempool_seq {
                            break 'res Err(
                                SequenceStreamError::MissingMempoolSequence(
                                    *next_mempool_seq,
                                ),
                            );
                        } else {
                            *next_mempool_seq = mempool_seq + 1;
                        }
                    }
                }
                Ok(Some(sequence_msg))
            };
            async { res }
        }
    })
    .boxed();
    Ok(SequenceStream(inner))
}
