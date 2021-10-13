use std::cmp::min;
use std::pin::Pin;
use std::sync::mpsc;
use std::sync::mpsc::{Receiver, Sender};

use bytes::Bytes;
use futures::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, BufReader, BufWriter, ReadBuf};

use crate::server::connection::{AsyncReader, AsyncWriter, Connection};

#[allow(dead_code)]
impl Connection {
    pub(crate) fn with_channel() -> (Connection, Sender<Bytes>, Receiver<Bytes>) {
        let (input_sender, input_receiver) = mpsc::channel::<Bytes>();
        let (output_sender, output_receiver) = mpsc::channel::<Bytes>();
        let connection = Self {
            input: BufReader::new(Box::new(FakeTcpReader {
                input: input_receiver,
            })),
            output: BufWriter::new(Box::new(FakeTcpWriter {
                output: output_sender,
            })),
        };
        (connection, input_sender, output_receiver)
    }
}

// READ

// type FakeReadStream = Vec<u8>;

struct FakeTcpReader {
    input: Receiver<Bytes>,
}

impl AsyncRead for FakeTcpReader {
    fn poll_read(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let msg = self.input.recv().unwrap();
        let size: usize = min(msg.len(), buf.capacity());
        buf.put_slice(&msg[..size]);
        Poll::Ready(Ok(()))
    }
}

impl AsyncReader for FakeTcpReader {}

// WRITE

struct FakeTcpWriter {
    output: Sender<Bytes>,
}

impl AsyncWriter for FakeTcpWriter {}

impl AsyncWrite for FakeTcpWriter {
    fn poll_write(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::result::Result<usize, std::io::Error>> {
        let bs: Bytes = Bytes::from(buf.to_vec());
        self.output.send(bs).unwrap();
        return Poll::Ready(Ok(buf.len()));
    }
    fn poll_flush(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<std::result::Result<(), std::io::Error>> {
        Poll::Ready(Ok(()))
    }
    fn poll_shutdown(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<std::result::Result<(), std::io::Error>> {
        Poll::Ready(Ok(()))
    }
}
