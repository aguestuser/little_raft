use std::cmp::min;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use crate::server::connection::{AsyncReader, AsyncWriter, Connection};
use bytes::BufMut;
use futures::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

#[allow(dead_code)]
impl Connection {
    // TODO: provide an impl with channels to test multiple reads/writes
    pub(crate) fn from_fake_input(input: Vec<u8>) -> Connection {
        Connection::from_halves(
            FakeTcpReader::boxed_from_fake_input(input),
            FakeTcpWriter::boxed_new(),
        )
    }
    pub(crate) fn with_fake_output() -> (Connection, FakeWriteStream) {
        let output = FakeTcpWriter::new().output;
        let connection = Connection::from_halves(
            FakeTcpReader::boxed_new(),
            FakeTcpWriter::boxed_from_fake_output(output.clone()),
        );
        (connection, output)
    }
}

// READ

type FakeReadStream = Vec<u8>;

struct FakeTcpReader {
    input: FakeReadStream,
}

impl FakeTcpReader {
    fn new() -> FakeTcpReader {
        Self { input: Vec::new() }
    }
    fn boxed_new() -> Box<dyn AsyncReader> {
        Box::new(Self::new())
    }
    fn from_fake_input(input: Vec<u8>) -> FakeTcpReader {
        Self { input }
    }
    fn boxed_from_fake_input(input: Vec<u8>) -> Box<dyn AsyncReader> {
        Box::new(Self::from_fake_input(input))
    }
}

impl AsyncRead for FakeTcpReader {
    fn poll_read(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let size: usize = min(self.input.len(), buf.capacity());
        buf.put_slice(&self.input[..size]);
        Poll::Ready(Ok(()))
    }
}

impl AsyncReader for FakeTcpReader {}

// WRITE

type FakeWriteStream = Arc<Mutex<Vec<u8>>>;

struct FakeTcpWriter {
    output: FakeWriteStream,
}

impl FakeTcpWriter {
    fn new() -> FakeTcpWriter {
        Self {
            output: Arc::new(Mutex::new(Vec::new())),
        }
    }
    fn boxed_new() -> Box<dyn AsyncWriter> {
        Box::new(Self::new())
    }
    fn from_fake_output(output: Arc<Mutex<Vec<u8>>>) -> FakeTcpWriter {
        Self { output }
    }
    fn boxed_from_fake_output(output: Arc<Mutex<Vec<u8>>>) -> Box<dyn AsyncWriter> {
        Box::new(Self::from_fake_output(output))
    }
}

impl AsyncWriter for FakeTcpWriter {}

impl AsyncWrite for FakeTcpWriter {
    fn poll_write(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::result::Result<usize, std::io::Error>> {
        // self.write_data = Vec::from(buf);
        let mut wd = self.output.lock().unwrap();
        wd.put_slice(buf);
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
