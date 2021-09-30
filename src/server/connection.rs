use bytes::Bytes;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;

use crate::frame::request::Request;
use crate::frame::response::Response;
use crate::server::error::Result;

pub trait AsyncReader: AsyncRead + Unpin + Send {}
impl AsyncReader for OwnedReadHalf {}

pub trait AsyncWriter: AsyncWrite + Unpin + Send {}
impl AsyncWriter for OwnedWriteHalf {}

pub struct Connection {
    input: BufReader<Box<dyn AsyncReader>>,
    output: BufWriter<Box<dyn AsyncWriter>>,
}

impl Connection {
    /// Create a new `Connection` backed by `socket`, with read and write buffers initialized.
    pub fn new(socket: TcpStream) -> Connection {
        let (r, w) = socket.into_split();
        let input = BufReader::new(Box::new(r) as Box<dyn AsyncReader>);
        let output = BufWriter::new(Box::new(w) as Box<dyn AsyncWriter>);
        Self { input, output }
    }

    /// Read a `Request` from the socket
    pub async fn read(&mut self) -> Result<Request> {
        // TODO use self.buf (BytesMute w/ Cursor) here instead of allocating new buf?
        let mut buf = Vec::new();
        self.input.read_until("\n".as_bytes()[0], &mut buf).await?;
        Ok(buf.into())
    }

    /// Write a `Response` to the socket
    pub async fn write(&mut self, response: Response) -> Result<()> {
        let bs: Bytes = response.into();
        self.output.write_all(&bs).await?;
        self.output.flush().await?;
        Ok(())
    }
}

#[cfg(test)]
mod connection_tests {
    use std::cmp::min;
    use std::pin::Pin;
    use std::sync::{Arc, Mutex};

    use bytes::BufMut;
    use futures::task::{Context, Poll};
    use tokio::io::{AsyncWrite, ReadBuf};

    use super::*;

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

    impl Connection {
        // TODO: provide an impl with channels to test multiple reads/writes
        fn from_fake_input(input: Vec<u8>) -> Connection {
            Self {
                input: BufReader::new(FakeTcpReader::boxed_from_fake_input(input)),
                output: BufWriter::new(FakeTcpWriter::boxed_new()),
            }
        }
        fn with_fake_output() -> (Connection, FakeWriteStream) {
            let output = FakeTcpWriter::new().output;
            let connection = Self {
                input: BufReader::new(FakeTcpReader::boxed_new()),
                output: BufWriter::new(FakeTcpWriter::boxed_from_fake_output(output.clone())),
            };
            (connection, output)
        }
    }

    #[tokio::test]
    async fn reads() {
        let request = [r#"{"id":42,"type":"Get","key":"foo"}"#, "\n"]
            .concat()
            .into();
        let expected_read = Request::Get {
            id: 42,
            key: "foo".to_string(),
        };

        let mut connection = Connection::from_fake_input(request);
        let actual_read = connection.read().await.unwrap();

        assert_eq!(actual_read, expected_read);
    }

    #[tokio::test]
    async fn writes() {
        let response = Response::ToGet {
            id: 42,
            value: Some("foo".to_string()),
        };
        let expected_write: Vec<u8> = [r#"{"id":42,"value":"foo"}"#, "\n"].concat().into();

        let (mut connection, output) = Connection::with_fake_output();
        let _ = connection.write(response).await;

        let actual_write: Vec<u8> = output.clone().lock().unwrap().to_vec();
        assert_eq!(actual_write, expected_write);
    }
}
