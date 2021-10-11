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
    pub(crate) input: BufReader<Box<dyn AsyncReader>>,
    pub(crate) output: BufWriter<Box<dyn AsyncWriter>>,
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
    use super::*;

    #[tokio::test]
    async fn reads() {
        let request: Bytes = [r#"{"id":42,"type":"Get","key":"foo"}"#, "\n"]
            .concat()
            .into();

        let (mut connection, input_sender, _) = Connection::with_channel();
        input_sender.send(request).unwrap();

        let actual_read = connection.read().await.unwrap();
        let expected_read = Request::Get {
            id: 42,
            key: "foo".to_string(),
        };
        assert_eq!(actual_read, expected_read);
    }

    #[tokio::test]
    async fn writes() {
        let response = Response::ToGet {
            id: 42,
            value: Some("foo".to_string()),
        };

        let (mut connection, _, output_receiver) = Connection::with_channel();
        let _ = connection.write(response).await;

        let actual_write: Bytes = output_receiver.recv().unwrap();
        let expected_write: Bytes = [r#"{"id":42,"value":"foo"}"#, "\n"].concat().into();
        assert_eq!(actual_write, expected_write);
    }
}
