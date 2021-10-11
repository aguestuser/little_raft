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

    pub fn from_halves(input: Box<dyn AsyncReader>, output: Box<dyn AsyncWriter>) -> Connection {
        Self {
            input: BufReader::new(input),
            output: BufWriter::new(output),
        }
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
