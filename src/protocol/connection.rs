use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;

use crate::error;
use crate::protocol::request::Request;
use crate::protocol::response::Response;
use crate::protocol::NEWLINE;

pub trait AsyncReader: AsyncRead + Unpin + Send {}
impl AsyncReader for OwnedReadHalf {}

pub trait AsyncWriter: AsyncWrite + Unpin + Send {}
impl AsyncWriter for OwnedWriteHalf {}

pub struct ClientConnection {
    pub input: BufReader<Box<dyn AsyncReader>>,
    pub output: BufWriter<Box<dyn AsyncWriter>>,
}

impl ClientConnection {
    /// Create a new `Connection` backed by `socket`, with read and write buffers initialized.
    pub fn new(socket: TcpStream) -> ClientConnection {
        let (r, w) = socket.into_split();
        let input = BufReader::new(Box::new(r) as Box<dyn AsyncReader>);
        let output = BufWriter::new(Box::new(w) as Box<dyn AsyncWriter>);
        Self { input, output }
    }

    /// Read a `Request` from the socket
    pub async fn read(&mut self) -> error::Result<Response> {
        // TODO use self.buf (BytesMute w/ Cursor) here instead of allocating new buf?
        let mut buf = Vec::new();
        self.input.read_until(NEWLINE, &mut buf).await?;
        Ok(buf.into())
    }

    /// Write a `Response` to the socket
    pub async fn write(&mut self, req: &Request) -> error::Result<()> {
        let bytes: Vec<u8> = req.clone().into();
        self.output.write_all(&bytes).await?;
        self.output.write(&[NEWLINE]).await?;
        self.output.flush().await?;
        Ok(())
    }
}

pub struct ServerConnection {
    pub input: BufReader<Box<dyn AsyncReader>>,
    pub output: BufWriter<Box<dyn AsyncWriter>>,
}

impl ServerConnection {
    /// Create a new `Connection` backed by `socket`, with read and write buffers initialized.
    pub fn new(socket: TcpStream) -> ServerConnection {
        let (r, w) = socket.into_split();
        let input = BufReader::new(Box::new(r) as Box<dyn AsyncReader>);
        let output = BufWriter::new(Box::new(w) as Box<dyn AsyncWriter>);
        Self { input, output }
    }

    /// Read a `Request` from the socket
    pub async fn read(&mut self) -> error::Result<Request> {
        // TODO use self.buf (BytesMute w/ Cursor) here instead of allocating new buf?
        let mut buf = Vec::new();
        self.input.read_until(NEWLINE, &mut buf).await?;
        Ok(buf.into())
    }

    /// Write a `Response` to the socket
    pub async fn write(&mut self, req: &Response) -> error::Result<()> {
        let bytes: Vec<u8> = req.clone().into();
        self.output.write_all(&bytes).await?;
        self.output.write(&[NEWLINE]).await?;
        self.output.flush().await?;
        Ok(())
    }
}

#[cfg(test)]
mod connection_tests {
    use crate::protocol::connection::ClientConnection;

    use super::*;

    #[tokio::test]
    async fn client_reads() {
        let response_bytes: Vec<u8> = [r#"{"type":"ToGet","id":42,"value":"bar"}"#, "\n"]
            .concat()
            .into();
        let response = Response::ToGet {
            id: 42,
            value: Some("bar".to_string()),
        };

        let (mut connection, input_sender, _) = ClientConnection::with_channel();
        input_sender.send(response_bytes.clone()).unwrap();

        assert_eq!(connection.read().await.unwrap(), response);
    }

    #[tokio::test]
    async fn client_writes() {
        let request = Request::Get {
            id: 42,
            key: "foo".to_string(),
        };
        let request_bytes: Vec<u8> = [r#"{"type":"Get","id":42,"key":"foo"}"#, "\n"]
            .concat()
            .into();

        let (mut connection, _, output_receiver) = ClientConnection::with_channel();
        let _ = connection.write(&request).await;

        assert_eq!(output_receiver.recv().unwrap(), request_bytes);
    }

    #[tokio::test]
    async fn server_reads() {
        let request = Request::Get {
            id: 42,
            key: "foo".to_string(),
        };
        let request_bytes: Vec<u8> = [r#"{"type":"Get","id":42,"key":"foo"}"#, "\n"]
            .concat()
            .into();

        let (mut connection, input_sender, _) = ServerConnection::with_channel();
        input_sender.send(request_bytes.clone()).unwrap();

        assert_eq!(connection.read().await.unwrap(), request);
    }

    #[tokio::test]
    async fn server_writes() {
        let response = Response::ToGet {
            id: 42,
            value: Some("bar".to_string()),
        };
        let response_bytes: Vec<u8> = [r#"{"type":"ToGet","id":42,"value":"bar"}"#, "\n"]
            .concat()
            .into();

        let (mut connection, _, output_receiver) = ServerConnection::with_channel();
        let _ = connection.write(&response).await;

        assert_eq!(output_receiver.recv().unwrap(), response_bytes);
    }
}
