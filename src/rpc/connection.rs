use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;

use crate::error::Result;
use crate::rpc::request::Request;
use crate::rpc::response::Response;
use crate::NEWLINE;
use std::marker::PhantomData;
use tokio::sync::Mutex;

pub trait AsyncReader: AsyncRead + Unpin + Send {}
impl AsyncReader for OwnedReadHalf {}

pub trait AsyncWriter: AsyncWrite + Unpin + Send {}
impl AsyncWriter for OwnedWriteHalf {}

pub struct Connection<InputFrame, OutputFrame>
where
    InputFrame: From<Vec<u8>>,
    OutputFrame: Into<Vec<u8>>,
{
    pub input: Mutex<BufReader<Box<dyn AsyncReader>>>,
    pub output: Mutex<BufWriter<Box<dyn AsyncWriter>>>,
    pub input_frame: PhantomData<InputFrame>,
    pub output_frame: PhantomData<OutputFrame>,
}

impl<InputFrame, OutputFrame> Connection<InputFrame, OutputFrame>
where
    InputFrame: From<Vec<u8>>,
    OutputFrame: Into<Vec<u8>>,
{
    /// Create a new `Connection` backed by `socket`, with read and write buffers initialized.
    pub fn new(socket: TcpStream) -> Connection<InputFrame, OutputFrame> {
        let (r, w) = socket.into_split();
        let input = Mutex::new(BufReader::new(Box::new(r) as Box<dyn AsyncReader>));
        let output = Mutex::new(BufWriter::new(Box::new(w) as Box<dyn AsyncWriter>));
        Self {
            input,
            output,
            input_frame: PhantomData,
            output_frame: PhantomData,
        }
    }

    /// Read a `Request` or `Response` from the socket
    pub async fn read(&self) -> Result<InputFrame> {
        // TODO: use a Cursor<BytesMut> here instead of a Vec<u8> for buf?
        //  (will require manually impl of `read_until`)
        let mut buf = Vec::new();

        let mut input = self.input.lock().await;
        input.read_until(NEWLINE, &mut buf).await?;
        let frame: InputFrame = buf.into();

        Ok(frame)
    }

    /// Write a `Request` or `Response` to the socket
    pub async fn write(&self, frame: OutputFrame) -> Result<()> {
        let bytes: Vec<u8> = frame.into();

        let mut output = self.output.lock().await;
        output.write_all(&bytes).await?;
        output.write(&[NEWLINE]).await?;
        output.flush().await?;

        Ok(())
    }
}

pub type ClientConnection = Connection<Response, Request>;
pub type ServerConnection = Connection<Request, Response>;

#[cfg(test)]
mod connection_tests {
    use crate::rpc::connection::ClientConnection;

    use super::*;
    use crate::rpc::request::Command;
    use crate::rpc::response::Outcome;

    #[tokio::test]
    async fn client_reads() {
        let response_bytes: Vec<u8> = [
            r#"{"id":42,"outcome":{"type":"OfGet","value":"bar"}}"#,
            "\n",
        ]
        .concat()
        .into();
        let response = Response {
            id: 42,
            outcome: Outcome::OfGet {
                value: Some("bar".to_string()),
            },
        };

        let (connection, input_sender, _) = ClientConnection::with_channel();
        input_sender.send(response_bytes.clone()).unwrap();

        assert_eq!(connection.read().await.unwrap(), response);
    }

    #[tokio::test]
    async fn client_writes() {
        let request = Request {
            id: 42,
            command: Command::Get {
                key: "foo".to_string(),
            },
        };
        let request_bytes: Vec<u8> = [r#"{"id":42,"command":{"type":"Get","key":"foo"}}"#, "\n"]
            .concat()
            .into();

        let (connection, _, output_receiver) = ClientConnection::with_channel();
        let _ = connection.write(request).await;

        assert_eq!(output_receiver.recv().unwrap(), request_bytes);
    }

    #[tokio::test]
    async fn server_reads() {
        let request = Request {
            id: 42,
            command: Command::Get {
                key: "foo".to_string(),
            },
        };
        let request_bytes: Vec<u8> = [r#"{"id":42,"command":{"type":"Get","key":"foo"}}"#, "\n"]
            .concat()
            .into();

        let (connection, input_sender, _) = ServerConnection::with_channel();
        input_sender.send(request_bytes.clone()).unwrap();

        assert_eq!(connection.read().await.unwrap(), request);
    }

    #[tokio::test]
    async fn server_writes() {
        let response = Response {
            id: 42,
            outcome: Outcome::OfGet {
                value: Some("bar".to_string()),
            },
        };
        let response_bytes: Vec<u8> = [
            r#"{"id":42,"outcome":{"type":"OfGet","value":"bar"}}"#,
            "\n",
        ]
        .concat()
        .into();

        let (connection, _, output_receiver) = ServerConnection::with_channel();
        let _ = connection.write(response).await;

        assert_eq!(output_receiver.recv().unwrap(), response_bytes);
    }
}
