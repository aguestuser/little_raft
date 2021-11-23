use std::convert::{TryFrom, TryInto};
use std::marker::PhantomData;

use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;
use tokio::sync::Mutex;

use crate::error::AsyncError;
use crate::error::NetworkError::MessageDeserializationError;
use crate::error::Result;
use crate::NEWLINE;
use std::fmt::Display;

pub trait AsyncReader: AsyncRead + Unpin + Send {}
impl AsyncReader for OwnedReadHalf {}

pub trait AsyncWriter: AsyncWrite + Unpin + Send {}
impl AsyncWriter for OwnedWriteHalf {}

pub struct Connection<I, O>
where
    I: TryFrom<Vec<u8>>,
    O: Into<Vec<u8>>,
{
    pub input: Mutex<BufReader<Box<dyn AsyncReader>>>,
    pub output: Mutex<BufWriter<Box<dyn AsyncWriter>>>,
    pub input_frame: PhantomData<I>,
    pub output_frame: PhantomData<O>,
}

impl<I, O> Connection<I, O>
where
    I: TryFrom<Vec<u8>>,
    O: Into<Vec<u8>>,
{
    /// Create a new `Connection` backed by `socket`, with read and write buffers initialized.
    pub fn new(socket: TcpStream) -> Connection<I, O> {
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

    /// Read a `RequestEnvelope` or `ResponseEnvelope` from the socket
    pub async fn read(&self) -> Result<I>
    where
        <I as TryFrom<Vec<u8>>>::Error: Display,
    {
        let mut buf = Vec::new();
        let mut input = self.input.lock().await;
        input.read_until(NEWLINE, &mut buf).await?;
        buf.try_into().map_err(|e: <I as TryFrom<Vec<u8>>>::Error| {
            Box::new(MessageDeserializationError(e.to_string())) as AsyncError
        })
    }

    /// Write a `RequestEnvelope` or `ResponseEnvelope` to the socket
    pub async fn write(&self, frame: O) -> Result<()> {
        let bytes: Vec<u8> = frame.into();

        let mut output = self.output.lock().await;
        output.write_all(&bytes).await?;
        output.write(&[NEWLINE]).await?;
        output.flush().await?;

        Ok(())
    }
}

#[cfg(test)]
mod connection_tests {
    use crate::rpc_legacy::request::{LegacyRpcRequest, LegacyRpcRequestEnvelope};
    use crate::rpc_legacy::response::{LegacyRpcResponse, LegacyRpcResponseEnvelope};
    use crate::rpc_legacy::{LegacyRpcClientConnection, LegacyRpcServerConnection};

    lazy_static! {
        static ref PUT_REQ: LegacyRpcRequestEnvelope = LegacyRpcRequestEnvelope {
            id: 42,
            body: LegacyRpcRequest::Put {
                key: "foo".to_string(),
                value: "bar".to_string(),
            },
        };
        static ref PUT_REQ_BYTES: Vec<u8> = [
            r#"{"id":42,"body":{"type":"Put","key":"foo","value":"bar"}}"#,
            "\n",
        ]
        .concat()
        .into();
        static ref PUT_RESP: LegacyRpcResponseEnvelope = LegacyRpcResponseEnvelope {
            id: 42,
            body: LegacyRpcResponse::ToPut { was_modified: true },
        };
        static ref PUT_RESP_BYTES: Vec<u8> = [
            r#"{"id":42,"body":{"type":"ToPut","was_modified":true}}"#,
            "\n",
        ]
        .concat()
        .into();
    }

    #[tokio::test]
    async fn client_reads() {
        let (connection, input_sender, _) = LegacyRpcClientConnection::with_channel();
        input_sender.send(PUT_RESP_BYTES.clone()).unwrap();

        assert_eq!(connection.read().await.unwrap(), PUT_RESP.clone());
    }

    #[tokio::test]
    async fn client_writes() {
        let (connection, _, output_receiver) = LegacyRpcClientConnection::with_channel();
        let _ = connection.write(PUT_REQ.clone()).await;

        assert_eq!(output_receiver.recv().unwrap(), PUT_REQ_BYTES.clone());
    }

    #[tokio::test]
    async fn server_reads() {
        let (connection, input_sender, _) = LegacyRpcServerConnection::with_channel();
        input_sender.send(PUT_REQ_BYTES.clone()).unwrap();

        assert_eq!(connection.read().await.unwrap(), PUT_REQ.clone());
    }

    #[tokio::test]
    async fn server_writes() {
        let (connection, _, output_receiver) = LegacyRpcServerConnection::with_channel();
        let _ = connection.write(PUT_RESP.clone()).await;

        assert_eq!(output_receiver.recv().unwrap(), PUT_RESP_BYTES.clone());
    }
}
