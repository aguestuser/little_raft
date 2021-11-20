use std::convert::{TryFrom, TryInto};
use std::marker::PhantomData;

use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;
use tokio::sync::Mutex;

use crate::error::NetworkError::MessageDeserializationError;
use crate::AsyncError;
use crate::Result;
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
    use crate::api::request::{ApiRequest, ApiRequestEnvelope};
    use crate::api::response::{ApiResponse, ApiResponseEnvelope};
    use crate::api::{ClientConnection, ServerConnection};

    #[tokio::test]
    async fn client_reads() {
        let response_bytes: Vec<u8> = [r#"{"id":42,"body":{"type":"ToGet","value":"bar"}}"#, "\n"]
            .concat()
            .into();
        let response = ApiResponseEnvelope {
            id: 42,
            body: ApiResponse::ToGet {
                value: Some("bar".to_string()),
            },
        };

        let (connection, input_sender, _) = ClientConnection::with_channel();
        input_sender.send(response_bytes.clone()).unwrap();

        assert_eq!(connection.read().await.unwrap(), response);
    }

    #[tokio::test]
    async fn client_writes() {
        let request = ApiRequestEnvelope {
            id: 42,
            body: ApiRequest::Get {
                key: "foo".to_string(),
            },
        };
        let request_bytes: Vec<u8> = [r#"{"id":42,"body":{"type":"Get","key":"foo"}}"#, "\n"]
            .concat()
            .into();

        let (connection, _, output_receiver) = ClientConnection::with_channel();
        let _ = connection.write(request).await;

        assert_eq!(output_receiver.recv().unwrap(), request_bytes);
    }

    #[tokio::test]
    async fn server_reads() {
        let request = ApiRequestEnvelope {
            id: 42,
            body: ApiRequest::Get {
                key: "foo".to_string(),
            },
        };
        let request_bytes: Vec<u8> = [r#"{"id":42,"body":{"type":"Get","key":"foo"}}"#, "\n"]
            .concat()
            .into();

        let (connection, input_sender, _) = ServerConnection::with_channel();
        input_sender.send(request_bytes.clone()).unwrap();

        assert_eq!(connection.read().await.unwrap(), request);
    }

    #[tokio::test]
    async fn server_writes() {
        let response = ApiResponseEnvelope {
            id: 42,
            body: ApiResponse::ToGet {
                value: Some("bar".to_string()),
            },
        };
        let response_bytes: Vec<u8> = [r#"{"id":42,"body":{"type":"ToGet","value":"bar"}}"#, "\n"]
            .concat()
            .into();

        let (connection, _, output_receiver) = ServerConnection::with_channel();
        let _ = connection.write(response).await;

        assert_eq!(output_receiver.recv().unwrap(), response_bytes);
    }
}
