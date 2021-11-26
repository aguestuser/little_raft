use std::convert::{TryFrom, TryInto};
use std::fmt::Display;
use std::marker::PhantomData;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;
use tokio::sync::Mutex;

use crate::error::NetworkError::{ConnectionClosed, MessageDeserializationError};
use crate::error::Result;
use crate::NEWLINE;

pub struct Connection<InputFrame, OutputFrame>
where
    InputFrame: TryFrom<Vec<u8>>,
    OutputFrame: Into<Vec<u8>>,
{
    pub input: Mutex<BufReader<OwnedReadHalf>>,
    pub output: Mutex<BufWriter<OwnedWriteHalf>>,
    pub input_frame: PhantomData<InputFrame>,
    pub output_frame: PhantomData<OutputFrame>,
}

#[macro_export]
macro_rules! tcp_serializable {
    ($struct_name:ident) => {
        impl TryFrom<Vec<u8>> for $struct_name {
            type Error = serde_json::Error;
            fn try_from(bytes: Vec<u8>) -> StdResult<$struct_name, Self::Error> {
                serde_json::from_slice(&bytes)
            }
        }
        impl Into<Vec<u8>> for $struct_name {
            fn into(self) -> Vec<u8> {
                serde_json::to_vec(&self).unwrap()
            }
        }
    };
}

impl<InputFrame, OutputFrame> Connection<InputFrame, OutputFrame>
where
    InputFrame: TryFrom<Vec<u8>>,
    OutputFrame: Into<Vec<u8>>,
{
    /// Create a new `Connection` backed by `socket`, with read and write buffers initialized.
    pub fn new(socket: TcpStream) -> Connection<InputFrame, OutputFrame> {
        let (r, w) = socket.into_split();
        let input = Mutex::new(BufReader::new(r));
        let output = Mutex::new(BufWriter::new(w));

        Self {
            input,
            output,
            input_frame: PhantomData,
            output_frame: PhantomData,
        }
    }

    /// Read an `InputFrame` from the socket
    pub async fn read(&self) -> Result<InputFrame>
    where
        <InputFrame as TryFrom<Vec<u8>>>::Error: Display,
    {
        let mut buf = Vec::new();
        let mut input = self.input.lock().await;
        input.read_until(NEWLINE, &mut buf).await?;

        if buf.is_empty() {
            Err(ConnectionClosed.boxed())
        } else {
            buf.try_into()
                .map_err(|e: <InputFrame as TryFrom<Vec<u8>>>::Error| {
                    MessageDeserializationError(e.to_string()).boxed()
                })
        }
    }

    /// Write an `OutputFrame` to the socket
    pub async fn write(&self, frame: OutputFrame) -> Result<()> {
        let bytes: Vec<u8> = frame.into();

        let mut output = self.output.lock().await;
        output.write_all(&bytes).await?;
        output.write(&[NEWLINE]).await?;
        output.flush().await?;

        Ok(())
    }

    pub async fn close(&self) -> Result<()> {
        let mut output = self.output.lock().await;
        let _ = output.shutdown().await?;
        Ok(())
    }
}

#[cfg(test)]
mod tcp_tests {
    use std::convert::TryFrom;
    use std::result::Result as StdResult;

    use serde::{Deserialize, Serialize};
    use serde_json;
    use test_context::{test_context, AsyncTestContext};
    use tokio::net::{TcpListener, TcpStream};
    use tokio::sync::oneshot;

    use crate::error::NetworkError::ConnectionClosed;
    use crate::tcp::Connection;
    use crate::test_support::gen::Gen;

    #[derive(Deserialize, Serialize, Debug, Clone, Eq, PartialEq)]
    struct FakeRequest {
        foo: usize,
    }
    tcp_serializable!(FakeRequest);

    #[derive(Deserialize, Serialize, Debug, Clone, Eq, PartialEq)]
    struct FakeResponse {
        bar: usize,
    }
    tcp_serializable!(FakeResponse);

    type FakeClientConnection = Connection<FakeResponse, FakeRequest>;
    type FakeServerConnection = Connection<FakeRequest, FakeResponse>;

    struct LiveConnections {
        client: FakeClientConnection,
        server: FakeServerConnection,
    }

    #[async_trait::async_trait]
    impl AsyncTestContext for LiveConnections {
        async fn setup() -> Self {
            let address = Gen::socket_addr();
            let server_address = address.clone();
            let (server_socket_tx, server_socket_rx) = oneshot::channel::<TcpStream>();
            let tcp_listener = TcpListener::bind(server_address).await.unwrap();

            tokio::spawn(async move {
                let (socket, _) = tcp_listener.accept().await.unwrap();
                let _ = server_socket_tx.send(socket);
            });

            let client_socket = TcpStream::connect(address).await.unwrap();
            let server_socket = server_socket_rx.await.unwrap();

            Self {
                client: FakeClientConnection::new(client_socket),
                server: FakeServerConnection::new(server_socket),
            }
        }
    }

    #[test_context(LiveConnections)]
    #[tokio::test]
    async fn server_reads_client_request(ctx: LiveConnections) {
        let req = FakeRequest { foo: 42 };
        let client_write = ctx.client.write(req.clone()).await;
        let server_read = ctx.server.read().await;

        assert!(client_write.is_ok());
        assert_eq!(server_read.unwrap(), req);
    }

    #[test_context(LiveConnections)]
    #[tokio::test]
    async fn server_sends_response_to_client(ctx: LiveConnections) {
        let resp = FakeResponse { bar: 42 };
        let server_write = ctx.server.write(resp.clone()).await;
        let client_read = ctx.client.read().await;

        assert!(server_write.is_ok());
        assert_eq!(client_read.unwrap(), resp);
    }

    #[test_context(LiveConnections)]
    #[tokio::test]
    async fn client_closes_connection_to_server(ctx: LiveConnections) {
        let client_write = ctx.client.close().await;
        let server_read = ctx.server.read().await;

        assert!(client_write.is_ok());
        assert!(server_read.is_err());
        assert_eq!(
            server_read.err().unwrap().downcast_ref(),
            Some(&ConnectionClosed)
        );
    }

    #[test_context(LiveConnections)]
    #[tokio::test]
    async fn server_closes_connection_to_client(ctx: LiveConnections) {
        let server_write = ctx.server.close().await;
        let client_read = ctx.client.read().await;

        assert!(server_write.is_ok());
        assert!(client_read.is_err());
        assert_eq!(
            client_read.err().unwrap().downcast_ref(),
            Some(&ConnectionClosed)
        );
    }
}
