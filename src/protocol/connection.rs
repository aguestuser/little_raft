use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;

use crate::error;
use crate::protocol::request::Request;
use crate::protocol::response::Response;
use crate::protocol::NEWLINE;
use tokio::sync::Mutex;

pub trait AsyncReader: AsyncRead + Unpin + Send {}
impl AsyncReader for OwnedReadHalf {}

pub trait AsyncWriter: AsyncWrite + Unpin + Send {}
impl AsyncWriter for OwnedWriteHalf {}

macro_rules! connection_of {
    ($struct_name:ident, $input_type:ty, $output_type:ty) => {
        pub struct $struct_name {
            pub input: Mutex<BufReader<Box<dyn AsyncReader>>>,
            pub output: Mutex<BufWriter<Box<dyn AsyncWriter>>>,
        }

        impl $struct_name {
            /// Create a new `$struct_name` backed by `socket`, with read and write buffers initialized.
            pub fn new(socket: TcpStream) -> $struct_name {
                let (r, w) = socket.into_split();
                let input = Mutex::new(BufReader::new(Box::new(r) as Box<dyn AsyncReader>));
                let output = Mutex::new(BufWriter::new(Box::new(w) as Box<dyn AsyncWriter>));
                Self { input, output }
            }

            /// Read a `$input_type` from the socket
            pub async fn read(&self) -> error::Result<$input_type> {
                // TODO: use a Cursor<BytesMut> here instead of a Vec<u8> for buf?
                //  (will require manually impl of `read_until`)
                let mut buf = Vec::new();

                let mut input = self.input.lock().await;
                input.read_until(NEWLINE, &mut buf).await?;
                let frame: $input_type = buf.into();

                Ok(frame)
            }

            /// Write a `$output_type` to the socket
            pub async fn write(&self, frame: &$output_type) -> error::Result<()> {
                let bytes: Vec<u8> = frame.clone().into();

                let mut output = self.output.lock().await;
                output.write_all(&bytes).await?;
                output.write(&[NEWLINE]).await?;
                output.flush().await?;

                Ok(())
            }
        }
    };
}

connection_of!(ClientConnection, Response, Request);
connection_of!(ServerConnection, Request, Response);

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

        let (connection, input_sender, _) = ClientConnection::with_channel();
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

        let (connection, _, output_receiver) = ClientConnection::with_channel();
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

        let (connection, input_sender, _) = ServerConnection::with_channel();
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

        let (connection, _, output_receiver) = ServerConnection::with_channel();
        let _ = connection.write(&response).await;

        assert_eq!(output_receiver.recv().unwrap(), response_bytes);
    }
}
