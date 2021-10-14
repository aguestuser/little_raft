use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;

use crate::error;
use crate::tcp::NEWLINE;

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
    pub async fn read(&mut self) -> error::Result<Vec<u8>> {
        // TODO use self.buf (BytesMute w/ Cursor) here instead of allocating new buf?
        let mut buf = Vec::new();
        self.input.read_until(NEWLINE, &mut buf).await?;
        Ok(buf)
    }

    /// Write a `Response` to the socket
    pub async fn write(&mut self, mut buf: Vec<u8>) -> error::Result<()> {
        buf.push(NEWLINE);
        self.output.write_all(&buf).await?;
        self.output.flush().await?;
        Ok(())
    }
}

#[cfg(test)]
mod connection_tests {
    use super::*;

    lazy_static! {
        static ref FRAME: Vec<u8> = [r#"{"id":42,"type":"Get","key":"foo"}"#, "\n"]
            .concat()
            .into();
    }

    #[tokio::test]
    async fn reads() {
        let (mut connection, input_sender, _) = Connection::with_channel();
        input_sender.send(FRAME.clone()).unwrap();

        assert_eq!(connection.read().await.unwrap(), *FRAME);
    }

    #[tokio::test]
    async fn writes() {
        let (mut connection, _, output_receiver) = Connection::with_channel();
        let _ = connection.write(FRAME.clone()).await;

        assert_eq!(
            output_receiver.recv().unwrap(),
            [FRAME.clone(), b"\n".to_vec()].concat(),
        );
    }
}
