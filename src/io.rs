use bytes::Bytes;
use futures_util::Stream;
use std::{
    io::{Error, ErrorKind},
    pin::pin,
    task::Poll,
};
use tokio::io::{AsyncRead, ReadBuf};

const BUF_SIZE: usize = 8192;

/// Make any AsynRead implementation work as Stream
pub struct ReaderStream<R> {
    reader: R,
}

impl<R> ReaderStream<R>
where
    R: AsyncRead,
{
    /// Create a new ReaderStream
    pub fn new(reader: R) -> Self {
        Self { reader }
    }
}

impl<R> Stream for ReaderStream<R>
where
    R: AsyncRead + Unpin,
{
    type Item = Result<Bytes, Error>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let mut buf = [0u8; BUF_SIZE];
        let read_buf = &mut ReadBuf::new(&mut buf);
        match pin!(&mut self.reader).poll_read(cx, read_buf) {
            Poll::Ready(Ok(_)) => {
                let filled = read_buf.filled();
                if filled.is_empty() {
                    return Poll::Ready(None);
                }
                Poll::Ready(Some(Ok(Bytes::copy_from_slice(filled))))
            }
            Poll::Ready(Err(e)) => Poll::Ready(Some(Err(Error::new(ErrorKind::Other, e)))),
            Poll::Pending => Poll::Pending,
        }
    }
}
