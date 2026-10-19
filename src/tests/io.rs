use futures_util::StreamExt;
use std::{
    io::{self, Cursor, ErrorKind},
    pin::Pin,
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, ReadBuf};

use crate::io::ReaderStream;

struct ErrorReader;

impl AsyncRead for ErrorReader {
    fn poll_read(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        _buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Poll::Ready(Err(io::Error::new(ErrorKind::PermissionDenied, "read failed")))
    }
}

#[tokio::test]
async fn reader_stream_yields_partial_data_then_eof() {
    let mut stream = ReaderStream::new(Cursor::new(b"hello".to_vec()));

    let data = stream
        .next()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(data.as_ref(), b"hello");
    assert!(
        stream
            .next()
            .await
            .is_none()
    );
}

#[tokio::test]
async fn reader_stream_yields_full_buffer_then_remaining_data() {
    let data = vec![42; 8193];
    let mut stream = ReaderStream::new(Cursor::new(data));

    assert_eq!(
        stream
            .next()
            .await
            .unwrap()
            .unwrap()
            .len(),
        8192
    );
    assert_eq!(
        stream
            .next()
            .await
            .unwrap()
            .unwrap()
            .as_ref(),
        &[42]
    );
    assert!(
        stream
            .next()
            .await
            .is_none()
    );
}

#[tokio::test]
async fn reader_stream_returns_read_errors() {
    let mut stream = ReaderStream::new(ErrorReader);

    let error = stream
        .next()
        .await
        .unwrap()
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Other);
    assert_eq!(error.to_string(), "read failed");
}
