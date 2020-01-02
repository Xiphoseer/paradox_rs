use futures::{
    io::{AsyncWrite, Error},
    task::Context,
    Poll,
};
use std::pin::Pin;
use tokio::{fs::File, prelude::AsyncWrite as TokioAsyncWrite};

pub struct FileWrapper(pub File);

impl FileWrapper {
    fn inner(self: Pin<&mut Self>) -> Pin<&mut File> {
        // This is okay because `0` is pinned when `self` is.
        unsafe { self.map_unchecked_mut(|s| &mut s.0) }
    }
}

impl AsyncWrite for FileWrapper {
    #[inline]
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &[u8],
    ) -> Poll<Result<usize, Error>> {
        <File as TokioAsyncWrite>::poll_write(self.inner(), cx, buf)
    }

    #[inline]
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Error>> {
        <File as TokioAsyncWrite>::poll_flush(self.inner(), cx)
    }

    #[inline]
    fn poll_close(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Error>> {
        <File as TokioAsyncWrite>::poll_shutdown(self.inner(), cx)
    }
}
