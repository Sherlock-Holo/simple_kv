use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures_util::{ready, FutureExt};
use tokio::task::JoinHandle;

pub struct Flatten<O, E> {
    join_handle: JoinHandle<Result<O, E>>,
}

impl<O, E> Future for Flatten<O, E> {
    type Output = Result<O, E>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Ready(ready!(self.join_handle.poll_unpin(cx)).unwrap())
    }
}

pub trait TokioResultTaskExt<O, E> {
    fn flatten_result(self) -> Flatten<O, E>;
}

impl<O, E> TokioResultTaskExt<O, E> for JoinHandle<Result<O, E>> {
    fn flatten_result(self) -> Flatten<O, E> {
        Flatten { join_handle: self }
    }
}
