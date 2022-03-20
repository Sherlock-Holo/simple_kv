use std::pin::Pin;
use std::task::{Context, Poll};

use futures_util::future::Either;
use futures_util::Stream;
use pin_project::pin_project;

pub fn select_either<S1, S2>(stream1: S1, stream2: S2) -> SelectEither<S1, S2> {
    SelectEither { stream1, stream2 }
}

#[must_use]
#[pin_project]
pub struct SelectEither<S1, S2> {
    #[pin]
    stream1: S1,

    #[pin]
    stream2: S2,
}

impl<S1: Stream, S2: Stream> Stream for SelectEither<S1, S2> {
    type Item = Either<S1::Item, S2::Item>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();

        if let Poll::Ready(Some(item)) = this.stream1.poll_next(cx) {
            return Poll::Ready(Some(Either::Left(item)));
        }

        this.stream2
            .poll_next(cx)
            .map(|item| item.map(Either::Right))
    }
}
