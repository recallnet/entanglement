// Copyright 2024 Entanglement Contributors
// SPDX-License-Identifier: Apache-2.0, MIT

use crate::entangler::{Entangler, Error, CHUNK_SIZE};
use bytes::{Bytes, BytesMut};
use storage::{self, Error as StorageError, Storage};

use futures::ready;
use futures::task::Poll;
use futures::Stream;
use futures::TryStreamExt;
use std::pin::Pin;
use std::task::Context;

/// A stream that wraps another stream and handles the repair process if a chunk fails during download.
pub struct RepairingStream<T: Storage + 'static> {
    entangler: Entangler<T>,
    hash: String,
    metadata_hash: String,
    inner: storage::ByteStream,
    buffer: BytesMut,
}

impl<T: Storage + 'static> RepairingStream<T> {
    pub fn new(
        entangler: Entangler<T>,
        hash: String,
        metadata_hash: String,
        inner: storage::ByteStream,
    ) -> Self {
        Self {
            entangler,
            hash,
            metadata_hash,
            inner,
            buffer: BytesMut::new(),
        }
    }
}

impl<T: Storage + Send + Sync + 'static> Stream for RepairingStream<T> {
    type Item = Result<Bytes, Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = unsafe { self.get_unchecked_mut() };

        loop {
            match ready!(this.inner.as_mut().poll_next(cx)) {
                Some(Ok(chunk)) => {
                    this.buffer.extend_from_slice(&chunk);
                    if this.buffer.len() >= CHUNK_SIZE as usize {
                        let chunk = this.buffer.split_to(CHUNK_SIZE as usize).freeze();
                        return Poll::Ready(Some(Ok(chunk)));
                    }
                }
                Some(Err(_)) => {
                    let entangler = this.entangler.clone();
                    let hash = this.hash.clone();
                    let metadata_hash = this.metadata_hash.clone();

                    let fut = async move {
                        let metadata = entangler.download_metadata(&metadata_hash).await?;
                        let repaired_stream = entangler.download_repaired(&hash, metadata).await?;
                        Ok::<_, Error>(repaired_stream)
                    };

                    let waker = cx.waker().clone();
                    let fut = Box::pin(fut);
                    match futures::executor::block_on(async {
                        let result = fut.await;
                        waker.wake_by_ref();
                        result
                    }) {
                        Ok(stream) => {
                            this.inner =
                                Box::pin(stream.map_err(|e| {
                                    StorageError::Other(storage::wrap_error(e.into()))
                                }));
                        }
                        Err(e) => {
                            return Poll::Ready(Some(Err(e)));
                        }
                    }
                }
                None => {
                    if !this.buffer.is_empty() {
                        let chunk = this.buffer.split().freeze();
                        return Poll::Ready(Some(Ok(chunk)));
                    } else {
                        return Poll::Ready(None);
                    }
                }
            }
        }
    }
}
