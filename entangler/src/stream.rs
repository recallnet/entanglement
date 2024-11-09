// Copyright 2024 Entanglement Contributors
// SPDX-License-Identifier: Apache-2.0, MIT

use crate::entangler::{ByteStream, Entangler, Error, CHUNK_SIZE};
use bytes::{Bytes, BytesMut};
use storage::{self, Error as StorageError, Storage};

use futures::future::Future;
use futures::ready;
use futures::task::Poll;
use futures::Stream;
use futures::TryStreamExt;
use std::pin::Pin;
use std::task::Context;

/// A stream that wraps another stream and handles the repair process if a chunk fails during download.
/// If a chunk fails to download, the stream will:
/// 1. Download the metadata associated with the blob
/// 2. Try to repair the entire blob using parity data
/// 3. Return a new stream of the repaired data
///
/// This stream ensures data integrity by automatically repairing corrupted chunks during streaming.
pub struct RepairingStream<T: Storage + 'static> {
    inner: storage::ByteStream,
    buffer: BytesMut,
    repair_future: Option<Pin<Box<dyn Future<Output = Result<ByteStream, Error>> + Send>>>,
    entangler: Entangler<T>,
    hash: String,
    metadata_hash: String,
}

impl<T: Storage + 'static> RepairingStream<T> {
    /// Creates a new `RepairingStream`.
    ///
    /// # Arguments
    ///
    /// * `entangler` - The entangler instance to use for repairs
    /// * `hash` - The hash of the blob being downloaded
    /// * `metadata_hash` - The hash of the metadata used for repairs
    /// * `inner` - The underlying byte stream to wrap
    ///
    /// # Returns
    ///
    /// A new `RepairingStream` instance that wraps the provided stream
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
            repair_future: None,
        }
    }
}

impl<T: Storage + 'static> Stream for RepairingStream<T> {
    type Item = Result<Bytes, Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = unsafe { self.get_unchecked_mut() };

        // First, try to make progress on repair if it's in progress
        if let Some(fut) = &mut this.repair_future {
            match fut.as_mut().poll(cx) {
                Poll::Ready(Ok(new_stream)) => {
                    this.inner = Box::pin(
                        new_stream.map_err(|e| StorageError::Other(storage::wrap_error(e.into()))),
                    );
                    this.repair_future = None;
                }
                Poll::Ready(Err(e)) => {
                    this.repair_future = None;
                    return Poll::Ready(Some(Err(e)));
                }
                Poll::Pending => return Poll::Pending,
            }
        }

        // Then continue with normal stream processing
        match ready!(this.inner.as_mut().poll_next(cx)) {
            Some(Ok(chunk)) => {
                this.buffer.extend_from_slice(&chunk);
                if this.buffer.len() >= CHUNK_SIZE as usize {
                    let chunk = this.buffer.split_to(CHUNK_SIZE as usize).freeze();
                    return Poll::Ready(Some(Ok(chunk)));
                } else {
                    return Poll::Pending;
                }
            }
            Some(Err(_)) => {
                // Start repair process
                let fut = {
                    let entangler = this.entangler.clone();
                    let hash = this.hash.clone();
                    let metadata_hash = this.metadata_hash.clone();

                    async move {
                        let metadata = entangler.download_metadata(&metadata_hash).await?;
                        entangler.download_repaired(&hash, metadata).await
                    }
                };
                this.repair_future = Some(Box::pin(fut));
                return Poll::Pending;
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

/// A stream that downloads chunks individually and attempts to repair them if they fail to download.
/// If a chunk fails to download, the stream will:
/// 1. If `always_repair` is true in the config:
///    - Try to repair and upload the entire blob
///    - Attempt to download the chunk again
/// 2. If `always_repair` is false:
///    - Try to repair just the failed chunk
///
/// # Type Parameters
///
/// * `T` - The storage backend type that implements the `Storage` trait
pub struct RepairingChunkStream<T: Storage + 'static> {
    entangler: Entangler<T>,
    hash: String,
    metadata_hash: String,
    chunk_ids: Vec<T::ChunkId>,
    current_index: usize,
}

impl<T: Storage + 'static> RepairingChunkStream<T> {
    /// Creates a new `RepairingChunkStream`.
    ///
    /// # Arguments
    ///
    /// * `entangler` - The entangler instance to use for repairs
    /// * `hash` - The hash of the blob to download chunks from
    /// * `metadata_hash` - The hash of the metadata used for repairs
    /// * `chunk_ids` - The IDs of the chunks to download
    ///
    /// # Returns
    ///
    /// A new `RepairingChunkStream` instance
    pub fn new(
        entangler: Entangler<T>,
        hash: String,
        metadata_hash: String,
        chunk_ids: Vec<T::ChunkId>,
    ) -> Self {
        Self {
            entangler,
            hash,
            metadata_hash,
            chunk_ids,
            current_index: 0,
        }
    }
}

impl<T: Storage + 'static> Stream for RepairingChunkStream<T> {
    type Item = (T::ChunkId, Result<Bytes, Error>);

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // this is safe because we never move the inner stream
        let this = unsafe { self.get_unchecked_mut() };

        if this.current_index >= this.chunk_ids.len() {
            return Poll::Ready(None);
        }

        let chunk_id = this.chunk_ids[this.current_index].clone();
        this.current_index += 1;

        let fut = async {
            match this
                .entangler
                .storage
                .download_chunk(&this.hash, chunk_id.clone())
                .await
            {
                Ok(chunk) => Ok((chunk_id.clone(), Ok(chunk))),
                Err(e) => {
                    let metadata = this
                        .entangler
                        .download_metadata(&this.metadata_hash)
                        .await?;

                    if this.entangler.config.always_repair {
                        // Try to repair and upload the whole blob
                        if let Err(e) = this.entangler.download_repaired(&this.hash, metadata).await
                        {
                            return Ok((
                                chunk_id.clone(),
                                Err(Error::BlobDownload {
                                    hash: this.hash.clone(),
                                    source: e.into(),
                                }),
                            ));
                        }
                        // Try downloading again after repair
                        match this
                            .entangler
                            .storage
                            .download_chunk(&this.hash, chunk_id.clone())
                            .await
                        {
                            Ok(chunk) => Ok((chunk_id.clone(), Ok(chunk))),
                            Err(e) => Ok((
                                chunk_id.clone(),
                                Err(Error::BlobDownload {
                                    hash: this.hash.clone(),
                                    source: e.into(),
                                }),
                            )),
                        }
                    } else {
                        // Try to repair just this chunk
                        let mapper = this
                            .entangler
                            .storage
                            .chunk_id_mapper(&this.hash)
                            .await
                            .map_err(|_| Error::BlobDownload {
                                hash: this.hash.clone(),
                                source: e.into(),
                            })?;

                        match this
                            .entangler
                            .repair_chunks(metadata, vec![chunk_id.clone()], mapper)
                            .await
                        {
                            Ok(mut repaired_data) => Ok((
                                chunk_id.clone(),
                                Ok(repaired_data.remove(&chunk_id).unwrap()),
                            )),
                            Err(e) => Ok((chunk_id.clone(), Err(e))),
                        }
                    }
                }
            }
        };

        let waker = cx.waker().clone();
        let fut = Box::pin(fut);
        let async_call = futures::executor::block_on(async {
            let result = fut.await;
            waker.wake_by_ref();
            result
        });
        match async_call {
            Ok((chunk_id, result)) => Poll::Ready(Some((chunk_id, result))),
            Err(e) => Poll::Ready(Some((chunk_id, Err(e)))),
        }
    }
}
