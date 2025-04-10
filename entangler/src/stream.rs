// Copyright 2024 Entanglement Contributors
// SPDX-License-Identifier: Apache-2.0, MIT

use crate::entangler::{ByteStream, Entangler, Error};
use bytes::Bytes;
use futures::{future::Future, ready, task::Poll, Stream, StreamExt, TryStreamExt};
use recall_entangler_storage::{
    self, ByteStream as StorageByteStream, Error as StorageError, Storage,
};
use std::pin::Pin;
use std::task::Context;

type ByteStreamFuture = Pin<Box<dyn Future<Output = Result<ByteStream, Error>> + Send>>;

/// A stream that wraps another stream and handles the repair process if a chunk fails during download.
/// If a chunk fails to download, the stream will:
/// 1. Download the metadata associated with the blob
/// 2. Try to repair the entire blob using parity data
/// 3. Return a new stream of the repaired data
///
/// This stream ensures data integrity by automatically repairing corrupted chunks during streaming.
pub struct RepairingStream<T: Storage + 'static> {
    inner: StorageByteStream,
    entangler: Entangler<T>,
    hash: String,
    metadata_hash: String,
    bytes_delivered: usize, // Track how many bytes we've delivered to the consumer
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
        inner: StorageByteStream,
    ) -> Self {
        Self {
            entangler,
            hash,
            metadata_hash,
            inner,
            bytes_delivered: 0,
        }
    }

    // Fast-forward a new stream to the given position
    async fn fast_forward_stream(
        mut stream: ByteStream,
        skip_bytes: usize,
    ) -> Result<ByteStream, Error> {
        let mut skipped = 0;
        let mut partial_chunk = None;

        // Keep reading chunks until we reach or exceed the skip position
        while skipped < skip_bytes {
            match stream.try_next().await? {
                Some(chunk) => {
                    let current_chunk_start = skipped;
                    let current_chunk_end = skipped + chunk.len();

                    if current_chunk_end > skip_bytes {
                        // This chunk contains our target position
                        let offset = skip_bytes - current_chunk_start;
                        partial_chunk = Some(chunk.slice(offset..));
                        break;
                    }

                    skipped += chunk.len();
                }
                None => {
                    return Err(Error::Other(anyhow::anyhow!(
                        "Stream ended during fast-forward"
                    )));
                }
            }
        }

        // Create a stream that starts at the exact position
        if let Some(partial) = partial_chunk {
            let first_chunk = futures::stream::once(async move { Ok(partial) });
            Ok(Box::pin(first_chunk.chain(stream)))
        } else {
            Ok(stream)
        }
    }

    /// Processes the repair future, updating the inner stream if repair was successful.
    /// This will return `Ok(())` if the repair was successful and the inner stream was updated.
    /// Otherwise, it will return an error.
    /// After successful call the inner must be polled again to get the next chunk.
    fn process_repair_future(
        &mut self,
        cx: &mut Context<'_>,
        mut fut: ByteStreamFuture,
    ) -> Result<(), Error> {
        match fut.as_mut().poll(cx) {
            Poll::Ready(result) => match result {
                Ok(new_stream) => {
                    // if we have delivered some bytes before the failure, we need to fast-forward the stream
                    // to the correct position
                    if self.bytes_delivered > 0 {
                        let mut ff_future =
                            Box::pin(Self::fast_forward_stream(new_stream, self.bytes_delivered));
                        match ff_future.as_mut().poll(cx) {
                            Poll::Ready(Ok(fast_forwarded_stream)) => {
                                self.inner = Box::pin(fast_forwarded_stream.map_err(|e| {
                                    StorageError::Other(recall_entangler_storage::wrap_error(
                                        e.into(),
                                    ))
                                }));
                            }
                            Poll::Ready(Err(e)) => return Err(e),
                            Poll::Pending => return Ok(()),
                        }
                    } else {
                        self.inner = Box::pin(new_stream.map_err(|e| {
                            StorageError::Other(recall_entangler_storage::wrap_error(e.into()))
                        }));
                    }
                    Ok(())
                }
                Err(e) => Err(e),
            },
            Poll::Pending => Ok(()),
        }
    }
}

impl<T: Storage + 'static> Stream for RepairingStream<T> {
    type Item = Result<Bytes, Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = unsafe { self.get_unchecked_mut() };

        // The loop is needed only to retry polling on inner future after it was replaced
        // with repaired stream
        loop {
            match ready!(this.inner.as_mut().poll_next(cx)) {
                Some(Ok(chunk)) => {
                    this.bytes_delivered += chunk.len();
                    return Poll::Ready(Some(Ok(chunk)));
                }
                Some(Err(_)) => {
                    let fut = {
                        let entangler = this.entangler.clone();
                        let hash = this.hash.clone();
                        let metadata_hash = this.metadata_hash.clone();

                        async move {
                            let metadata = entangler.download_metadata(&metadata_hash).await?;
                            entangler.download_repaired(&hash, metadata).await
                        }
                    };
                    // If succeeds it will replace `inner` future, so we repeat the loop again
                    if let Err(e) = this.process_repair_future(cx, Box::pin(fut)) {
                        return Poll::Ready(Some(Err(e)));
                    }
                }
                None => {
                    return Poll::Ready(None);
                }
            }
        }
    }
}

type ChunkStreamFuture<T> =
    Pin<Box<dyn Future<Output = Result<(T, Result<Bytes, Error>), Error>> + Send>>;

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
    current_future: Option<ChunkStreamFuture<T::ChunkId>>,
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
            current_future: None,
        }
    }

    // Extract future creation into a separate method
    fn create_download_future(&self, chunk_id: T::ChunkId) -> ChunkStreamFuture<T::ChunkId> {
        let entangler = self.entangler.clone();
        let hash = self.hash.clone();
        let metadata_hash = self.metadata_hash.clone();

        Box::pin(async move {
            match entangler
                .storage
                .download_chunk(&hash, chunk_id.clone())
                .await
            {
                Ok(chunk) => Ok((chunk_id, Ok(chunk))),
                Err(e) => {
                    let metadata = entangler.download_metadata(&metadata_hash).await?;
                    if entangler.config.always_repair {
                        // Try to repair and upload the whole blob
                        if let Err(e) = entangler.download_repaired(&hash, metadata).await {
                            return Ok((
                                chunk_id.clone(),
                                Err(Error::BlobDownload {
                                    hash: hash.clone(),
                                    source: e.into(),
                                }),
                            ));
                        }
                        // Try downloading again after repair
                        match entangler
                            .storage
                            .download_chunk(&hash, chunk_id.clone())
                            .await
                        {
                            Ok(chunk) => Ok((chunk_id, Ok(chunk))),
                            Err(e) => Ok((
                                chunk_id,
                                Err(Error::BlobDownload {
                                    hash,
                                    source: e.into(),
                                }),
                            )),
                        }
                    } else {
                        let mapper =
                            entangler
                                .storage
                                .chunk_id_mapper(&hash)
                                .await
                                .map_err(|_| Error::BlobDownload {
                                    hash: hash.clone(),
                                    source: e.into(),
                                })?;

                        let mut repaired_data = entangler
                            .repair_chunks(metadata, vec![chunk_id.clone()], mapper)
                            .await?;

                        Ok((
                            chunk_id.clone(),
                            repaired_data.remove(&chunk_id).map(Ok).unwrap_or_else(|| {
                                Err(Error::Other(anyhow::anyhow!("Chunk repair failed")))
                            }),
                        ))
                    }
                }
            }
        })
    }
}

impl<T: Storage + 'static> Stream for RepairingChunkStream<T> {
    type Item = (T::ChunkId, Result<Bytes, Error>);

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = unsafe { self.get_unchecked_mut() };

        if this.current_index >= this.chunk_ids.len() {
            return Poll::Ready(None);
        }

        // Poll existing future or create a new one
        let fut = if let Some(fut) = this.current_future.as_mut() {
            fut
        } else {
            let chunk_id = this.chunk_ids[this.current_index].clone();
            this.current_future = Some(this.create_download_future(chunk_id));
            this.current_future.as_mut().unwrap()
        };

        match fut.as_mut().poll(cx) {
            Poll::Ready(result) => {
                this.current_future = None;
                this.current_index += 1;
                Poll::Ready(Some(match result {
                    Ok((chunk_id, res)) => (chunk_id, res),
                    Err(e) => (this.chunk_ids[this.current_index - 1].clone(), Err(e)),
                }))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}
