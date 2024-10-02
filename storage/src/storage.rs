// Copyright 2024 Entanglement Contributors
// SPDX-License-Identifier: Apache-2.0, MIT

use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use futures::Stream;
use std::pin::Pin;
use thiserror;

/// Error type for storage operations.
#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// Error indicating that a blob with the specified hash was not found.
    #[error("Blob with hash {0} not found")]
    BlobNotFound(String),

    /// Error indicating that a chunk with the specified id was not found in the specified blob.
    #[error("Chunk with id {0} not found in blob {1}: {2}")]
    ChunkNotFound(String, String, anyhow::Error),

    /// General storage error.
    #[error("Storage error: {0}")]
    StorageError(#[from] anyhow::Error),

    /// Error indicating that the provided hash is invalid.
    #[error("Invalid hash {0}. Error: {1}")]
    InvalidHash(String, String),

    /// A catch-all error for other types of errors.
    #[error("Error occurred: {0}")]
    Other(#[source] anyhow::Error),
}

/// Type alias for a stream of bytes.
pub type ByteStream<T> = Pin<Box<dyn Stream<Item = (T, Result<Bytes>)> + Send>>;

/// Trait representing a storage backend.
#[async_trait]
pub trait Storage: Send + Clone {
    /// The type used to identify chunks.
    type ChunkId: Clone + Default + PartialEq + Eq + std::hash::Hash;

    /// Uploads the given bytes to the storage and returns a hash identifying the stored data.
    ///
    /// # Arguments
    ///
    /// * `bytes` - The bytes to upload.
    ///
    /// # Returns
    ///
    /// A `Result` containing the hash of the uploaded data, or an error if the upload fails.
    async fn upload_bytes(&self, bytes: impl Into<Bytes> + Send) -> Result<String>;

    /// Downloads the bytes identified by the given hash.
    ///
    /// # Arguments
    ///
    /// * `hash` - The hash identifying the data to download.
    ///
    /// # Returns
    ///
    /// A `Result` containing the downloaded bytes, or an `Error` if the download fails.
    async fn download_bytes(&self, hash: &str) -> Result<Bytes, Error>;

    /// Returns a stream of chunks for the data identified by the given hash.
    ///
    /// # Arguments
    ///
    /// * `hash` - The hash identifying the data.
    ///
    /// # Returns
    ///
    /// A `Result` containing a stream of chunks, or an `Error` if the operation fails.
    async fn iter_chunks(&self, hash: &str) -> Result<ByteStream<Self::ChunkId>, Error>;

    /// Downloads the chunk identified by the given hash and chunk id.
    ///
    /// # Arguments
    ///
    /// * `hash` - The hash identifying the data.
    /// * `chunk_id` - The id of the chunk to download.
    ///
    /// # Returns
    ///
    /// A `Result` containing the downloaded chunk bytes, or an `Error` if the download fails.
    async fn download_chunk(&self, hash: &str, chunk_id: Self::ChunkId) -> Result<Bytes, Error>;
}
