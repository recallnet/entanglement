// Copyright 2024 Entanglement Contributors
// SPDX-License-Identifier: Apache-2.0, MIT

use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use futures::Stream;
use std::{fmt::Display, pin::Pin};
use thiserror;

#[cfg(any(test, feature = "mock"))]
type ClonableError = String;
#[cfg(not(any(test, feature = "mock")))]
type ClonableError = anyhow::Error;

#[cfg_attr(any(test, feature = "mock"), derive(Clone))]
/// Error type for storage operations.
#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// Error indicating that a blob with the specified hash was not found.
    #[error("Blob with hash {0} not found")]
    BlobNotFound(String),
    /// Error indicating that a chunk with the specified id was not found in the specified blob.
    #[error("Chunk with id {0} not found in blob {1}: {2}")]
    ChunkNotFound(String, String, ClonableError),

    /// General storage error.
    #[error("Storage error: {0}")]
    StorageError(ClonableError),

    /// Error indicating that the provided hash is invalid.
    #[error("Invalid hash {0}. Error: {1}")]
    InvalidHash(String, String),

    /// A catch-all error for other types of errors.
    #[error("Error occurred: {0}")]
    Other(ClonableError),
}

pub fn wrap_error(err: anyhow::Error) -> ClonableError {
    #[cfg(any(test, feature = "mock"))]
    return err.to_string();
    #[cfg(not(any(test, feature = "mock")))]
    return err;
}

/// Trait used to identify chunks.
pub trait ChunkId:
    Clone + Default + PartialEq + Eq + std::hash::Hash + Display + Send + Sync
{
}

/// Type alias for a stream of bytes.
pub type ByteStream = Pin<Box<dyn Stream<Item = Result<Bytes, Error>> + Send>>;

/// Type alias for a stream of chunks with associated chunk IDs.
pub type ChunkStream<T> = Pin<Box<dyn Stream<Item = (T, Result<Bytes, Error>)> + Send>>;

/// Trait for mapping chunk indices to chunk ids and vice versa.
///
/// The methods in this trait can be called multiple times with the same arguments so
/// the implementation should be idempotent and probably cache the results.
pub trait ChunkIdMapper<T: ChunkId>: Clone + Send {
    /// Returns a chunk id corresponding to the given index.
    fn index_to_id(&self, index: u64) -> Result<T, Error>;
    /// Returns the index corresponding to the given chunk id.
    fn id_to_index(&self, chunk_id: &T) -> Result<u64, Error>;
}

/// Result of an upload operation.
#[derive(Debug, PartialEq, Clone)]
pub struct UploadResult {
    /// The hash of the uploaded blob.
    pub hash: String,
    /// Size of the uploaded blob in bytes.
    pub size: u64,
    /// Additional storage-specific information.
    pub info: std::collections::HashMap<String, String>,
}

/// Trait representing a storage backend.
#[async_trait]
pub trait Storage: Send + Sync + Clone {
    type ChunkId: ChunkId;
    type ChunkIdMapper: ChunkIdMapper<Self::ChunkId>;

    /// Uploads the given bytes to the storage and returns a result identifying the stored data.
    ///
    /// # Arguments
    ///
    /// * `stream` - A stream of bytes to upload.
    ///
    /// # Returns
    ///
    /// A `Result` containing the upload result with hash of the uploaded data and additional info,
    /// or an error if the upload fails.
    async fn upload_bytes<S, E>(&self, stream: S) -> Result<UploadResult, Error>
    where
        S: Stream<Item = Result<Bytes, E>> + Send + Unpin + 'static,
        E: std::error::Error + Send + Sync + 'static;

    /// Downloads the bytes identified by the given hash as a stream of bytes.
    ///
    /// # Arguments
    ///
    /// * `hash` - The hash identifying the data to download.
    ///
    /// # Returns
    ///
    /// A `Result` containing a stream of the downloaded bytes, or an `Error` if the download fails.
    async fn download_bytes(&self, hash: &str) -> Result<ByteStream, Error>;

    /// Returns a stream of chunks for the data identified by the given hash.
    ///
    /// # Arguments
    ///
    /// * `hash` - The hash identifying the data.
    ///
    /// # Returns
    ///
    /// A `Result` containing a stream of chunks, or an `Error` if the operation fails.
    async fn iter_chunks(&self, hash: &str) -> Result<ChunkStream<Self::ChunkId>, Error>;

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

    /// Returns a chunk id mapper for the blob identified by the given hash.
    /// The chunk id mapper is used to map chunk indices to chunk ids and vice versa.
    ///
    /// # Arguments
    ///
    /// * `hash` - The hash identifying the blob.
    ///
    /// # Returns
    ///
    /// A `Result` containing a chunk id mapper, or an `Error` if the operation fails.
    async fn chunk_id_mapper(&self, hash: &str) -> Result<Self::ChunkIdMapper, Error>;

    /// Returns the size of the blob identified by the given hash.
    ///
    /// # Arguments
    ///
    /// * `hash` - The hash identifying the blob.
    ///
    /// # Returns
    ///
    /// A `Result` containing the size of the blob in bytes, or an `Error` if the operation fails.
    async fn get_blob_size(&self, hash: &str) -> Result<u64, Error>;
}
