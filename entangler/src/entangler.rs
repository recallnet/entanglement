// Copyright 2024 Entanglement Contributors
// SPDX-License-Identifier: Apache-2.0, MIT

use anyhow::Result;
use bytes::{Bytes, BytesMut};
use futures::TryStreamExt;
use futures::{Stream, StreamExt};
use recall_entangler_storage::{self, ChunkIdMapper, Error as StorageError, Storage, UploadResult};
use std::collections::HashMap;
use std::pin::Pin;

use crate::executer;
use crate::grid::Positioner;
use crate::repairer::{self, Repairer};
use crate::stream::{RepairingChunkStream, RepairingStream};
use crate::Config;
use crate::Metadata;

pub const CHUNK_SIZE: u64 = 1024;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Invalid parameter {0}: {1}")]
    InvalidEntanglementParameter(String, u8),

    #[error("Input vector is empty")]
    EmptyInput,

    #[error("Failed to download a blob with hash {hash}: {source}")]
    BlobDownload {
        hash: String,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[error("Failed to download chunks {chunks:?} for blob with hash {hash}: {source}")]
    ChunksDownload {
        hash: String,
        chunks: Vec<String>,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[error("Storage error: {0}")]
    Storage(#[from] StorageError),

    #[error("Failed to parse metadata: {0}")]
    ParsingMetadata(#[from] serde_json::Error),

    #[error("Error during entanglement execution: {0}")]
    Execution(#[from] executer::Error),

    #[error("Error occurred: {0}")]
    Other(#[from] anyhow::Error),

    #[error("Repairing failed: {0}")]
    Repair(#[from] repairer::Error),
}

pub type ByteStream = Pin<Box<dyn Stream<Item = Result<Bytes, Error>> + Send>>;

pub type ChunkStream<T> = Pin<Box<dyn Stream<Item = (T, Result<Bytes, Error>)> + Send>>;

/// Result of the entanglement operation.
#[derive(Debug, Clone)]
pub struct EntanglementResult {
    /// The hash of the original blob.
    pub orig_hash: String,
    /// The hash of the metadata blob.
    pub metadata_hash: String,
    /// Results from all storage uploads (original blob in case of `upload`, parity blobs, and metadata blob).
    pub upload_results: Vec<UploadResult>,
}

/// The `Entangler` struct is responsible for managing the entanglement process of data chunks.
/// It interacts with a storage backend to upload and download data, and ensures data integrity
/// through the use of parity chunks.
#[derive(Clone)]
pub struct Entangler<T: Storage + 'static> {
    pub(crate) storage: T,
    pub(crate) config: Config,
}

/// Represents a range of chunks to download.
/// All ranges are inclusive.
#[derive(Debug, Copy, Clone)]
pub enum ChunkRange {
    /// Range of chunks starting from the given index till the end.
    From(u64),
    /// Range of chunks starting from the beginning till the given index inclusive.
    Till(u64),
    /// Range of chunks between the given indices inclusive.
    Between(u64, u64),
}

impl ChunkRange {
    /// Converts the range to a tuple of the beginning and end indices.
    /// The end index is exclusive, i.e. the range is `[begin, end)`.
    fn to_beg_end(self) -> (u64, Option<u64>) {
        match self {
            ChunkRange::From(first) => (first, None),
            ChunkRange::Till(last) => (0, Some(last + 1)),
            ChunkRange::Between(first, last) => (first, Some(last + 1)),
        }
    }
}

impl<T: Storage> Entangler<T> {
    /// Creates a new `Entangler` instance with the given storage backend and configuration.
    ///
    /// # Arguments
    ///
    /// * `storage` - The storage backend to use.
    /// * `conf` - The configuration to use.
    ///
    /// # Returns
    ///
    /// A new `Entangler` instance.
    ///
    /// See also [storage]
    pub fn new(storage: T, conf: Config) -> Result<Self, Error> {
        if conf.alpha == 0 || conf.alpha > 3 || conf.s == 0 {
            return Err(Error::InvalidEntanglementParameter(
                (if conf.s == 0 { "s" } else { "alpha" }).to_string(),
                if conf.s == 0 { conf.s } else { conf.alpha },
            ));
        }
        Ok(Self {
            storage,
            config: conf,
        })
    }

    /// Creates entangled parity blobs for the given data and uploads them to the storage backend.
    /// The original data is also uploaded to the storage backend.
    ///
    /// # Arguments
    ///
    /// * `stream` - The data stream to upload.
    ///
    /// # Returns
    ///
    /// An `EntanglementResult` containing the hash of the original data, the hash of the metadata,
    /// and all upload results from the underlying storage.
    pub async fn upload<S, E>(&self, stream: S) -> Result<EntanglementResult>
    where
        S: Stream<Item = Result<Bytes, E>> + Send + Unpin + 'static,
        E: std::error::Error + Send + Sync + 'static,
    {
        let mut upload_results = Vec::new();

        let orig_upload_result = self.storage.upload_bytes(stream).await?;
        upload_results.push(orig_upload_result.clone());

        let mut entanglement_result = self
            .entangle_uploaded(orig_upload_result.hash.clone())
            .await?;
        entanglement_result
            .upload_results
            .insert(0, orig_upload_result);
        Ok(entanglement_result)
    }

    /// Creates entangled parity blobs for the given data and uploads them to the storage backend.
    /// Returns the hash of the metadata and the upload results for parity blobs and metadata.
    async fn entangle<S, E>(&self, stream: S, hash: String) -> Result<(String, Vec<UploadResult>)>
    where
        S: Stream<Item = Result<Bytes, E>> + Send + Unpin + 'static,
        E: std::error::Error + Send + Sync + 'static,
    {
        let executer =
            executer::Executer::from_config(&self.config).with_chunk_size(CHUNK_SIZE as usize);

        let parity_streams = executer.entangle(stream).await?;

        let mut parity_hashes = Vec::new();
        let mut upload_results = Vec::new();

        // Upload each parity stream concurrently
        let mut upload_tasks = Vec::new();
        for stream in parity_streams {
            let storage = self.storage.clone();
            let task = tokio::spawn(async move { storage.upload_bytes(stream).await });
            upload_tasks.push(task);
        }

        // Wait for all uploads to complete
        for task in upload_tasks {
            let upload_result = task.await.map_err(|e| Error::Other(e.into()))??;
            parity_hashes.push(upload_result.hash.clone());
            upload_results.push(upload_result);
        }

        let num_bytes = self.storage.get_blob_size(&hash).await?;
        let metadata = Metadata {
            orig_hash: hash,
            num_bytes,
            parity_hashes,
            chunk_size: CHUNK_SIZE,
            s: self.config.s,
            p: self.config.s,
        };

        let metadata = serde_json::to_string(&metadata).unwrap();
        let metadata_stream = bytes_to_stream(Bytes::from(metadata));
        let metadata_result = self.storage.upload_bytes(metadata_stream).await?;
        upload_results.push(metadata_result.clone());

        Ok((metadata_result.hash, upload_results))
    }

    /// Entangles the uploaded data identified by the given hash, uploads entangled parity blobs
    /// to the storage backend, and returns an EntanglementResult containing the hash of the original data,
    /// the hash of the metadata, and all upload results.
    ///
    /// # Arguments
    ///
    /// * `hash` - The hash of the data to entangle.
    ///
    /// # Returns
    ///
    /// An `EntanglementResult` containing the hash of the original data, the hash of the metadata,
    /// and all upload results (parity blobs and metadata blob).
    pub async fn entangle_uploaded(&self, hash: String) -> Result<EntanglementResult> {
        let orig_data_stream = self.storage.download_bytes(&hash).await?;
        let (metadata_hash, upload_results) = self.entangle(orig_data_stream, hash.clone()).await?;

        Ok(EntanglementResult {
            orig_hash: hash,
            metadata_hash,
            upload_results,
        })
    }

    /// Downloads the data identified by the given hash. If the data is corrupted, it attempts to
    /// repair the data using the parity blobs identified by the metadata hash.
    ///
    /// # Arguments
    ///
    /// * `hash` - The hash of the data to download.
    /// * `metadata_hash` - The hash of the metadata.
    ///
    /// # Returns
    ///
    /// A `Result` containing a stream of bytes.
    pub async fn download(
        &self,
        hash: &str,
        metadata_hash: Option<&str>,
    ) -> Result<ByteStream, Error> {
        match (self.storage.download_bytes(hash).await, metadata_hash) {
            (Ok(stream), None) => Ok(Box::pin(stream.map_err(Error::from))),
            (Ok(stream), Some(metadata_hash)) => Ok(Box::pin(RepairingStream::new(
                self.clone(),
                hash.to_string(),
                metadata_hash.to_string(),
                stream,
            ))),
            (Err(e), None) => Err(Error::BlobDownload {
                hash: hash.to_string(),
                source: e.into(),
            }),
            (Err(_), Some(metadata_hash)) => {
                let metadata = self.download_metadata(metadata_hash).await?;
                let repaired_stream = self.download_repaired(hash, metadata).await?;
                Ok(Box::pin(repaired_stream))
            }
        }
    }

    /// Downloads a range of chunks of the data identified by the given hash as a stream. If the data is
    /// corrupted, it attempts to repair the data using the parity blobs identified by the metadata
    /// hash.
    ///
    /// # Arguments
    ///
    /// * `hash` - The hash of the data to download.
    /// * `chunk_range` - The range of chunks to download.
    /// * `metadata_hash` - The hash of the metadata.
    ///
    /// # Returns
    ///
    /// A `Result` containing a stream of the downloaded bytes.
    ///
    /// See [ChunkRange] for more information on the range of chunks.
    pub async fn download_range(
        &self,
        hash: &str,
        chunk_range: ChunkRange,
        metadata_hash: Option<String>,
    ) -> Result<ByteStream, Error> {
        let (beg, end) = chunk_range.to_beg_end();

        let mut index = beg;
        let mut chunk_ids = Vec::new();
        let mapper = self.storage.chunk_id_mapper(hash).await?;
        while let Ok(chunk_id) = mapper.index_to_id(index) {
            chunk_ids.push(chunk_id);
            index += 1;
            if end.is_some() && index == end.unwrap() {
                break;
            }
        }

        let chunk_stream = self.download_chunks(hash.to_string(), chunk_ids, metadata_hash)?;

        let byte_stream = chunk_stream.map(|(_, bytes)| bytes);

        Ok(Box::pin(byte_stream))
    }

    /// Downloads the chunks with specific ids of the data identified by the given hash as a stream. If the data
    /// is corrupted, it attempts to repair the data using the parity blobs identified by the
    /// metadata hash.
    ///
    /// # Arguments
    ///
    /// * `hash` - The hash of the data to download.
    /// * `chunk_ids` - The ids of the chunks to download.
    /// * `metadata_hash` - The hash of the metadata.
    ///
    /// # Returns
    ///
    /// A `Result` containing a stream of chunk ids and the downloaded data.
    /// The chunks are guaranteed to be in the same order as the input chunk ids.
    pub fn download_chunks(
        &self,
        hash: String,
        chunk_ids: Vec<T::ChunkId>,
        metadata_hash: Option<String>,
    ) -> Result<ChunkStream<T::ChunkId>, Error> {
        if chunk_ids.is_empty() {
            return Err(Error::EmptyInput);
        }

        if let Some(metadata_hash) = metadata_hash {
            Ok(Box::pin(RepairingChunkStream::new(
                self.clone(),
                hash,
                metadata_hash,
                chunk_ids,
            )))
        } else {
            Ok(Box::pin(futures::stream::unfold(
                (chunk_ids, 0, hash, self.clone()),
                |(chunk_ids, i, hash, ent)| async move {
                    if i < chunk_ids.len() {
                        let chunk_id = chunk_ids[i].clone();
                        let result = ent
                            .storage
                            .download_chunk(&hash, chunk_id.clone())
                            .await
                            .map_err(Error::from);
                        Some(((chunk_id, result), (chunk_ids, i + 1, hash, ent)))
                    } else {
                        None
                    }
                },
            )))
        }
    }

    pub async fn download_metadata(&self, metadata_hash: &str) -> Result<Metadata, Error> {
        let stream = self.storage.download_bytes(metadata_hash).await?;
        Ok(serde_json::from_slice(&read_stream(stream).await?)?)
    }

    /// Downloads the data identified by the given hash and attempts to repair it using the parity
    /// blobs identified by the metadata hash. Returns a stream of the repaired data.
    /// It downloads the original blob chunk-by-chunk and tries to repair the missing chunks.
    pub(crate) async fn download_repaired(
        &self,
        hash: &str,
        metadata: Metadata,
    ) -> Result<ByteStream, Error> {
        match self.storage.iter_chunks(hash).await {
            Ok(mut stream) => {
                let mut available_chunks = Vec::new();
                let mut missing_chunks = Vec::new();
                let mapper = self.storage.chunk_id_mapper(hash).await?;

                while let Some((chunk_id, res)) = stream.next().await {
                    match res {
                        Ok(chunk) => {
                            available_chunks.push((chunk_id.clone(), chunk));
                        }
                        Err(_) => {
                            missing_chunks.push(chunk_id.clone());
                        }
                    }
                }

                let repaired_chunks = self
                    .repair_chunks(metadata.clone(), missing_chunks, mapper.clone())
                    .await?;

                let mut all_chunks = HashMap::new();
                for (chunk_id, chunk) in available_chunks {
                    all_chunks.insert(chunk_id, chunk);
                }
                all_chunks.extend(repaired_chunks);

                let num_chunks = metadata.num_bytes.div_ceil(metadata.chunk_size);

                // Create a stream from the chunks directly
                let chunk_stream = futures::stream::iter((0..num_chunks).map(move |index| {
                    match mapper.index_to_id(index) {
                        Ok(chunk_id) => match all_chunks.get(&chunk_id) {
                            Some(chunk) => Ok(chunk.clone()),
                            None => {
                                Err(Error::Other(anyhow::anyhow!("Missing chunk after repair")))
                            }
                        },
                        Err(e) => Err(Error::from(e)),
                    }
                }));

                let upload_stream = chunk_stream
                    .map(|result| result.map_err(|e| std::io::Error::other(e.to_string())));

                self.storage.upload_bytes(upload_stream).await?;

                match self.storage.download_bytes(hash).await {
                    Ok(stream) => Ok(Box::pin(stream.map_err(Error::from))),
                    Err(e) => Err(Error::from(e)),
                }
            }
            Err(e) => Err(Error::from(e)),
        }
    }

    pub(crate) async fn repair_chunks(
        &self,
        metadata: Metadata,
        missing_indexes: Vec<T::ChunkId>,
        mapper: T::ChunkIdMapper,
    ) -> std::result::Result<HashMap<T::ChunkId, Bytes>, Error> {
        let positioner = Positioner::new(
            metadata.s as u64,
            metadata.num_bytes.div_ceil(metadata.chunk_size),
        );
        Repairer::new(&self.storage, positioner, metadata, mapper)
            .repair_chunks(missing_indexes.clone())
            .await
            .map_err(Error::Repair)
    }
}

/// Reads an entire stream into a Bytes object.
///
/// # Arguments
///
/// * `stream` - The stream to read from.
///
/// # Returns
///
/// A `Result` containing the bytes read from the stream.
pub async fn read_stream<S, E>(mut stream: S) -> Result<Bytes, anyhow::Error>
where
    S: Stream<Item = Result<Bytes, E>> + Unpin,
    E: std::error::Error + Send + Sync + 'static,
{
    let mut bytes = BytesMut::with_capacity(stream.size_hint().0);
    while let Some(chunk) = stream.next().await {
        bytes.extend_from_slice(&chunk?);
    }
    Ok(bytes.freeze())
}

fn bytes_to_stream(bytes: Bytes) -> ByteStream {
    Box::pin(futures::stream::once(
        async move { Ok::<Bytes, Error>(bytes) },
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use recall_entangler_storage::mock::{DummyStorage, SpyStorage};

    #[test]
    fn test_entangler_new_valid_parameters() {
        let storage = DummyStorage;
        let result = Entangler::new(storage, Config::new(3, 2));
        assert!(result.is_ok());
        let entangler = result.unwrap();
        assert_eq!(entangler.config.alpha, 3);
        assert_eq!(entangler.config.s, 2);
    }

    #[test]
    fn test_entangler_new_alpha_zero() {
        let storage = DummyStorage;
        let result = Entangler::new(storage, Config::new(0, 2));
        assert!(result.is_err());
        assert!(matches!(
            result.err().unwrap(),
            Error::InvalidEntanglementParameter(param, value) if param == "alpha" && value == 0
        ));
    }

    #[test]
    fn test_entangler_new_alpha_too_large() {
        let storage = DummyStorage;
        let result = Entangler::new(storage, Config::new(4, 2));
        assert!(result.is_err());
        assert!(matches!(
            result.err().unwrap(),
            Error::InvalidEntanglementParameter(param, value) if param == "alpha" && value == 4
        ));
    }

    #[test]
    fn test_entangler_new_s_zero() {
        let storage = DummyStorage;
        let result = Entangler::new(storage, Config::new(3, 0));
        assert!(result.is_err());
        assert!(matches!(
            result.err().unwrap(),
            Error::InvalidEntanglementParameter(param, value) if param == "s" && value == 0
        ));
    }

    #[tokio::test]
    async fn test_if_download_fails_it_should_upload_to_storage_after_repair() {
        let hash = "hash".to_string();
        let m_hash = "metadata_hash".to_string();
        let chunks = ["chunk0", "chunk1", "chunk2"];
        let repaired_data = Bytes::from(chunks.join(""));

        let mut storage = SpyStorage::default();
        // First download attempt fails
        storage.stub_download_bytes(
            Some(hash.clone()),
            Err(StorageError::BlobNotFound(hash.clone())),
        );
        // Second download attempt (after repair) succeeds
        storage.stub_download_bytes(Some(hash.clone()), Ok(repaired_data.clone()));

        storage.stub_iter_chunks(
            chunks
                .iter()
                .enumerate()
                .map(|(i, chunk)| (i as u64, Ok(Bytes::from(*chunk))))
                .collect(),
        );

        let metadata = Metadata {
            orig_hash: hash.clone(),
            num_bytes: 18,
            parity_hashes: Vec::new(),
            chunk_size: 6,
            s: 3,
            p: 3,
        };
        let json = serde_json::json!(metadata).to_string();
        storage.stub_download_bytes(Some(m_hash.clone()), Ok(Bytes::from(json)));

        let entangler = Entangler::new(storage.clone(), Config::new(3, 3)).unwrap();

        let result = entangler.download(&hash, Some(&m_hash)).await;
        assert!(result.is_ok());

        let calls = storage.upload_bytes_calls();
        assert_eq!(
            calls.len(),
            1,
            "Expected 1 call to upload_bytes, got {:?}",
            calls.len()
        );
        assert_eq!(calls[0], repaired_data, "Unexpected data uploaded");
    }
}
