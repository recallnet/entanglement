// Copyright 2024 Entanglement Contributors
// SPDX-License-Identifier: Apache-2.0, MIT

use anyhow::Result;
use bytes::{Bytes, BytesMut};
use futures::TryStreamExt;
use futures::{Stream, StreamExt};
use std::collections::HashMap;
use std::pin::Pin;
use storage::{self, ChunkIdMapper, Error as StorageError, Storage};

use crate::executer;
use crate::grid::{Grid, Positioner};
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

    #[error("Error occurred: {0}")]
    Other(#[from] anyhow::Error),

    #[error("Repairing failed: {0}")]
    Repair(#[from] repairer::Error),
}

pub type ByteStream = Pin<Box<dyn Stream<Item = Result<Bytes, Error>> + Send>>;

pub type ChunkStream<T> = Pin<Box<dyn Stream<Item = (T, Result<Bytes, Error>)> + Send>>;

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
    /// Download chunks starting from the given index till the end.
    From(u64),
    /// Download chunks starting from the beginning till the given index inclusive.
    Till(u64),
    /// Download chunks between the given indices inclusive.
    Between(u64, u64),
}

impl<T: Storage> Entangler<T> {
    /// Creates a new `Entangler` instance with the given storage backend, alpha, s, and p parameters.
    ///
    /// # Arguments
    ///
    /// * `storage` - The storage backend to use.
    /// * `alpha` - The number of parity chunks to generate for each data chunk.
    /// * `s` - The number of horizontal strands in the grid.
    /// * `p` - The number of helical strands in the grid.
    ///
    /// # Returns
    ///
    /// A new `Entangler` instance.
    ///
    /// See also [storage]
    pub fn new(storage: T, conf: Config) -> Result<Self, Error> {
        if conf.alpha == 0 || conf.s == 0 {
            return Err(Error::InvalidEntanglementParameter(
                (if conf.alpha == 0 { "alpha" } else { "s" }).to_string(),
                if conf.alpha == 0 { conf.alpha } else { conf.s },
            ));
        }
        // at the moment it's not clear how to take a helical strand around the cylinder so that
        // it completes a revolution after LW on the same horizontal strand. That's why
        // p should be a multiple of s.
        if conf.p != 0 && (conf.p < conf.s || conf.p % conf.s != 0) {
            return Err(Error::InvalidEntanglementParameter("p".to_string(), conf.p));
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
    /// * `bytes` - The data to upload.
    ///
    /// # Returns
    ///
    /// The hash of the original data and the hash of the metadata.
    pub async fn upload(&self, bytes: impl Into<Bytes> + Send) -> Result<(String, String)> {
        let bytes: Bytes = bytes.into();
        let orig_hash = self.storage.upload_bytes(bytes.clone()).await?;
        let metadata_hash = self.entangle(bytes, orig_hash.clone()).await?;
        Ok((orig_hash, metadata_hash))
    }

    /// Creates entangled parity blobs for the given data and uploads them to the storage backend.
    /// The original data is also uploaded to the storage backend.
    /// Returns the hash of the original data and the hash of the metadata.
    async fn entangle(&self, bytes: Bytes, hash: String) -> Result<String> {
        let num_bytes = bytes.len();

        let chunks = bytes_to_chunks(bytes, CHUNK_SIZE);
        let num_chunks = chunks.len() as u64;

        let orig_grid = Grid::new(chunks, u64::min(self.config.s as u64, num_chunks))?;

        let exec = executer::Executer::new(self.config.alpha);

        let mut parity_hashes = HashMap::new();
        for parity_grid in exec.iter_parities(orig_grid) {
            let data = parity_grid.grid.assemble_data();
            let parity_hash = self.storage.upload_bytes(data).await?;
            parity_hashes.insert(parity_grid.strand_type, parity_hash);
        }

        let metadata = Metadata {
            orig_hash: hash,
            parity_hashes,
            num_bytes: num_bytes as u64,
            chunk_size: CHUNK_SIZE,
            s: self.config.s,
            p: self.config.s,
        };

        let metadata = serde_json::to_string(&metadata).unwrap();
        let metadata_hash = self.storage.upload_bytes(metadata).await?;

        Ok(metadata_hash)
    }

    /// Entangles the uploaded data identified by the given hash, uploads entangled parity blobs
    /// to the storage backend, and returns the hash of the metadata. [Metadata]
    ///
    /// # Arguments
    ///
    /// * `hash` - The hash of the data to entangle.
    ///
    /// # Returns
    ///
    /// The hash of the metadata.
    pub async fn entangle_uploaded(&self, hash: String) -> Result<String> {
        let orig_data_stream = self.storage.download_bytes(&hash).await?;
        self.entangle(read_stream(orig_data_stream).await?, hash)
            .await
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

    /// Downloads a range of chunks of the data identified by the given hash. If the data is
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
    /// The downloaded data.
    ///
    /// See [ChunkRange] for more information on the range of chunks.
    pub async fn download_range(
        &self,
        hash: &str,
        chunk_range: ChunkRange,
        metadata_hash: Option<&str>,
    ) -> Result<ByteStream, Error> {
        let (beg, end) = match chunk_range {
            ChunkRange::From(first) => (first, None),
            ChunkRange::Till(last) => (0, Some(last + 1)),
            ChunkRange::Between(first, last) => (first, Some(last + 1)),
        };

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

        let chunk_stream = self
            .download_chunks(hash.to_string(), chunk_ids, metadata_hash)
            .await?;

        let byte_stream = chunk_stream.map(|item| match item {
            (_, bytes) => bytes,
        });

        Ok(Box::pin(byte_stream))
    }

    /// Downloads the chunks with specific ids of the data identified by the given hash. If the data
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
    /// A map of chunk ids to the downloaded data.
    ///
    /// # Note
    ///
    /// The caller is responsible for ensuring that the chunks fit into the memory.
    pub async fn download_chunks(
        &self,
        hash: String,
        chunk_ids: Vec<T::ChunkId>,
        metadata_hash: Option<&str>,
    ) -> Result<ChunkStream<T::ChunkId>, Error> {
        if chunk_ids.is_empty() {
            return Err(Error::EmptyInput);
        }

        if let Some(metadata_hash) = metadata_hash {
            Ok(Box::pin(RepairingChunkStream::new(
                self.clone(),
                hash,
                metadata_hash.to_string(),
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
                        Some(((chunk_id.clone(), result), (chunk_ids, i + 1, hash, ent)))
                    } else {
                        None
                    }
                },
            )))
        }
    }

    pub(crate) async fn download_metadata(&self, metadata_hash: &str) -> Result<Metadata, Error> {
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

                let num_chunks =
                    (metadata.num_bytes + metadata.chunk_size - 1) / metadata.chunk_size;
                let mut data = BytesMut::with_capacity((num_chunks * CHUNK_SIZE) as usize);
                for index in 0..num_chunks {
                    let chunk_id = mapper.index_to_id(index)?;
                    if let Some(chunk) = all_chunks.get(&chunk_id) {
                        data.extend_from_slice(chunk);
                    } else {
                        return Err(Error::Other(anyhow::anyhow!("Missing chunk after repair")));
                    }
                }

                let owned_data = data.freeze();

                self.storage.upload_bytes(owned_data.clone()).await?;

                let stream = futures::stream::iter((0..num_chunks).map(move |i| {
                    let start = i as usize * CHUNK_SIZE as usize;
                    let end = (start + CHUNK_SIZE as usize).min(owned_data.len());
                    Ok(owned_data.slice(start..end))
                }));

                Ok(Box::pin(stream))
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
            (metadata.num_bytes + metadata.chunk_size - 1) / metadata.chunk_size,
        );
        Repairer::new(&self.storage, positioner, metadata, mapper)
            .repair_chunks(missing_indexes.clone())
            .await
            .map_err(Error::Repair)
    }
}

async fn read_stream(mut stream: storage::ByteStream) -> Result<Bytes, anyhow::Error> {
    let mut bytes = BytesMut::with_capacity(stream.size_hint().0);
    while let Some(chunk) = stream.next().await {
        bytes.extend_from_slice(&chunk?);
    }
    Ok(bytes.freeze())
}

fn bytes_to_chunks(bytes: Bytes, chunk_size: u64) -> Vec<Bytes> {
    let chunk_size = chunk_size as usize;
    let mut chunks = Vec::with_capacity((bytes.len() + chunk_size - 1) / chunk_size);
    let mut start = 0;

    while start < bytes.len() {
        let end = std::cmp::min(start + chunk_size, bytes.len());
        chunks.push(bytes.slice(start..end));
        start = end;
    }

    // if last chunk is smaller than chunk_size, add padding
    if let Some(last_chunk) = chunks.last_mut() {
        *last_chunk = add_padding(last_chunk, chunk_size);
    }

    chunks
}

fn add_padding(chunk: &Bytes, chunk_size: usize) -> Bytes {
    let mut chunk = chunk.to_vec();
    chunk.resize(chunk_size, 0);
    Bytes::from(chunk)
}

#[cfg(test)]
mod tests {
    use super::*;
    use storage::mock::{DummyStorage, SpyStorage};

    #[test]
    fn test_entangler_new_valid_parameters() {
        let storage = DummyStorage;
        let result = Entangler::new(storage, Config::new(3, 2, 4));
        assert!(result.is_ok());
        let entangler = result.unwrap();
        assert_eq!(entangler.config.alpha, 3);
        assert_eq!(entangler.config.s, 2);
    }

    #[test]
    fn test_entangler_new_alpha_zero() {
        let storage = DummyStorage;
        let result = Entangler::new(storage, Config::new(0, 2, 4));
        assert!(result.is_err());
        assert!(matches!(
            result.err().unwrap(),
            Error::InvalidEntanglementParameter(param, value) if param == "alpha" && value == 0
        ));
    }

    #[test]
    fn test_entangler_new_s_zero() {
        let storage = DummyStorage;
        let result = Entangler::new(storage, Config::new(3, 0, 4));
        assert!(result.is_err());
        assert!(matches!(
            result.err().unwrap(),
            Error::InvalidEntanglementParameter(param, value) if param == "s" && value == 0
        ));
    }

    #[test]
    fn test_entangler_new_p_less_than_s() {
        let storage = DummyStorage;
        let result = Entangler::new(storage, Config::new(3, 4, 2));
        assert!(result.is_err());
        assert!(matches!(
            result.err().unwrap(),
            Error::InvalidEntanglementParameter(param, value) if param == "p" && value == 2
        ));
    }

    #[test]
    fn test_entangler_new_p_not_multiple_of_s() {
        let storage = DummyStorage;
        let result = Entangler::new(storage, Config::new(3, 3, 7));
        assert!(result.is_err());
        assert!(matches!(
            result.err().unwrap(),
            Error::InvalidEntanglementParameter(param, value) if param == "p" && value == 7
        ));
    }

    #[test]
    fn test_entangler_new_p_zero() {
        let storage = DummyStorage;
        let result = Entangler::new(storage, Config::new(3, 2, 0));
        assert!(result.is_ok());
        let entangler = result.unwrap();
        assert_eq!(entangler.config.alpha, 3);
        assert_eq!(entangler.config.s, 2);
    }

    #[test]
    fn test_entangler_new_p_valid_multiple_of_s() {
        let storage = DummyStorage;
        let result = Entangler::new(storage, Config::new(3, 2, 6));
        assert!(result.is_ok());
        let entangler = result.unwrap();
        assert_eq!(entangler.config.alpha, 3);
        assert_eq!(entangler.config.s, 2);
    }

    #[tokio::test]
    async fn test_if_download_fails_it_should_upload_to_storage_after_repair() {
        let hash = "hash".to_string();
        let m_hash = "metadata_hash".to_string();
        let chunks = vec!["chunk0", "chunk1", "chunk2"];

        let mut storage = SpyStorage::default();
        storage.stub_download_bytes(
            Some(hash.clone()),
            Err(StorageError::BlobNotFound(hash.clone())),
        );
        storage.stub_iter_chunks(
            chunks
                .iter()
                .enumerate()
                .map(|(i, chunk)| (i as u64, Ok(Bytes::from(*chunk))))
                .collect(),
        );

        let metadata = Metadata {
            orig_hash: hash.clone(),
            parity_hashes: HashMap::new(),
            num_bytes: 18,
            chunk_size: 6,
            s: 3,
            p: 3,
        };
        let json = serde_json::json!(metadata).to_string();
        storage.stub_download_bytes(Some(m_hash.clone()), Ok(Bytes::from(json)));

        let entangler = Entangler::new(storage.clone(), Config::new(3, 3, 3)).unwrap();

        let result = entangler.download(&hash, Some(&m_hash)).await;
        assert!(result.is_ok());

        let calls = storage.upload_bytes_calls();
        assert_eq!(
            calls.len(),
            1,
            "Expected 1 call to upload_bytes, got {:?}",
            calls.len()
        );
        assert_eq!(
            calls[0],
            Bytes::from(chunks.into_iter().collect::<String>()),
            "Unexpected data uploaded"
        );
    }
}
