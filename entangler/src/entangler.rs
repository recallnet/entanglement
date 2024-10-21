// Copyright 2024 Entanglement Contributors
// SPDX-License-Identifier: Apache-2.0, MIT

use anyhow::Result;
use bytes::{Bytes, BytesMut};
use futures::StreamExt;
use std::collections::HashMap;
use storage::{ByteStream, ChunkIdMapper, Error as StorageError, Storage};

use crate::executer;
use crate::grid::{Grid, Positioner};
use crate::repairer::{self, Repairer};
use crate::Metadata;

const CHUNK_SIZE: u64 = 1024;

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

/// The `Entangler` struct is responsible for managing the entanglement process of data chunks.
/// It interacts with a storage backend to upload and download data, and ensures data integrity
/// through the use of parity chunks.
pub struct Entangler<T: Storage> {
    storage: T,
    alpha: u8,
    s: u8,
}

pub enum ChunkRange {
    From(u64),
    Till(u64),
    Between(u64, u64),
}

impl<T: Storage> Entangler<T> {
    /// Creates a new `Entangler` instance with the given storage backend, alpha, s, and p parameters.
    /// The alpha parameter determines the number of parity chunks to generate for each data chunk.
    /// The s parameter determines the number of horizontal strands in the grid.
    /// The p parameter determines the number of helical strands in the grid.
    pub fn new(storage: T, alpha: u8, s: u8, p: u8) -> Result<Self, Error> {
        if alpha == 0 || s == 0 {
            return Err(Error::InvalidEntanglementParameter(
                (if alpha == 0 { "alpha" } else { "s" }).to_string(),
                if alpha == 0 { alpha } else { s },
            ));
        }
        // at the moment it's not clear how to take a helical strand around the cylinder so that
        // it completes a revolution after LW on the same horizontal strand. That's why
        // p should be a multiple of s.
        if p != 0 && (p < s || p % s != 0) {
            return Err(Error::InvalidEntanglementParameter("p".to_string(), p));
        }
        Ok(Self { storage, alpha, s })
    }

    /// Creates entangled parity blobs for the given data and uploads them to the storage backend.
    /// The original data is also uploaded to the storage backend.
    /// Returns the hash of the original data and the hash of the metadata.
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

        let orig_grid = Grid::new(chunks, u64::min(self.s as u64, num_chunks))?;

        let exec = executer::Executer::new(self.alpha);
        let parities = exec.execute(orig_grid)?;

        let mut parity_hashes = HashMap::new();
        for parity_grid in parities {
            let data = parity_grid.grid.assemble_data();
            let parity_hash = self.storage.upload_bytes(data).await?;
            parity_hashes.insert(parity_grid.strand_type, parity_hash);
        }

        let metadata = Metadata {
            orig_hash: hash,
            parity_hashes,
            num_bytes: num_bytes as u64,
            chunk_size: CHUNK_SIZE as u64,
            s: self.s,
            p: self.s,
        };

        let metadata = serde_json::to_string(&metadata).unwrap();
        let metadata_hash = self.storage.upload_bytes(metadata).await?;

        Ok(metadata_hash)
    }

    /// Entangles the uploaded data identified by the given hash, uploads entangled parity blobs
    /// to the storage backend, and returns the hash of the metadata. [Metadata]
    /// Returns the hash of the metadata.
    pub async fn entangle_uploaded(&self, hash: String) -> Result<String> {
        let orig_data = self.storage.download_bytes(&hash).await?;
        self.entangle(orig_data, hash).await
    }

    /// Downloads the data identified by the given hash. If the data is corrupted, it attempts to
    /// repair the data using the parity blobs identified by the metadata hash.
    /// Returns the downloaded data.
    pub async fn download(&self, hash: &str, metadata_hash: Option<&str>) -> Result<Bytes, Error> {
        match (self.storage.download_bytes(hash).await, metadata_hash) {
            (Ok(data), _) => Ok(data),
            (Err(_), Some(metadata_hash)) => self.download_repaired(hash, metadata_hash).await,
            (Err(e), _) => Err(Error::BlobDownload {
                hash: hash.to_string(),
                source: e.into(),
            }),
        }
    }

    pub async fn download_range(
        &self,
        hash: &str,
        chunk_range: ChunkRange,
        metadata_hash: Option<&str>,
    ) -> Result<Bytes, Error> {
        let (first, last) = match chunk_range {
            ChunkRange::From(first) => (first, None),
            ChunkRange::Till(last) => (0, Some(last)),
            ChunkRange::Between(first, last) => (first, Some(last)),
        };

        let mut index = first;
        let mut chunk_ids = Vec::new();
        let mapper = self.storage.chunk_id_mapper(hash);
        while let Ok(chunk_id) = mapper.index_to_id(index) {
            chunk_ids.push(chunk_id);
            if last.is_some() && index == last.unwrap() {
                break;
            }
            index += 1;
        }

        let chunks = self.download_chunks(hash, chunk_ids, metadata_hash).await?;
        let mut buf = BytesMut::with_capacity(CHUNK_SIZE as usize * chunks.len());
        for i in first..=index {
            let id = mapper.index_to_id(i)?;
            buf.extend_from_slice(&chunks[&id]);
        }

        Ok(buf.into())
    }

    // TODO: this should have a documentation saying that it's up to the caller to ensure that
    // that the chunks fit into the memory.
    pub async fn download_chunks(
        &self,
        hash: &str,
        chunk_ids: Vec<T::ChunkId>,
        metadata_hash: Option<&str>,
    ) -> Result<HashMap<T::ChunkId, Bytes>, Error> {
        let mut chunks = HashMap::new();
        let mut failed_chunks = Vec::new();
        let mut err: Option<StorageError> = None;
        for chunk_id in chunk_ids {
            match self.storage.download_chunk(hash, chunk_id.clone()).await {
                Ok(chunk) => {
                    chunks.insert(chunk_id, chunk);
                }
                Err(e) => {
                    if err.is_none() {
                        err = Some(e);
                    }
                    failed_chunks.push(chunk_id);
                }
            }
        }

        if err.is_none() {
            return Ok(chunks);
        }

        if metadata_hash.is_none() {
            return Err(Error::ChunksDownload {
                hash: hash.to_string(),
                chunks: failed_chunks.iter().map(|c| c.to_string()).collect(),
                source: err.unwrap().into(),
            });
        }

        let metadata = self.download_metadata(metadata_hash.unwrap()).await?;
        let repaired_data = self
            .repair_chunks(metadata, failed_chunks, self.storage.chunk_id_mapper(hash))
            .await?;

        for (chunk_id, chunk) in &repaired_data {
            chunks.insert(chunk_id.clone(), chunk.clone());
        }

        Ok(chunks)
    }

    async fn download_metadata(&self, metadata_hash: &str) -> Result<Metadata, Error> {
        let metadata_bytes = self.storage.download_bytes(metadata_hash).await?;
        Ok(serde_json::from_slice(&metadata_bytes)?)
    }

    /// Downloads the data identified by the given hash and attempts to repair it using the parity
    /// blobs identified by the metadata hash. Returns the repaired data.
    /// It downloads the original blob chunk-by-chunk and tries to repair the missing chunks.
    async fn download_repaired(&self, hash: &str, metadata_hash: &str) -> Result<Bytes, Error> {
        let metadata = self.download_metadata(metadata_hash).await?;

        match self.storage.iter_chunks(hash).await {
            Ok(stream) => {
                let num_chunks = (metadata.num_bytes + CHUNK_SIZE - 1) / CHUNK_SIZE;
                let height = metadata.s as u64;
                let (available_chunks, missing_indexes, mapper) =
                    self.analyze_chunks(hash, stream, num_chunks).await?;
                let rep_chunks = self
                    .repair_chunks(metadata, missing_indexes, mapper.clone())
                    .await?;

                let mut grid = Grid::new(
                    available_chunks.into_iter().map(|(_, b)| b).collect(),
                    height,
                )
                .map_err(|e| Error::Other(e.into()))?;

                let positioner = Positioner::new(height, num_chunks as u64);
                for (chunk_id, chunk) in rep_chunks {
                    let index = mapper.id_to_index(&chunk_id)?;
                    grid.set_cell(positioner.index_to_pos(index), chunk);
                }
                Ok(grid.assemble_data())
            }
            Err(e) => Err(Error::Storage(e)),
        }
    }

    /// Analyzes the chunks in the stream and returns the available chunks, missing indexes, and
    /// a map of chunk ids to positions.
    async fn analyze_chunks(
        &self,
        hash: &str,
        mut stream: ByteStream<T::ChunkId>,
        num_chunks: u64,
    ) -> Result<(Vec<(T::ChunkId, Bytes)>, Vec<T::ChunkId>, T::ChunkIdMapper), Error> {
        let mut missing_indexes = Vec::new();
        let mut available_chunks = vec![(T::ChunkId::default(), Bytes::new()); num_chunks as usize];
        let mapper = self.storage.chunk_id_mapper(hash);
        while let Some((chunk_id, chunk_result)) = stream.next().await {
            let index = mapper
                .id_to_index(&chunk_id)
                .map_err(|e| Error::Storage(e))? as usize;
            match chunk_result {
                Ok(chunk) => available_chunks[index] = (chunk_id.clone(), chunk),
                Err(_) => {
                    available_chunks[index] = (chunk_id.clone(), Bytes::new());
                    missing_indexes.push(chunk_id.clone());
                }
            }
        }

        Ok((available_chunks, missing_indexes, mapper))
    }

    async fn repair_chunks(
        &self,
        metadata: Metadata,
        missing_indexes: Vec<T::ChunkId>,
        mapper: T::ChunkIdMapper,
    ) -> std::result::Result<HashMap<T::ChunkId, Bytes>, Error> {
        let positioner = Positioner::new(
            metadata.s as u64,
            ((metadata.num_bytes + metadata.chunk_size - 1) / metadata.chunk_size) as u64,
        );
        Repairer::new(&self.storage, positioner, metadata, mapper)
            .repair_chunks(missing_indexes.clone())
            .await
            .map_err(Error::Repair)
    }
}

fn bytes_to_chunks(bytes: Bytes, chunk_size: u64) -> Vec<Bytes> {
    let chunk_size = chunk_size as usize;
    let mut chunks = Vec::with_capacity((bytes.len() + chunk_size - 1) / chunk_size);
    let mut start = 0;

    while start < bytes.len() {
        let end = std::cmp::min(start + chunk_size as usize, bytes.len());
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
    use async_trait::async_trait;
    use storage::ChunkIdMapper;

    #[derive(Clone)]
    struct MockStorage;

    #[derive(Clone)]
    pub struct MockChunkIdMapper {}

    impl ChunkIdMapper<u64> for MockChunkIdMapper {
        fn index_to_id(&self, index: u64) -> Result<u64, StorageError> {
            Ok(index as u64)
        }

        fn id_to_index(&self, chunk_id: &u64) -> Result<u64, StorageError> {
            Ok(*chunk_id as u64)
        }
    }

    #[async_trait]
    impl Storage for MockStorage {
        type ChunkId = u64;
        type ChunkIdMapper = MockChunkIdMapper;

        async fn upload_bytes(&self, _: impl Into<Bytes> + Send) -> Result<String> {
            Ok("mock_hash".to_string())
        }

        fn chunk_id_mapper(&self, _: &str) -> Self::ChunkIdMapper {
            MockChunkIdMapper {}
        }

        async fn download_bytes(&self, _: &str) -> Result<Bytes, StorageError> {
            Ok(Bytes::from("mock data"))
        }

        async fn iter_chunks(&self, _: &str) -> Result<ByteStream<Self::ChunkId>, StorageError> {
            let chunks = vec![Bytes::from("mock data")];

            let stream = futures::stream::iter(
                chunks
                    .into_iter()
                    .enumerate()
                    .map(move |(index, chunk)| (index as u64, Ok(chunk))),
            );

            Ok(Box::pin(stream))
        }

        async fn download_chunk(&self, _: &str, _: Self::ChunkId) -> Result<Bytes, StorageError> {
            Ok(Bytes::from("mock data"))
        }
    }

    #[test]
    fn test_entangler_new_valid_parameters() {
        let storage = MockStorage;
        let result = Entangler::new(storage, 3, 2, 4);
        assert!(result.is_ok());
        let entangler = result.unwrap();
        assert_eq!(entangler.alpha, 3);
        assert_eq!(entangler.s, 2);
    }

    #[test]
    fn test_entangler_new_alpha_zero() {
        let storage = MockStorage;
        let result = Entangler::new(storage, 0, 2, 4);
        assert!(result.is_err());
        assert!(matches!(
            result.err().unwrap(),
            Error::InvalidEntanglementParameter(param, value) if param == "alpha" && value == 0
        ));
    }

    #[test]
    fn test_entangler_new_s_zero() {
        let storage = MockStorage;
        let result = Entangler::new(storage, 3, 0, 4);
        assert!(result.is_err());
        assert!(matches!(
            result.err().unwrap(),
            Error::InvalidEntanglementParameter(param, value) if param == "s" && value == 0
        ));
    }

    #[test]
    fn test_entangler_new_p_less_than_s() {
        let storage = MockStorage;
        let result = Entangler::new(storage, 3, 4, 2);
        assert!(result.is_err());
        assert!(matches!(
            result.err().unwrap(),
            Error::InvalidEntanglementParameter(param, value) if param == "p" && value == 2
        ));
    }

    #[test]
    fn test_entangler_new_p_not_multiple_of_s() {
        let storage = MockStorage;
        let result = Entangler::new(storage, 3, 3, 7);
        assert!(result.is_err());
        assert!(matches!(
            result.err().unwrap(),
            Error::InvalidEntanglementParameter(param, value) if param == "p" && value == 7
        ));
    }

    #[test]
    fn test_entangler_new_p_zero() {
        let storage = MockStorage;
        let result = Entangler::new(storage, 3, 2, 0);
        assert!(result.is_ok());
        let entangler = result.unwrap();
        assert_eq!(entangler.alpha, 3);
        assert_eq!(entangler.s, 2);
    }

    #[test]
    fn test_entangler_new_p_valid_multiple_of_s() {
        let storage = MockStorage;
        let result = Entangler::new(storage, 3, 2, 6);
        assert!(result.is_ok());
        let entangler = result.unwrap();
        assert_eq!(entangler.alpha, 3);
        assert_eq!(entangler.s, 2);
    }
}
