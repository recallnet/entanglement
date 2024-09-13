// Copyright 2024 Entanglement Contributors
// SPDX-License-Identifier: Apache-2.0, MIT

use anyhow::Result;
use bytes::Bytes;
use futures::StreamExt;
use std::collections::HashMap;
use storage::{ByteStream, Error as StorageError, Storage};

use crate::executer;
use crate::grid::Grid;
use crate::metadata::Metadata;
use crate::repairer::Repairer;

const CHUNK_SIZE: usize = 1024;

fn bytes_to_chunks(bytes: Bytes, chunk_size: usize) -> Vec<Bytes> {
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

pub struct Entangler<T: Storage> {
    storage: T,
    alpha: u8,
    s: u8,
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Invalid parameter {0}: {1}")]
    InvalidEntanglementParameter(String, u8),

    #[error("Input vector is empty")]
    EmptyInput,

    #[error("Failed to download a blob with hash {hash}: {source}")]
    FailedToDownload {
        hash: String,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[error("Storage error: {0}")]
    StorageError(#[from] StorageError),

    #[error("Failed to parse metadata: {0}")]
    ParsingMetadata(#[from] serde_json::Error),

    #[error("Error occurred: {0}")]
    Other(#[from] anyhow::Error),
}

impl<T: Storage> Entangler<T> {
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

    pub async fn upload(&self, bytes: impl Into<Bytes> + Send) -> Result<(String, String)> {
        let bytes: Bytes = bytes.into();
        let chunks = bytes_to_chunks(bytes.clone(), CHUNK_SIZE);
        let num_chunks = chunks.len();

        let orig_grid = Grid::new(chunks, usize::min(self.s as usize, num_chunks))?;

        let exec = executer::Executer::new(self.alpha);
        let lattice = exec.execute(orig_grid)?;

        let num_bytes = bytes.len();
        let orig_hash = self.storage.upload_bytes(bytes).await?;

        let mut parity_hashes = HashMap::new();
        for parity_grid in lattice.get_parities() {
            let data = parity_grid.grid.assemble_data();
            let parity_hash = self.storage.upload_bytes(data).await?;
            parity_hashes.insert(parity_grid.strand_type, parity_hash);
        }

        let metadata = Metadata {
            orig_hash: orig_hash.clone(),
            parity_hashes,
            num_bytes: num_bytes as u64,
            chunk_size: CHUNK_SIZE as u64,
            s: self.s,
            p: self.s,
        };

        let metadata = serde_json::to_string(&metadata).unwrap();
        let metadata_hash = self.storage.upload_bytes(metadata).await?;

        Ok((orig_hash, metadata_hash))
    }

    pub async fn download(&self, hash: &str, metadata_hash: Option<&str>) -> Result<Bytes, Error> {
        match (self.storage.download_bytes(hash).await, metadata_hash) {
            (Ok(data), _) => Ok(data),
            (Err(_), Some(metadata_hash)) => self.download_repaired(hash, metadata_hash).await,
            (Err(e), _) => Err(Error::FailedToDownload {
                hash: hash.to_string(),
                source: e.into(),
            }),
        }
    }

    async fn download_metadata(&self, metadata_hash: &str) -> Result<Metadata, Error> {
        let metadata_bytes = self.storage.download_bytes(metadata_hash).await?;
        Ok(serde_json::from_slice(&metadata_bytes)?)
    }

    async fn download_repaired(&self, hash: &str, metadata_hash: &str) -> Result<Bytes, Error> {
        let metadata = self.download_metadata(metadata_hash).await?;

        match self.storage.iter_chunks(hash).await {
            Ok(stream) => {
                let num_chunks = (metadata.num_bytes as usize + CHUNK_SIZE - 1) / CHUNK_SIZE;
                let (available_chunks, missing_indexes) =
                    self.find_missing_chunks(stream, num_chunks).await?;
                self.repair_chunks(metadata, available_chunks, missing_indexes)
                    .await
            }
            Err(e) => return Err(Error::StorageError(e.into())),
        }
    }

    async fn find_missing_chunks(
        &self,
        mut stream: ByteStream<T::ChunkId>,
        num_chunks: usize,
    ) -> Result<(Vec<Bytes>, Vec<usize>), Error> {
        let mut missing_indexes = Vec::new();
        let mut available_chunks = vec![Bytes::new(); num_chunks];
        let mut index = 0;
        while let Some((_, chunk_result)) = stream.next().await {
            match chunk_result {
                Ok(chunk) => available_chunks[index] = chunk,
                Err(_) => missing_indexes.push(index),
            }
            index += 1;
        }

        Ok((available_chunks, missing_indexes))
    }

    async fn repair_chunks(
        &self,
        metadata: Metadata,
        chunks: Vec<Bytes>,
        missing_indexes: Vec<usize>,
    ) -> std::result::Result<Bytes, Error> {
        let mut grid =
            Grid::new(chunks, metadata.s as usize).map_err(|e| Error::Other(e.into()))?;

        Repairer::new(&self.storage, &mut grid, metadata)
            .repair_chunks(missing_indexes.clone())
            .await?;

        Ok(grid.assemble_data())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;

    #[derive(Clone)]
    struct MockStorage;

    #[async_trait]
    impl Storage for MockStorage {
        type ChunkId = usize;

        async fn upload_bytes(&self, _: impl Into<Bytes> + Send) -> Result<String> {
            Ok("mock_hash".to_string())
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
                    .map(move |(index, chunk)| (index, Ok(chunk))),
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
