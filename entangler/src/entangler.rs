// Copyright 2024 Entanglement Contributors
// SPDX-License-Identifier: Apache-2.0, MIT

use anyhow::Result;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use storage::Storage;

use crate::executer;
use crate::grid::Grid;
use crate::lattice::StrandType;

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
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Metadata {
    pub orig_hash: String,
    pub parity_hashes: HashMap<StrandType, String>,
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

    pub async fn upload_bytes(&self, bytes: impl Into<Bytes> + Send) -> Result<(String, String)> {
        let bytes: Bytes = bytes.into();
        let chunks = bytes_to_chunks(bytes.clone(), CHUNK_SIZE);
        let num_chunks = chunks.len();

        let orig_grid = Grid::new(chunks, usize::min(self.s as usize, num_chunks))?;

        let exec = executer::Executer::new(self.alpha);
        let lattice = exec.execute(orig_grid)?;

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
        };

        let metadata = serde_json::to_string(&metadata).unwrap();
        let metadata_hash = self.storage.upload_bytes(metadata).await?;

        Ok((orig_hash, metadata_hash))
    }

    pub async fn download_bytes(&self, hash: &str) -> Result<Bytes> {
        self.storage.download_bytes(hash).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;

    struct MockStorage;

    #[async_trait]
    impl Storage for MockStorage {
        async fn upload_bytes(&self, _: impl Into<Bytes> + Send) -> Result<String> {
            Ok("mock_hash".to_string())
        }
        async fn download_bytes(&self, _: &str) -> Result<Bytes> {
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
