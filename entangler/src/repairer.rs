// Copyright 2024 Entanglement Contributors
// SPDX-License-Identifier: Apache-2.0, MIT

use crate::grid::Grid;
use crate::lattice::{ParityGrid, StrandType};
use anyhow::Result;
use storage::{Error as StorageError, Storage};

use crate::metadata::Metadata;
use bytes::Bytes;

pub struct Repairer<T: Storage> {
    metadata: Metadata,
    storage: T,
}

fn xor_chunks(chunk1: &Bytes, chunk2: &Bytes) -> Bytes {
    let mut chunk = Vec::with_capacity(chunk1.len());
    for i in 0..chunk1.len() {
        chunk.push(chunk1[i] ^ chunk2[i]);
    }
    Bytes::from(chunk)
}

impl<T: Storage> Repairer<T> {
    pub fn new(storage: T, metadata: Metadata) -> Self {
        return Self { metadata, storage };
    }

    pub async fn repair_chunks(&self, indexes: Vec<usize>) -> Result<Vec<Bytes>, StorageError> {
        self.metadata.num_bytes;

        let (_, h_parity_hash) = self
            .metadata
            .parity_hashes
            .iter()
            .find(|(strand, _)| **strand == StrandType::Horizontal)
            .unwrap();

        let mut result = Vec::with_capacity(indexes.len());

        match self.storage.download_bytes(&h_parity_hash).await {
            Ok(h_parity_bytes) => {
                let h_parity_grid = ParityGrid {
                    grid: Grid::from_bytes(
                        h_parity_bytes,
                        self.metadata.s as usize,
                        self.metadata.chunk_size as usize,
                    )
                    .map_err(|e| StorageError::Other(anyhow::anyhow!(e.to_string())))?,
                    strand_type: StrandType::Horizontal,
                };

                for index in indexes {
                    let (left, right) = h_parity_grid.get_pair_for(index as u64).unwrap();
                    let repaired_bytes = xor_chunks(&left, &right);
                    result.push(repaired_bytes);
                }
            }
            Err(e) => {
                return Err(StorageError::Other(anyhow::anyhow!(
                    "Failed to download horizontal parity: {}",
                    e
                )));
            }
        }

        Ok(result)
    }
}
