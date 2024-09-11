// Copyright 2024 Entanglement Contributors
// SPDX-License-Identifier: Apache-2.0, MIT

use crate::grid::{Dir, Grid};
use crate::lattice::{ParityGrid, StrandType};
use anyhow::Result;
use storage::{Error as StorageError, Storage};

use crate::metadata::Metadata;
use bytes::Bytes;

pub struct Repairer<'a, 'b, T: Storage> {
    metadata: Metadata,
    storage: &'a T,
    grid: &'b mut Grid,
}

fn xor_chunks(chunk1: &Bytes, chunk2: &Bytes) -> Bytes {
    let mut chunk = Vec::with_capacity(chunk1.len());
    for i in 0..chunk1.len() {
        chunk.push(chunk1[i] ^ chunk2[i]);
    }
    Bytes::from(chunk)
}

impl<'a, 'b, T: Storage> Repairer<'a, 'b, T> {
    pub fn new(storage: &'a T, grid: &'b mut Grid, metadata: Metadata) -> Self {
        return Self {
            metadata,
            storage,
            grid,
        };
    }

    pub async fn repair_chunks(&mut self, indexes: Vec<usize>) -> Result<(), StorageError> {
        self.metadata.num_bytes;

        let (_, h_parity_hash) = self
            .metadata
            .parity_hashes
            .iter()
            .find(|(strand, _)| **strand == StrandType::Horizontal)
            .unwrap();

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
                    let pos = self.grid.index_to_pos(index);
                    let neig_pos = pos.adjacent(Dir::L);
                    if let Some(neig_cell) = self.grid.try_get_cell(neig_pos) {
                        let parity_cell = h_parity_grid.grid.get_cell(neig_pos);
                        self.grid
                            .set_cell(pos, xor_chunks(&neig_cell, &parity_cell));
                    }
                }
            }
            Err(e) => {
                return Err(StorageError::Other(anyhow::anyhow!(
                    "Failed to download horizontal parity: {}",
                    e
                )));
            }
        }

        Ok(())
    }
}
