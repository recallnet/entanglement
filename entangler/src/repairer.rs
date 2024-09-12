// Copyright 2024 Entanglement Contributors
// SPDX-License-Identifier: Apache-2.0, MIT

use crate::grid::{Dir, Grid};
use crate::lattice::ParityGrid;
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

    pub async fn repair_chunks(&mut self, mut indexes: Vec<usize>) -> Result<(), StorageError> {
        self.metadata.num_bytes;

        for (strand, parity_hash) in &self.metadata.parity_hashes {
            let parity_result = self.storage.download_bytes(parity_hash).await;
            if parity_result.is_err() {
                match parity_result.err().unwrap() {
                    StorageError::BlobNotFound(_) => continue,
                    other => return Err(other),
                }
            }

            let parity_grid = ParityGrid {
                grid: Grid::from_bytes(
                    parity_result.unwrap(),
                    self.metadata.s as usize,
                    self.metadata.chunk_size as usize,
                )
                .map_err(|e| StorageError::Other(anyhow::anyhow!(e.to_string())))?,
                strand_type: *strand,
            };

            let mut i = 0;
            while i < indexes.len() {
                let index = indexes[i];
                let pos = self.grid.index_to_pos(index);
                let dir: Dir = (*strand).into();
                let neig_pos = pos.adjacent(dir.opposite());
                if let Some(neig_cell) = self.grid.try_get_cell(neig_pos) {
                    let parity_cell = parity_grid.grid.get_cell(neig_pos);
                    self.grid
                        .set_cell(pos, xor_chunks(&neig_cell, &parity_cell));

                    indexes.swap_remove(i);
                } else {
                    i += 1;
                }
            }

            if indexes.is_empty() {
                break;
            }
        }

        if !indexes.is_empty() {
            return Err(StorageError::Other(anyhow::anyhow!(
                "Failed to repair all chunks"
            )));
        }

        Ok(())
    }
}
