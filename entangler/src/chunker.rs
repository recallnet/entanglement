// Copyright 2024 Entanglement Contributors
// SPDX-License-Identifier: Apache-2.0, MIT

use bytes::Bytes;
use thiserror;

use anyhow::Result;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Invalid chunk index: {0}")]
    InvalidChunkIndex(usize),
}

pub struct Chunker {
    data: Bytes,
    chunk_size: usize,
}

impl Chunker {
    pub fn new(data: Bytes, chunk_size: usize) -> Self {
        Self { data, chunk_size }
    }

    pub fn get_chunk(&self, index: usize) -> Result<Bytes, Error> {
        let start = index * self.chunk_size;
        let end = (index + 1) * self.chunk_size;
        if end > self.data.len() {
            return Err(Error::InvalidChunkIndex(index));
        }
        Ok(self.data.slice(start..end))
    }
}
