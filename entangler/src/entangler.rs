// Copyright 2024 Entanglement Contributors
// SPDX-License-Identifier: Apache-2.0, MIT

use anyhow::Result;
use bytes::Bytes;
use storage::Storage;

use crate::executer;
use crate::grid::Grid;

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
    p: u8,
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Invalid parameter {0}: {1}")]
    InvalidEntanglementParameter(String, u8),
    #[error("Input vector is empty")]
    EmptyInput,
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
        Ok(Self {
            storage,
            alpha,
            s,
            p,
        })
    }

    pub async fn upload_bytes(&self, bytes: impl Into<Bytes> + Send) -> Result<String> {
        let bytes: Bytes = bytes.into();
        let chunks = bytes_to_chunks(bytes.clone(), CHUNK_SIZE);

        let orig_grid = Grid::new(chunks, self.s as usize, self.p as usize)?;

        let exec = executer::Executer::new(self.alpha, self.s, self.p);
        let lattice = exec.execute(orig_grid)?;

        let orig_hash = self.storage.upload_bytes(bytes).await?;
        self.storage.upload_bytes(bytes).await
    }

    pub async fn download_bytes(&self, hash: &str) -> Result<Bytes> {
        self.storage.download_bytes(hash).await
    }
}
