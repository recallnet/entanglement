// Copyright 2024 Entanglement Contributors
// SPDX-License-Identifier: Apache-2.0, MIT

use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use futures::Stream;
use std::pin::Pin;
use thiserror;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Blob with hash {0} not found")]
    BlobNotFound(String),

    #[error("Storage error: {0}")]
    StorageError(#[from] anyhow::Error),

    #[error("Invalid hash {0}. Error: {1}")]
    InvalidHash(String, String),

    #[error("Error occurred: {0}")]
    Other(#[source] anyhow::Error),
}

pub type ByteStream = Pin<Box<dyn Stream<Item = Result<Bytes>> + Send>>;

#[async_trait]
pub trait Storage: Send + Clone {
    async fn upload_bytes(&self, bytes: impl Into<Bytes> + Send) -> Result<String>;
    async fn download_bytes(&self, hash: &str) -> Result<Bytes, Error>;
    async fn iter_chunks(&self, hash: &str) -> Result<ByteStream, Error>;
}
