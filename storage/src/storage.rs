// Copyright 2024 Entanglement Contributors
// SPDX-License-Identifier: Apache-2.0, MIT

use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;

#[async_trait]
pub trait Storage: Send {
    async fn upload_bytes(&self, bytes: impl Into<Bytes> + Send) -> Result<String>;
    async fn download_bytes(&self, hash: &str) -> Result<Bytes>;
}
