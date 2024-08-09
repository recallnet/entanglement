// Copyright 2024 Entanglement Contributors
// SPDX-License-Identifier: Apache-2.0, MIT

use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use core::net::SocketAddr;
use iroh::client::Iroh as Client;
use std::{path::Path, str::FromStr};

use crate::storage::Storage;

pub struct IrohStorage {
    client: Client,
}

impl IrohStorage {
    pub async fn from_path(root: impl AsRef<Path>) -> Result<Self> {
        let c = Client::connect_path(root).await?;
        Ok(Self { client: c })
    }

    pub async fn from_addr(addr: SocketAddr) -> Result<Self> {
        let c = Client::connect_addr(addr).await?;
        Ok(Self { client: c })
    }

    pub fn from_client(client: Client) -> Self {
        Self { client }
    }

    pub async fn new_in_memory() -> Result<Self> {
        let node = iroh::node::Node::memory().spawn().await?;
        Ok(Self::from_client(node.client().clone()))
    }

    pub async fn new_permanent(root: impl AsRef<Path>) -> Result<Self> {
        let node = iroh::node::Node::persistent(root).await?.spawn().await?;
        Ok(Self::from_client(node.client().clone()))
    }
}

#[async_trait]
impl Storage for IrohStorage {
    async fn upload_bytes(&self, bytes: impl Into<Bytes> + Send) -> Result<String> {
        let blob = self.client.blobs().add_bytes(bytes).await.unwrap();
        Ok(blob.hash.to_string())
    }

    async fn download_bytes(&self, hash: &str) -> Result<Bytes> {
        let hash = iroh::blobs::Hash::from_str(hash)?;
        let res = self.client.blobs().read_to_bytes(hash).await?;
        Ok(res)
    }
}
