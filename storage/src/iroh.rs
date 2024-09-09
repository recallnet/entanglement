// Copyright 2024 Entanglement Contributors
// SPDX-License-Identifier: Apache-2.0, MIT

use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use core::net::SocketAddr;
use futures::stream;
use iroh::{blobs::Hash, client::Iroh as Client};
use std::sync::Arc;
use std::{path::Path, str::FromStr};

use crate::storage::{ByteStream, Error as StorageError, Storage};

const CHUNK_SIZE: usize = 1024;

trait ClientProvider: Send + Sync {
    fn client(&self) -> &Client;
}

pub struct IrohStorage {
    client_provider: Arc<dyn ClientProvider>,
}

impl Clone for IrohStorage {
    fn clone(&self) -> Self {
        IrohStorage {
            client_provider: Arc::clone(&self.client_provider),
        }
    }
}

#[derive(Clone)]
struct ClientHolder {
    client: Client,
}

impl ClientProvider for ClientHolder {
    fn client(&self) -> &Client {
        &self.client
    }
}

struct NodeHolder<S> {
    node: iroh::node::Node<S>,
}

impl<S: iroh::blobs::store::Store> ClientProvider for NodeHolder<S> {
    fn client(&self) -> &Client {
        self.node.client()
    }
}

impl<S: Clone> Clone for NodeHolder<S> {
    fn clone(&self) -> Self {
        NodeHolder {
            node: self.node.clone(),
        }
    }
}

impl IrohStorage {
    pub async fn from_path(root: impl AsRef<Path>) -> Result<Self> {
        let client = Client::connect_path(root).await?;
        Ok(Self {
            client_provider: Arc::new(ClientHolder { client }),
        })
    }

    pub async fn from_addr(addr: SocketAddr) -> Result<Self> {
        let client = Client::connect_addr(addr).await?;
        Ok(Self {
            client_provider: Arc::new(ClientHolder { client }),
        })
    }

    pub fn from_client(client: Client) -> Self {
        Self {
            client_provider: Arc::new(ClientHolder { client }),
        }
    }

    pub fn from_node<S: iroh::blobs::store::Store + 'static>(node: iroh::node::Node<S>) -> Self {
        Self {
            client_provider: Arc::new(NodeHolder { node }),
        }
    }

    pub async fn new_in_memory() -> Result<Self> {
        let node = iroh::node::Node::memory().spawn().await?;
        Ok(Self::from_node(node))
    }

    pub async fn new_permanent(root: impl AsRef<Path>) -> Result<Self> {
        let node = iroh::node::Node::persistent(root).await?.spawn().await?;
        Ok(Self::from_client(node.client().clone()))
    }

    fn client(&self) -> &Client {
        self.client_provider.client()
    }
}

fn parse_hash(hash: &str) -> Result<Hash, StorageError> {
    Hash::from_str(hash).map_err(|e| StorageError::InvalidHash(hash.to_string(), e.to_string()))
}

#[async_trait]
impl Storage for IrohStorage {
    async fn upload_bytes(&self, bytes: impl Into<Bytes> + Send) -> Result<String> {
        let blob = self.client().blobs().add_bytes(bytes).await.unwrap();
        Ok(blob.hash.to_string())
    }

    async fn download_bytes(&self, hash: &str) -> Result<Bytes, StorageError> {
        let hash = parse_hash(hash)?;
        let res = self.client().blobs().read_to_bytes(hash).await?;
        Ok(res)
    }

    async fn iter_chunks(&self, hash: &str) -> Result<ByteStream, StorageError> {
        let hash = parse_hash(hash)?;
        let reader = self.client().blobs().read(hash).await;
        if let Err(e) = reader {
            let err_str = e.to_string();
            if err_str.contains("not found") {
                return Err(StorageError::BlobNotFound(hash.to_string()));
            } else {
                return Err(StorageError::StorageError(e.into()));
            }
        }
        let total_size = reader?.size();

        let stream = stream::unfold(
            (self.client().blobs().clone(), hash, 0u64, total_size),
            move |(client, hash, offset, total_size)| async move {
                if offset >= total_size {
                    return None;
                }

                let remaining = total_size - offset;
                let len = std::cmp::min(CHUNK_SIZE, remaining as usize);

                match client.read_at_to_bytes(hash, offset, Some(len)).await {
                    Ok(chunk) => {
                        let new_offset = offset + len as u64;
                        Some((Ok(chunk), (client, hash, new_offset, total_size)))
                    }
                    Err(e) => Some((
                        Err(e.into()),
                        (client, hash, offset + len as u64, total_size),
                    )),
                }
            },
        );

        Ok(Box::pin(stream))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::Storage;
    use anyhow::Result;
    use bytes::Bytes;
    use futures::StreamExt;
    use tokio;

    async fn collect_chunks(storage: &IrohStorage, hash: &str) -> Result<Vec<Bytes>> {
        let stream = storage.iter_chunks(hash).await?;
        let results: Vec<Result<Bytes>> = stream.collect().await;
        results.into_iter().collect()
    }

    #[tokio::test]
    async fn test_iter_chunks_small_blob() -> Result<()> {
        let storage = IrohStorage::new_in_memory().await?;
        let data = Bytes::from("Hello, World!");
        let hash = storage.upload_bytes(data.clone()).await?;

        let chunks = collect_chunks(&storage, &hash).await?;

        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0], data);
        Ok(())
    }

    #[tokio::test]
    async fn test_iter_chunks_large_blob() -> Result<()> {
        let storage = IrohStorage::new_in_memory().await?;
        let data = Bytes::from(vec![0u8; 3000]); // 3000 bytes, should be 3 chunks
        let hash = storage.upload_bytes(data.clone()).await?;

        let chunks = collect_chunks(&storage, &hash).await?;

        assert_eq!(chunks.len(), 3);
        assert_eq!(chunks[0].len(), 1024);
        assert_eq!(chunks[1].len(), 1024);
        assert_eq!(chunks[2].len(), 952);
        assert_eq!(Bytes::from(chunks.concat()), data);
        Ok(())
    }

    #[tokio::test]
    async fn test_iter_chunks_empty_blob() -> Result<()> {
        let storage = IrohStorage::new_in_memory().await?;
        let data = Bytes::new();
        let hash = storage.upload_bytes(data).await?;

        let chunks = collect_chunks(&storage, &hash).await?;

        assert_eq!(chunks.len(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn test_iter_chunks_exact_multiple() -> Result<()> {
        let storage = IrohStorage::new_in_memory().await?;
        let data = Bytes::from(vec![0u8; 2048]);
        let hash = storage.upload_bytes(data.clone()).await?;

        let chunks = collect_chunks(&storage, &hash).await?;

        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].len(), 1024);
        assert_eq!(chunks[1].len(), 1024);
        assert_eq!(Bytes::from(chunks.concat()), data);
        Ok(())
    }

    #[tokio::test]
    async fn test_iter_chunks_invalid_hash() -> Result<()> {
        let storage = IrohStorage::new_in_memory().await?;
        let result = storage.iter_chunks("invalid_hash").await;
        assert!(result.is_err());
        Ok(())
    }
}
