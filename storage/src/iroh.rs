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
    type ChunkId = usize;

    async fn upload_bytes(&self, bytes: impl Into<Bytes> + Send) -> Result<String> {
        let blob = self.client().blobs().add_bytes(bytes).await.unwrap();
        Ok(blob.hash.to_string())
    }

    async fn download_bytes(&self, hash: &str) -> Result<Bytes, StorageError> {
        let hash = parse_hash(hash)?;
        let res = self.client().blobs().read_to_bytes(hash).await?;
        Ok(res)
    }

    async fn iter_chunks(&self, hash: &str) -> Result<ByteStream<Self::ChunkId>, StorageError> {
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
            (self.client().blobs().clone(), 0u64),
            move |(client, offset)| async move {
                if offset >= total_size {
                    return None;
                }

                let remaining = total_size - offset;
                let len = std::cmp::min(CHUNK_SIZE, remaining as usize);

                let chunk_id = offset as usize / CHUNK_SIZE;
                Some(
                    match client.read_at_to_bytes(hash, offset, Some(len)).await {
                        Ok(chunk) => {
                            let new_offset = offset + len as u64;
                            ((chunk_id, Ok(chunk)), (client, new_offset))
                        }
                        Err(e) => ((chunk_id, Err(e.into())), (client, offset + len as u64)),
                    },
                )
            },
        );

        Ok(Box::pin(stream))
    }

    async fn download_chunk(
        &self,
        hash: &str,
        chunk_id: Self::ChunkId,
    ) -> Result<Bytes, StorageError> {
        let hash = parse_hash(hash)?;
        let offset = chunk_id as u64 * CHUNK_SIZE as u64;
        let mut size = CHUNK_SIZE;

        match self
            .client()
            .blobs()
            .read_at_to_bytes(hash, offset, Some(size))
            .await
        {
            Ok(bytes) => Ok(bytes),
            Err(e) => {
                let error_msg = e.to_string();
                if error_msg.contains("requested range is out of bounds") {
                    if let Some(correct_size) = error_msg
                        .split("(")
                        .last()
                        .and_then(|s| s.strip_suffix(")"))
                        .and_then(|s| s.parse::<usize>().ok())
                    {
                        size = correct_size.saturating_sub(offset as usize);
                        if size == 0 {
                            return Err(StorageError::ChunkNotFound(
                                chunk_id.to_string(),
                                hash.to_string(),
                                e,
                            ));
                        }
                        // Retry once with the new size
                        return self
                            .client()
                            .blobs()
                            .read_at_to_bytes(hash, offset, Some(size))
                            .await
                            .map_err(|e| {
                                StorageError::ChunkNotFound(
                                    chunk_id.to_string(),
                                    hash.to_string(),
                                    e,
                                )
                            });
                    }
                }
                // If we couldn't parse the size or it's not an out of bounds error, return the original error
                Err(StorageError::ChunkNotFound(
                    chunk_id.to_string(),
                    hash.to_string(),
                    e,
                ))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use futures::StreamExt;
    use tokio;

    async fn collect_chunks(storage: &IrohStorage, hash: &str) -> Result<Vec<Bytes>> {
        let stream = storage.iter_chunks(hash).await?;
        let results: Vec<Result<Bytes>> = stream.map(|res| res.1).collect().await;
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

    #[tokio::test]
    async fn test_download_chunk_small_blob() -> Result<()> {
        let storage = IrohStorage::new_in_memory().await?;
        let data = Bytes::from("Hello, World!");
        let hash = storage.upload_bytes(data.clone()).await?;

        let chunk = storage.download_chunk(&hash, 0).await?;
        assert_eq!(chunk, data);
        Ok(())
    }

    #[tokio::test]
    async fn test_download_chunk_large_blob() -> Result<()> {
        let storage = IrohStorage::new_in_memory().await?;
        let data = Bytes::from(vec![0u8; 3000]); // 3000 bytes, should be 3 chunks
        let hash = storage.upload_bytes(data.clone()).await?;

        let chunk0 = storage.download_chunk(&hash, 0).await?;
        let chunk1 = storage.download_chunk(&hash, 1).await?;
        let chunk2 = storage.download_chunk(&hash, 2).await?;

        assert_eq!(chunk0.len(), 1024);
        assert_eq!(chunk1.len(), 1024);
        assert_eq!(chunk2.len(), 952);
        assert_eq!(Bytes::from([chunk0, chunk1, chunk2].concat()), data);
        Ok(())
    }

    #[tokio::test]
    async fn test_download_chunk_exact_multiple() -> Result<()> {
        let storage = IrohStorage::new_in_memory().await?;
        let data = Bytes::from(vec![0u8; 2048]);
        let hash = storage.upload_bytes(data.clone()).await?;

        let chunk0 = storage.download_chunk(&hash, 0).await?;
        let chunk1 = storage.download_chunk(&hash, 1).await?;

        assert_eq!(chunk0.len(), 1024);
        assert_eq!(chunk1.len(), 1024);
        assert_eq!(Bytes::from([chunk0, chunk1].concat()), data);
        Ok(())
    }

    #[tokio::test]
    async fn test_download_chunk_invalid_hash() -> Result<()> {
        let storage = IrohStorage::new_in_memory().await?;
        let result = storage.download_chunk("invalid_hash", 0).await;
        assert!(result.is_err());
        Ok(())
    }

    #[tokio::test]
    async fn test_download_chunk_out_of_bounds() -> Result<()> {
        let storage = IrohStorage::new_in_memory().await?;
        let data = Bytes::from("Hello, World!");
        let hash = storage.upload_bytes(data).await?;

        let result = storage.download_chunk(&hash, 1).await;
        assert!(result.is_err());
        assert!(matches!(
            result.err().unwrap(),
            StorageError::ChunkNotFound(c, h, _) if h == hash && c == "1"
        ));
        Ok(())
    }
}
