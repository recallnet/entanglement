// Copyright 2024 Entanglement Contributors
// SPDX-License-Identifier: Apache-2.0, MIT

use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use core::net::SocketAddr;
use futures::stream;
use iroh::client::blobs::ReadAtLen;
use iroh::{blobs::Hash, client::Iroh as Client};
use std::sync::Arc;
use std::{path::Path, str::FromStr};

use crate::storage::{
    self, ByteStream, ChunkId, ChunkIdMapper, ChunkStream, Error as StorageError, Storage,
};

const CHUNK_SIZE: u64 = 1024;

/// `ClientProvider` is a trait for types that can provide an Iroh client.
trait ClientProvider: Send + Sync {
    fn client(&self) -> &Client;
}

/// `IrohStorage` is a storage backend that interacts with the Iroh client to store and retrieve data.
/// It supports various initialization methods, including in-memory and persistent storage, and can
/// upload and download data in chunks.
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

/// `ClientHolder` is a wrapper around an Iroh client that implements `ClientProvider`.
#[derive(Clone)]
struct ClientHolder {
    client: Client,
}

impl ClientProvider for ClientHolder {
    fn client(&self) -> &Client {
        &self.client
    }
}

/// `NodeHolder` is a wrapper around an Iroh node that implements `ClientProvider`.
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

impl ChunkId for u64 {}

#[derive(Clone)]
pub struct IrohChunkIdMapper {
    hash: String,
    num_chunks: u64,
}

impl ChunkIdMapper<u64> for IrohChunkIdMapper {
    fn index_to_id(&self, index: u64) -> Result<u64, StorageError> {
        if index >= self.num_chunks {
            return Err(StorageError::ChunkNotFound(
                index.to_string(),
                self.hash.clone(),
                storage::wrap_error(anyhow::anyhow!("Chunk index out of bounds")),
            ));
        }
        Ok(index)
    }

    fn id_to_index(&self, chunk_id: &u64) -> Result<u64, StorageError> {
        if *chunk_id >= self.num_chunks {
            return Err(StorageError::ChunkNotFound(
                chunk_id.to_string(),
                self.hash.clone(),
                storage::wrap_error(anyhow::anyhow!("Chunk id out of bounds")),
            ));
        }
        Ok(*chunk_id)
    }
}

use futures::StreamExt;

#[async_trait]
impl Storage for IrohStorage {
    type ChunkId = u64;
    type ChunkIdMapper = IrohChunkIdMapper;

    async fn upload_bytes(&self, bytes: impl Into<Bytes> + Send) -> Result<String, StorageError> {
        let blob = self.client().blobs().add_bytes(bytes).await.unwrap();
        Ok(blob.hash.to_string())
    }

    async fn download_bytes(&self, hash: &str) -> Result<ByteStream, StorageError> {
        let hash = parse_hash(hash)?;
        let reader = self
            .client()
            .blobs()
            .read(hash)
            .await
            .map_err(|e| StorageError::StorageError(storage::wrap_error(e)))?;
        let stream = reader
            .map(|res| res.map_err(|e| StorageError::StorageError(storage::wrap_error(e.into()))));
        Ok(Box::pin(stream))
    }

    async fn iter_chunks(&self, hash: &str) -> Result<ChunkStream<Self::ChunkId>, StorageError> {
        let hash = parse_hash(hash)?;
        let reader = self.client().blobs().read(hash).await.map_err(|e| {
            let err_str = e.to_string();
            if err_str.contains("not found") {
                StorageError::BlobNotFound(hash.to_string())
            } else {
                StorageError::StorageError(storage::wrap_error(e))
            }
        })?;
        let total_size = reader.size();

        let stream = stream::unfold(
            (self.client().blobs().clone(), 0u64),
            move |(client, offset)| async move {
                if offset >= total_size {
                    return None;
                }

                let remaining = total_size - offset;
                let len = std::cmp::min(CHUNK_SIZE, remaining);

                let chunk_id = offset / CHUNK_SIZE;
                Some(
                    match client
                        .read_at_to_bytes(hash, offset, ReadAtLen::Exact(len))
                        .await
                    {
                        Ok(chunk) => {
                            let new_offset = offset + len as u64;
                            ((chunk_id, Ok(chunk)), (client, new_offset))
                        }
                        Err(e) => (
                            (
                                chunk_id,
                                Err(StorageError::StorageError(storage::wrap_error(e))),
                            ),
                            (client, offset + len as u64),
                        ),
                    },
                )
            },
        );

        Ok(Box::pin(stream))
    }

    async fn download_chunk(&self, hash: &str, chunk_id: u64) -> Result<Bytes, StorageError> {
        let hash = parse_hash(hash)?;
        let offset = chunk_id * CHUNK_SIZE;

        self.client()
            .blobs()
            .read_at_to_bytes(hash, offset, ReadAtLen::AtMost(CHUNK_SIZE))
            .await
            .map_err(|e| {
                StorageError::ChunkNotFound(
                    chunk_id.to_string(),
                    hash.to_string(),
                    storage::wrap_error(e),
                )
            })
    }

    async fn chunk_id_mapper(&self, hash: &str) -> Result<IrohChunkIdMapper, StorageError> {
        let hash = parse_hash(hash).map_err(|_| StorageError::BlobNotFound(hash.to_string()))?;
        let reader = self
            .client()
            .blobs()
            .read(hash)
            .await
            .map_err(|_| StorageError::BlobNotFound(hash.to_string()))?;

        Ok(IrohChunkIdMapper {
            hash: hash.to_string(),
            num_chunks: (reader.size() + CHUNK_SIZE - 1) / CHUNK_SIZE,
        })
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
        let results: Vec<Result<Bytes, StorageError>> = stream
            .map(|res| res.1)
            .collect()
            .await;
        let bytes_vec = results.into_iter().collect::<Result<Vec<_>, _>>()?;
        Ok(bytes_vec)
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

    #[tokio::test]
    async fn test_chunk_id_mapper() -> Result<()> {
        let storage = IrohStorage::new_in_memory().await?;
        let data = vec![0u8; 3000]; // 3 chunks
        let hash = storage.upload_bytes(data).await?;

        let mapper = storage.chunk_id_mapper(&hash).await?;
        assert_eq!(mapper.index_to_id(0)?, 0);
        assert_eq!(mapper.index_to_id(1)?, 1);
        assert_eq!(mapper.index_to_id(2)?, 2);
        assert!(
            matches!(mapper.id_to_index(&3), Err(StorageError::ChunkNotFound(c_id, h, _)) if c_id == "3" && h == hash),
            "Expected error because chunk id is out of bounds"
        );

        let res = storage.chunk_id_mapper("invalid").await;
        assert!(res.is_err(), "Expected error because hash is invalid");
        assert!(
            matches!(res.err().unwrap(), StorageError::BlobNotFound(h) if h == "invalid"),
            "Expected error because hash is invalid"
        );

        // make valid not existing hash from existing hash by replacing 1 character
        let last_char = (hash.chars().last().unwrap() as u8 + 1) as char;
        let non_existing_hash = hash
            .chars()
            .take(hash.len() - 1)
            .chain(std::iter::once(last_char))
            .collect::<String>();
        let res = storage.chunk_id_mapper(&non_existing_hash).await;
        assert!(res.is_err(), "Expected error because hash does not exist");
        assert!(
            matches!(res.err().unwrap(), StorageError::BlobNotFound(h) if h == non_existing_hash),
            "Expected error because hash does not exist"
        );
        Ok(())
    }
}
