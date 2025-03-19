// Copyright 2024 Entanglement Contributors
// SPDX-License-Identifier: Apache-2.0, MIT

use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream;
use futures_lite::{Stream, StreamExt};
use iroh::protocol::Router;
use iroh::Endpoint;
use iroh_blobs::net_protocol::Blobs;
use iroh_blobs::rpc::client::blobs::{MemClient, ReadAtLen};
use iroh_blobs::util::SetTagOption;
use iroh_blobs::{Hash, Tag};
use std::{path::Path, str::FromStr};
use uuid::Uuid;

use crate::storage::{
    self, ByteStream, ChunkId, ChunkIdMapper, ChunkStream, Error as StorageError, Storage,
};

const CHUNK_SIZE: u64 = 1024;

/// `IrohStorage` is a storage backend that interacts with the Iroh client to store and retrieve data.
/// It supports various initialization methods, including in-memory and persistent storage, and can
/// upload and download data in chunks.
///
/// Upon upload a blob it will include in the `UploadResult::info` under "tag" key the tag of the
/// blob that iroh assigned to the blob with `SetTagOption::Auto`.
#[derive(Debug, Clone)]
pub enum IrohStorage {
    Full { router: Router, blobs: BlobsWrapper },
    Client { blobs_client: MemClient },
}

#[derive(Debug, Clone)]
pub enum BlobsWrapper {
    Mem(Blobs<iroh_blobs::store::mem::Store>),
    Fs(Blobs<iroh_blobs::store::fs::Store>),
}

impl BlobsWrapper {
    pub fn client(&self) -> &MemClient {
        match self {
            BlobsWrapper::Mem(b) => b.client(),
            BlobsWrapper::Fs(b) => b.client(),
        }
    }
}

impl IrohStorage {
    pub fn from_client(blobs_client: MemClient) -> Self {
        Self::Client { blobs_client }
    }

    pub async fn new_in_memory() -> Result<Self> {
        let endpoint = Endpoint::builder().discovery_n0().bind().await?;
        let blobs = Blobs::memory().build(&endpoint);
        let router = Router::builder(endpoint)
            .accept(iroh_blobs::ALPN, blobs.clone())
            .spawn()
            .await?;

        Ok(Self::Full {
            router,
            blobs: BlobsWrapper::Mem(blobs),
        })
    }

    pub async fn new_permanent(root: impl AsRef<Path>) -> Result<Self> {
        let endpoint = Endpoint::builder().discovery_n0().bind().await?;
        let blobs = Blobs::persistent(root).await?.build(&endpoint);
        let router = Router::builder(endpoint)
            .accept(iroh_blobs::ALPN, blobs.clone())
            .spawn()
            .await?;

        Ok(Self::Full {
            router,
            blobs: BlobsWrapper::Fs(blobs),
        })
    }

    pub fn blobs_client(&self) -> &MemClient {
        match self {
            Self::Full { blobs, .. } => blobs.client(),
            Self::Client { blobs_client } => blobs_client,
        }
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

#[async_trait]
impl Storage for IrohStorage {
    type ChunkId = u64;
    type ChunkIdMapper = IrohChunkIdMapper;

    async fn upload_bytes<S, E>(&self, stream: S) -> Result<storage::UploadResult, StorageError>
    where
        S: Stream<Item = Result<Bytes, E>> + Send + Unpin + 'static,
        E: std::error::Error + Send + Sync + 'static,
    {
        use futures::TryStreamExt;

        let iroh_stream = stream
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
            .map_ok(|bytes| bytes);

        let tag = format!("ent-{}", Uuid::new_v4());

        let progress = self
            .blobs_client()
            .add_stream(iroh_stream, SetTagOption::Named(Tag::from(tag.clone())))
            .await
            .map_err(|e| StorageError::StorageError(storage::wrap_error(e)))?;

        let blob = progress
            .finish()
            .await
            .map_err(|e| StorageError::StorageError(storage::wrap_error(e)))?;

        let mut info = std::collections::HashMap::new();
        info.insert("tag".to_string(), tag);

        Ok(storage::UploadResult {
            hash: blob.hash.to_string(),
            info,
            size: blob.size,
        })
    }

    async fn download_bytes(&self, hash: &str) -> Result<ByteStream, StorageError> {
        let hash = parse_hash(hash)?;

        let reader = self
            .blobs_client()
            .read(hash)
            .await
            .map_err(|e| StorageError::StorageError(storage::wrap_error(e)))?;

        let stream = reader
            .map(|res| res.map_err(|e| StorageError::StorageError(storage::wrap_error(e.into()))));

        Ok(Box::pin(stream))
    }

    async fn iter_chunks(&self, hash: &str) -> Result<ChunkStream<Self::ChunkId>, StorageError> {
        let hash = parse_hash(hash)?;
        let reader = self.blobs_client().read(hash).await.map_err(|e| {
            let err_str = e.to_string();
            if err_str.contains("not found") {
                StorageError::BlobNotFound(hash.to_string())
            } else {
                StorageError::StorageError(storage::wrap_error(e))
            }
        })?;
        let total_size = reader.size();

        let stream = stream::unfold(
            (self.blobs_client().clone(), 0u64),
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

        self.blobs_client()
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
            .blobs_client()
            .read(hash)
            .await
            .map_err(|_| StorageError::BlobNotFound(hash.to_string()))?;

        Ok(IrohChunkIdMapper {
            hash: hash.to_string(),
            num_chunks: reader.size().div_ceil(CHUNK_SIZE),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use futures::StreamExt;
    use tokio;

    #[cfg(test)]
    fn bytes_to_stream<T>(
        bytes: T,
    ) -> impl Stream<Item = Result<Bytes, StorageError>> + Send + Unpin
    where
        T: Into<Bytes> + Send,
    {
        Box::pin(futures::stream::once(async move {
            Ok::<Bytes, StorageError>(bytes.into())
        }))
    }

    async fn collect_chunks(storage: &IrohStorage, hash: &str) -> Result<Vec<Bytes>> {
        let stream = storage.iter_chunks(hash).await?;
        let results: Vec<Result<Bytes, StorageError>> = stream.map(|res| res.1).collect().await;
        let bytes_vec = results.into_iter().collect::<Result<Vec<_>, _>>()?;
        Ok(bytes_vec)
    }

    #[tokio::test]
    async fn test_iter_chunks_small_blob() -> Result<()> {
        let storage = IrohStorage::new_in_memory().await?;
        let data = Bytes::from("Hello, World!");
        let upload_result = storage.upload_bytes(bytes_to_stream(data.clone())).await?;
        let hash = upload_result.hash;

        let chunks = collect_chunks(&storage, &hash).await?;

        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0], data);
        Ok(())
    }

    #[tokio::test]
    async fn test_iter_chunks_large_blob() -> Result<()> {
        let storage = IrohStorage::new_in_memory().await?;
        let data = Bytes::from(vec![0u8; 3000]); // 3000 bytes, should be 3 chunks
        let upload_result = storage.upload_bytes(bytes_to_stream(data.clone())).await?;
        let hash = upload_result.hash;

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
        let upload_result = storage.upload_bytes(bytes_to_stream(data)).await?;
        let hash = upload_result.hash;

        let chunks = collect_chunks(&storage, &hash).await?;

        assert_eq!(chunks.len(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn test_iter_chunks_exact_multiple() -> Result<()> {
        let storage = IrohStorage::new_in_memory().await?;
        let data = Bytes::from(vec![0u8; 2048]);
        let upload_result = storage.upload_bytes(bytes_to_stream(data.clone())).await?;
        let hash = upload_result.hash;

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
        let upload_result = storage.upload_bytes(bytes_to_stream(data.clone())).await?;
        let hash = upload_result.hash;

        let chunk = storage.download_chunk(&hash, 0).await?;
        assert_eq!(chunk, data);
        Ok(())
    }

    #[tokio::test]
    async fn test_download_chunk_large_blob() -> Result<()> {
        let storage = IrohStorage::new_in_memory().await?;
        let data = Bytes::from(vec![0u8; 3000]); // 3000 bytes, should be 3 chunks
        let upload_result = storage.upload_bytes(bytes_to_stream(data.clone())).await?;
        let hash = upload_result.hash;

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
        let upload_result = storage.upload_bytes(bytes_to_stream(data.clone())).await?;
        let hash = upload_result.hash;

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
        let upload_result = storage.upload_bytes(bytes_to_stream(data)).await?;
        let hash = upload_result.hash;

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
        let upload_result = storage.upload_bytes(bytes_to_stream(data)).await?;
        let hash = upload_result.hash;

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

    #[tokio::test]
    async fn test_upload_bytes_metadata() -> Result<()> {
        let storage = IrohStorage::new_in_memory().await?;
        let data = Bytes::from("Hello, World!");
        let upload_result = storage.upload_bytes(bytes_to_stream(data.clone())).await?;

        // Verify the UploadResult contains expected fields
        assert!(!upload_result.hash.is_empty(), "Hash should not be empty");
        assert_eq!(
            upload_result.size,
            data.len() as u64,
            "Size should match data length"
        );
        assert!(
            upload_result.info.contains_key("tag"),
            "Should contain tag info"
        );
        assert!(
            upload_result
                .info
                .get("tag")
                .is_some_and(|tag| tag.starts_with("ent-") && tag.len() == 40), // 4 + 36 (uuid)
            "Tag should be in the format \"ent-<uuid>\""
        );

        Ok(())
    }
}
