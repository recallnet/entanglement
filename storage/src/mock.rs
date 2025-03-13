// Copyright 2024 Entanglement Contributors
// SPDX-License-Identifier: Apache-2.0, MIT

use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use cid::Cid;
use futures::{future::ready, stream, Stream};
use multihash::{Code, MultihashDigest};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::sync::RwLock;
use std::sync::{Arc, Mutex};

use crate::storage::{
    ByteStream, ChunkIdMapper, ChunkStream, Error as StorageError, Storage, UploadResult,
};

// Helper function to convert bytes to a stream for tests
#[allow(dead_code)]
fn bytes_to_stream<T>(bytes: T) -> impl Stream<Item = Result<Bytes, StorageError>> + Send + Unpin
where
    T: Into<Bytes> + Send + 'static,
{
    Box::pin(futures::stream::once(async move {
        Ok::<Bytes, StorageError>(bytes.into())
    }))
}

const CHUNK_SIZE: usize = 1024;

/// `FakeStorage` is an in-memory implementation of the `Storage` trait for testing purposes.
/// It allows simulating various storage scenarios, including successful uploads/downloads,
/// chunk failures, and blob failures.
#[derive(Clone)]
pub struct FakeStorage {
    data: Arc<Mutex<HashMap<String, Vec<Bytes>>>>,
    fail_chunks: Arc<Mutex<HashMap<String, Vec<u64>>>>,
    fail_blobs: Arc<Mutex<HashMap<String, bool>>>,
    fail_streams: Arc<Mutex<HashMap<String, usize>>>, // Add this field
}

impl Default for FakeStorage {
    fn default() -> Self {
        Self::new()
    }
}

impl FakeStorage {
    pub fn new() -> Self {
        FakeStorage {
            data: Arc::new(Mutex::new(HashMap::new())),
            fail_chunks: Arc::new(Mutex::new(HashMap::new())),
            fail_blobs: Arc::new(Mutex::new(HashMap::new())),
            fail_streams: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Simulate a failure for a specific chunk of a blob.
    ///
    /// This state is being reset after a subsequent upload of the same blob.
    pub fn fake_failed_chunks(&self, hash: &str, chunks: Vec<u64>) {
        self.fail_chunks
            .lock()
            .unwrap()
            .insert(hash.to_string(), chunks);
    }

    /// Simulate a failure for a specific blob.
    ///
    /// This state is being reset after a subsequent upload of the same blob.
    pub fn fake_failed_download(&self, hash: &str) {
        self.fail_blobs
            .lock()
            .unwrap()
            .insert(hash.to_string(), true);
    }

    /// Simulate a failure in byte stream at specified position.
    /// The stream will provide bytes until the specified position and then fail.
    pub fn fake_failed_stream(&self, hash: &str, fail_at: usize) {
        self.fail_streams
            .lock()
            .unwrap()
            .insert(hash.to_string(), fail_at);
    }
}

#[derive(Clone)]
pub struct FakeChunkIdMapper {
    hash: String,
    num_chunks: u64,
}

impl ChunkIdMapper<u64> for FakeChunkIdMapper {
    fn index_to_id(&self, index: u64) -> Result<u64, StorageError> {
        if index >= self.num_chunks {
            return Err(StorageError::ChunkNotFound(
                index.to_string(),
                self.hash.clone(),
                "Chunk index out of bounds".to_string(),
            ));
        }
        Ok(index)
    }

    fn id_to_index(&self, chunk_id: &u64) -> Result<u64, StorageError> {
        if *chunk_id >= self.num_chunks {
            return Err(StorageError::ChunkNotFound(
                chunk_id.to_string(),
                self.hash.clone(),
                "Chunk id out of bounds".to_string(),
            ));
        }
        Ok(*chunk_id)
    }
}

#[async_trait]
impl Storage for FakeStorage {
    type ChunkId = u64;
    type ChunkIdMapper = FakeChunkIdMapper;

    async fn upload_bytes<S, E>(&self, stream: S) -> Result<UploadResult, StorageError>
    where
        S: Stream<Item = Result<Bytes, E>> + Send + Unpin,
        E: std::error::Error + Send + Sync + 'static,
    {
        // Collect all the bytes from the stream
        use futures::TryStreamExt;
        let mut all_bytes = Vec::new();
        let mut stream = stream.map_err(|e| StorageError::Other(e.to_string()));

        while let Some(bytes_result) = stream.try_next().await? {
            all_bytes.extend_from_slice(&bytes_result);
        }

        let bytes = Bytes::from(all_bytes);
        let size = bytes.len();

        let mut hasher = Sha256::new();
        hasher.update(&bytes);
        let hash = hasher.finalize();

        let multihash = Code::Sha2_256.wrap(&hash).unwrap();
        let cid = Cid::new_v1(0x55, multihash);
        let hash_str = cid.to_string();

        let chunks = bytes
            .chunks(CHUNK_SIZE)
            .map(Bytes::copy_from_slice)
            .collect();
        self.data.lock().unwrap().insert(hash_str.clone(), chunks);

        self.fail_blobs.lock().unwrap().remove(&hash_str);
        self.fail_chunks.lock().unwrap().remove(&hash_str);
        self.fail_streams.lock().unwrap().remove(&hash_str);

        let mut info = std::collections::HashMap::new();
        info.insert("tag".to_string(), format!("tag-{}", hash_str));

        Ok(UploadResult {
            hash: hash_str,
            size: size as u64,
            info,
        })
    }

    async fn chunk_id_mapper(&self, hash: &str) -> Result<FakeChunkIdMapper, StorageError> {
        let num_chunks = match self.data.lock().unwrap().get(hash) {
            Some(chunks) => chunks.len() as u64,
            None => return Err(StorageError::BlobNotFound(hash.to_string())),
        };

        Ok(FakeChunkIdMapper {
            hash: hash.to_string(),
            num_chunks,
        })
    }

    async fn download_bytes(&self, hash: &str) -> Result<ByteStream, StorageError> {
        if self.fail_blobs.lock().unwrap().get(hash).is_some() {
            return Err(StorageError::BlobNotFound(hash.to_string()));
        }

        let data = self
            .data
            .lock()
            .unwrap()
            .get(hash)
            .map(|chunks| chunks.concat())
            .ok_or_else(|| StorageError::BlobNotFound(hash.to_string()))?;

        let fail_at = self.fail_streams.lock().unwrap().get(hash).cloned();

        // Return a stream that will fail at the specified position
        if let Some(fail_pos) = fail_at {
            let mut chunks = Vec::new();
            let mut remaining = fail_pos;

            // Only output complete chunks that fit within fail_pos
            while remaining >= CHUNK_SIZE {
                let position = fail_pos - remaining;
                chunks.push(Ok(Bytes::copy_from_slice(
                    &data[position..position + CHUNK_SIZE],
                )));
                remaining -= CHUNK_SIZE;
            }

            // Add any remaining partial chunk before failure
            if remaining > 0 {
                let position = fail_pos - remaining;
                chunks.push(Ok(Bytes::copy_from_slice(&data[position..fail_pos])));
            }

            // Add the failure
            chunks.push(Err(StorageError::Other(format!(
                "Stream failed at position {}",
                fail_pos
            ))));

            Ok(Box::pin(futures::stream::iter(chunks)))
        } else {
            // Normal stream without failures
            let chunks = data
                .chunks(CHUNK_SIZE)
                .map(|chunk| Ok(Bytes::copy_from_slice(chunk)))
                .collect::<Vec<_>>();
            Ok(Box::pin(futures::stream::iter(chunks)))
        }
    }

    async fn iter_chunks(&self, hash: &str) -> Result<ChunkStream<Self::ChunkId>, StorageError> {
        let chunks = self
            .data
            .lock()
            .unwrap()
            .get(hash)
            .cloned()
            .ok_or_else(|| StorageError::BlobNotFound(hash.to_string()))?;

        let fail_chunks = self
            .fail_chunks
            .lock()
            .unwrap()
            .get(hash)
            .cloned()
            .unwrap_or_default();

        let hash = hash.to_string();
        let stream = stream::iter(chunks.into_iter().enumerate().map(move |(index, chunk)| {
            let index = index as Self::ChunkId;
            if fail_chunks.contains(&index) {
                (
                    index,
                    Err(StorageError::ChunkNotFound(
                        index.to_string(),
                        hash.clone(),
                        "Simulated chunk failure".to_string(),
                    )),
                )
            } else {
                (index, Ok(chunk))
            }
        }));

        Ok(Box::pin(stream))
    }

    async fn download_chunk(&self, hash: &str, chunk_id: u64) -> Result<Bytes, StorageError> {
        let fail_chunks = self
            .fail_chunks
            .lock()
            .unwrap()
            .get(hash)
            .cloned()
            .unwrap_or_default();

        if fail_chunks.contains(&chunk_id) {
            return Err(StorageError::ChunkNotFound(
                chunk_id.to_string(),
                hash.to_string(),
                "Simulated chunk failure".to_string(),
            ));
        }

        let data = self.data.lock().unwrap();
        let chunks = data.get(hash);
        if let Some(chunks) = chunks {
            if chunk_id >= chunks.len() as u64 {
                return Err(StorageError::ChunkNotFound(
                    chunk_id.to_string(),
                    hash.to_string(),
                    "Chunk not found".to_string(),
                ));
            }
        }
        chunks
            .map(|chunks| chunks[chunk_id as usize].clone())
            .ok_or_else(|| StorageError::BlobNotFound(hash.to_string()))
    }
}

#[derive(Clone)]
pub struct DummyStorage;

#[derive(Clone)]
pub struct DummyChunkIdMapper {}

impl ChunkIdMapper<u64> for DummyChunkIdMapper {
    fn index_to_id(&self, index: u64) -> Result<u64, StorageError> {
        Ok(index)
    }

    fn id_to_index(&self, chunk_id: &u64) -> Result<u64, StorageError> {
        Ok(*chunk_id)
    }
}

#[async_trait]
impl Storage for DummyStorage {
    type ChunkId = u64;
    type ChunkIdMapper = DummyChunkIdMapper;

    async fn upload_bytes<S, E>(&self, _stream: S) -> Result<UploadResult, StorageError>
    where
        S: Stream<Item = Result<Bytes, E>> + Send + Unpin,
        E: std::error::Error + Send + Sync + 'static,
    {
        // Just create a fixed response for the dummy storage
        let mut info = std::collections::HashMap::new();
        info.insert("mock_info".to_string(), "dummy_storage".to_string());

        Ok(UploadResult {
            hash: "dummy_hash".to_string(),
            info,
            size: 10, // Fixed dummy size
        })
    }

    async fn chunk_id_mapper(&self, _: &str) -> Result<Self::ChunkIdMapper, StorageError> {
        Ok(DummyChunkIdMapper {})
    }

    async fn download_bytes(&self, _: &str) -> Result<ByteStream, StorageError> {
        Ok(Box::pin(futures::stream::once(ready(Ok(Bytes::from(
            "dummy data",
        ))))))
    }

    async fn iter_chunks(&self, _: &str) -> Result<ChunkStream<Self::ChunkId>, StorageError> {
        let chunks = vec![Bytes::from("dummy data")];

        let stream = futures::stream::iter(
            chunks
                .into_iter()
                .enumerate()
                .map(move |(index, chunk)| (index as u64, Ok(chunk))),
        );

        Ok(Box::pin(stream))
    }

    async fn download_chunk(&self, _: &str, _: Self::ChunkId) -> Result<Bytes, StorageError> {
        Ok(Bytes::from("dummy data"))
    }
}

#[derive(Clone)]
pub struct StubStorage {
    upload_bytes_result: Result<UploadResult, StorageError>,
    download_bytes_result: HashMap<Option<String>, Result<Bytes, StorageError>>,
    iter_chunks_result: Vec<(u64, Result<Bytes, StorageError>)>,
    download_chunk_result: HashMap<Option<(String, u64)>, Result<Bytes, StorageError>>,
    chunk_id_mapper_result: Result<DummyChunkIdMapper, StorageError>,
}

impl Default for StubStorage {
    fn default() -> Self {
        let mut info = std::collections::HashMap::new();
        info.insert("mock_info".to_string(), "stub_storage".to_string());

        Self {
            upload_bytes_result: Ok(UploadResult {
                hash: "dummy_hash".to_string(),
                info,
                size: 0,
            }),
            download_bytes_result: HashMap::new(),
            iter_chunks_result: vec![(0, Ok(Bytes::from("dummy data")))],
            download_chunk_result: HashMap::new(),
            chunk_id_mapper_result: Ok(DummyChunkIdMapper {}),
        }
    }
}

impl StubStorage {
    pub fn stub_upload_bytes(&mut self, result: Result<UploadResult, StorageError>) {
        self.upload_bytes_result = result;
    }

    pub fn stub_download_bytes(
        &mut self,
        hash: Option<String>,
        result: Result<Bytes, StorageError>,
    ) {
        self.download_bytes_result.insert(hash, result);
    }

    pub fn stub_iter_chunks(&mut self, chunks: Vec<(u64, Result<Bytes, StorageError>)>) {
        self.iter_chunks_result = chunks;
    }

    pub fn stub_download_chunk(
        &mut self,
        params: Option<(String, u64)>,
        result: Result<Bytes, StorageError>,
    ) {
        self.download_chunk_result.insert(params, result);
    }

    pub fn stub_chunk_id_mapper(&mut self, result: Result<DummyChunkIdMapper, StorageError>) {
        self.chunk_id_mapper_result = result;
    }
}

#[async_trait]
impl Storage for StubStorage {
    type ChunkId = u64;
    type ChunkIdMapper = DummyChunkIdMapper;

    async fn upload_bytes<S, E>(&self, _stream: S) -> Result<UploadResult, StorageError>
    where
        S: Stream<Item = Result<Bytes, E>> + Send + Unpin,
        E: std::error::Error + Send + Sync + 'static,
    {
        self.upload_bytes_result.clone()
    }

    async fn download_bytes(&self, hash: &str) -> Result<ByteStream, StorageError> {
        let result = self
            .download_bytes_result
            .get(&Some(hash.to_string()))
            .or_else(|| self.download_bytes_result.get(&None))
            .cloned()
            .unwrap_or_else(|| Ok(Bytes::from("dummy data")))?;

        Ok(Box::pin(futures::stream::once(ready(Ok(result)))))
    }

    async fn iter_chunks(&self, _: &str) -> Result<ChunkStream<Self::ChunkId>, StorageError> {
        Ok(Box::pin(futures::stream::iter(
            self.iter_chunks_result.clone().into_iter(),
        )))
    }

    async fn download_chunk(
        &self,
        hash: &str,
        chunk_id: Self::ChunkId,
    ) -> Result<Bytes, StorageError> {
        self.download_chunk_result
            .get(&Some((hash.to_string(), chunk_id)))
            .or_else(|| self.download_chunk_result.get(&None))
            .cloned()
            .unwrap_or_else(|| Ok(Bytes::from("dummy data")))
    }

    async fn chunk_id_mapper(&self, _: &str) -> Result<Self::ChunkIdMapper, StorageError> {
        self.chunk_id_mapper_result.clone()
    }
}

#[derive(Clone)]
pub struct SpyStorage {
    inner: StubStorage,
    upload_bytes_calls: Arc<RwLock<Vec<Bytes>>>,
    download_bytes_calls: Arc<RwLock<Vec<String>>>,
    iter_chunks_calls: Arc<RwLock<Vec<String>>>,
    download_chunk_calls: Arc<RwLock<Vec<(String, u64)>>>,
    chunk_id_mapper_calls: Arc<RwLock<Vec<String>>>,
}

impl std::ops::Deref for SpyStorage {
    type Target = StubStorage;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl std::ops::DerefMut for SpyStorage {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl Default for SpyStorage {
    fn default() -> Self {
        Self {
            inner: StubStorage::default(),
            upload_bytes_calls: Arc::new(RwLock::new(Vec::new())),
            download_bytes_calls: Arc::new(RwLock::new(Vec::new())),
            iter_chunks_calls: Arc::new(RwLock::new(Vec::new())),
            download_chunk_calls: Arc::new(RwLock::new(Vec::new())),
            chunk_id_mapper_calls: Arc::new(RwLock::new(Vec::new())),
        }
    }
}

impl SpyStorage {
    pub fn upload_bytes_calls(&self) -> Vec<Bytes> {
        self.upload_bytes_calls.read().unwrap().clone()
    }

    pub fn download_bytes_calls(&self) -> Vec<String> {
        self.download_bytes_calls.read().unwrap().clone()
    }

    pub fn iter_chunks_calls(&self) -> Vec<String> {
        self.iter_chunks_calls.read().unwrap().clone()
    }

    pub fn download_chunk_calls(&self) -> Vec<(String, u64)> {
        self.download_chunk_calls.read().unwrap().clone()
    }

    pub fn chunk_id_mapper_calls(&self) -> Vec<String> {
        self.chunk_id_mapper_calls.read().unwrap().clone()
    }
}

#[async_trait]
impl Storage for SpyStorage {
    type ChunkId = u64;
    type ChunkIdMapper = DummyChunkIdMapper;

    async fn upload_bytes<S, E>(&self, stream: S) -> Result<UploadResult, StorageError>
    where
        S: Stream<Item = Result<Bytes, E>> + Send + Unpin,
        E: std::error::Error + Send + Sync + 'static,
    {
        // Collect all bytes from the stream to record them
        use futures::TryStreamExt;
        let mut all_bytes = Vec::new();
        let mut stream = stream.map_err(|e| StorageError::Other(e.to_string()));

        while let Some(bytes_result) = stream.try_next().await? {
            all_bytes.extend_from_slice(&bytes_result);
        }

        let bytes = Bytes::from(all_bytes);
        self.upload_bytes_calls.write().unwrap().push(bytes.clone());

        // Create a new stream with the same bytes for the inner storage
        let inner_stream = Box::pin(futures::stream::once(async move {
            Ok::<Bytes, StorageError>(bytes)
        }));
        self.inner.upload_bytes(inner_stream).await
    }

    async fn download_bytes(&self, hash: &str) -> Result<ByteStream, StorageError> {
        self.download_bytes_calls
            .write()
            .unwrap()
            .push(hash.to_string());
        self.inner.download_bytes(hash).await
    }

    async fn iter_chunks(&self, hash: &str) -> Result<ChunkStream<Self::ChunkId>, StorageError> {
        self.iter_chunks_calls
            .write()
            .unwrap()
            .push(hash.to_string());
        self.inner.iter_chunks(hash).await
    }

    async fn download_chunk(
        &self,
        hash: &str,
        chunk_id: Self::ChunkId,
    ) -> Result<Bytes, StorageError> {
        self.download_chunk_calls
            .write()
            .unwrap()
            .push((hash.to_string(), chunk_id));
        self.inner.download_chunk(hash, chunk_id).await
    }

    async fn chunk_id_mapper(&self, hash: &str) -> Result<Self::ChunkIdMapper, StorageError> {
        self.chunk_id_mapper_calls
            .write()
            .unwrap()
            .push(hash.to_string());
        self.inner.chunk_id_mapper(hash).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::{Stream, StreamExt};
    use std::task::{Context, Poll};

    #[tokio::test]
    async fn test_upload_and_download_bytes() -> Result<()> {
        let storage = FakeStorage::new();
        let data = b"Hello, world!".to_vec();
        let result = storage.upload_bytes(bytes_to_stream(data.clone())).await?;

        // Verify the UploadResult contains the expected fields
        assert!(!result.hash.is_empty());
        assert!(result.info.contains_key("tag"));
        assert_eq!(
            *result.info.get("tag").unwrap(),
            format!("tag-{}", result.hash)
        );

        let mut stream = storage.download_bytes(&result.hash).await?;
        let mut downloaded = Vec::new();
        while let Some(chunk) = stream.next().await {
            downloaded.extend_from_slice(&chunk?);
        }
        assert_eq!(data, downloaded);
        Ok(())
    }

    #[tokio::test]
    async fn test_download_non_existent_blob() {
        let storage = FakeStorage::new();
        let result = storage.download_bytes("non_existent_hash").await;
        assert!(matches!(result, Err(StorageError::BlobNotFound(_))));
    }

    #[tokio::test]
    async fn test_fake_failed_download() -> Result<()> {
        let storage = FakeStorage::new();
        let data = b"Test data".to_vec();
        let result = storage.upload_bytes(bytes_to_stream(data)).await?;

        storage.fake_failed_download(&result.hash);
        let result = storage.download_bytes(&result.hash).await;
        assert!(matches!(result, Err(StorageError::BlobNotFound(_))));
        Ok(())
    }

    #[tokio::test]
    async fn faked_failed_blob_download_after_upload_should_be_available() -> Result<()> {
        let storage = FakeStorage::new();
        let data = b"Test data".to_vec();
        let result = storage.upload_bytes(bytes_to_stream(data.clone())).await?;

        let dl_result = storage.download_bytes(&result.hash).await;
        assert!(dl_result.is_ok());

        storage.fake_failed_chunks(&result.hash, vec![0]);
        let new_result = storage.upload_bytes(bytes_to_stream(data)).await?;

        let chunk_result = storage.download_chunk(&new_result.hash, 0).await;
        assert!(
            chunk_result.is_ok(),
            "Expected download to succeed after upload"
        );
        Ok(())
    }

    #[tokio::test]
    async fn faked_failed_chunk_download_after_upload_should_be_available() -> Result<()> {
        let storage = FakeStorage::new();
        let data = b"Test data".to_vec();
        let result = storage.upload_bytes(bytes_to_stream(data.clone())).await?;

        let dl_result = storage.download_bytes(&result.hash).await;
        assert!(dl_result.is_ok());

        storage.fake_failed_download(&result.hash);
        let new_result = storage.upload_bytes(bytes_to_stream(data)).await?;

        let dl_result = storage.download_bytes(&new_result.hash).await;
        assert!(
            dl_result.is_ok(),
            "Expected download to succeed after upload"
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_iter_chunks() -> Result<()> {
        let storage = FakeStorage::new();
        let data = (0..3000).map(|i| (i % 256) as u8).collect::<Vec<u8>>(); // 3 chunks with predictable content
        let result = storage.upload_bytes(bytes_to_stream(data.clone())).await?;

        let mut stream = storage.iter_chunks(&result.hash).await?;
        let mut chunk_count = 0;
        let mut total_bytes = 0;

        while let Some((_, chunk_result)) = stream.next().await {
            let chunk = chunk_result?;
            chunk_count += 1;

            // Check the content of each chunk
            let start = total_bytes;
            let end = total_bytes + chunk.len();
            assert_eq!(
                &data[start..end],
                &chunk[..],
                "Chunk {} content mismatch",
                chunk_count
            );

            total_bytes += chunk.len();
        }

        assert_eq!(chunk_count, 3, "Expected 3 chunks");
        assert_eq!(total_bytes, data.len(), "Total bytes mismatch");

        let last_chunk_size = data.len() % CHUNK_SIZE;
        assert_eq!(
            total_bytes % CHUNK_SIZE,
            last_chunk_size,
            "Last chunk size mismatch"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_iter_chunks_non_existent_blob() {
        let storage = FakeStorage::new();
        let result = storage.iter_chunks("non_existent_hash").await;
        assert!(matches!(result, Err(StorageError::BlobNotFound(_))));
    }

    #[tokio::test]
    async fn test_fake_failed_chunks() -> Result<()> {
        let storage = FakeStorage::new();
        let data = vec![0u8; 3000]; // 3 chunks
        let result = storage.upload_bytes(bytes_to_stream(data)).await?;

        let fail_chunk_index = 1;
        storage.fake_failed_chunks(&result.hash, vec![fail_chunk_index]);

        let mut stream = storage.iter_chunks(&result.hash).await?;
        let mut chunk_results = Vec::new();

        while let Some(chunk_result) = stream.next().await {
            chunk_results.push(chunk_result);
        }

        assert_eq!(chunk_results.len(), 3, "Expected 3 chunk results");

        for (index, chunk_result) in chunk_results {
            if index == fail_chunk_index {
                assert!(chunk_result.is_err(), "Expected chunk {} to fail", index);
                let err = chunk_result.unwrap_err();
                if let StorageError::ChunkNotFound(index_str, hash, _) = err {
                    assert_eq!(index_str, "1");
                    assert_eq!(hash, result.hash);
                } else {
                    panic!("Expected ChunkNotFound error");
                }
            } else {
                assert!(chunk_result.is_ok(), "Expected chunk {} to succeed", index);
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_large_upload() -> Result<()> {
        let storage = FakeStorage::new();
        let data = vec![0u8; 10 * CHUNK_SIZE * CHUNK_SIZE]; // 10 MB
        let result = storage.upload_bytes(bytes_to_stream(data.clone())).await?;

        let mut stream = storage.download_bytes(&result.hash).await?;
        let mut downloaded = Vec::new();
        while let Some(chunk) = stream.next().await {
            downloaded.extend_from_slice(&chunk?);
        }
        assert_eq!(data.len(), downloaded.len());
        assert_eq!(data, downloaded);
        Ok(())
    }

    #[tokio::test]
    async fn test_empty_upload() -> Result<()> {
        let storage = FakeStorage::new();
        let data = vec![];
        let result = storage.upload_bytes(bytes_to_stream(data.clone())).await?;

        let mut stream = storage.download_bytes(&result.hash).await?;
        let mut downloaded = Vec::new();
        while let Some(chunk) = stream.next().await {
            downloaded.extend_from_slice(&chunk?);
        }
        assert_eq!(data, downloaded);
        Ok(())
    }

    #[tokio::test]
    async fn test_concurrent_uploads() -> Result<()> {
        let storage = FakeStorage::new();
        let data1 = b"Data 1".to_vec();
        let data2 = b"Data 2".to_vec();

        let (result1, result2) = tokio::join!(
            storage.upload_bytes(bytes_to_stream(data1.clone())),
            storage.upload_bytes(bytes_to_stream(data2.clone()))
        );

        let result1 = result1?;
        let result2 = result2?;
        assert_ne!(result1.hash, result2.hash);

        let (stream1, stream2) = tokio::join!(
            storage.download_bytes(&result1.hash),
            storage.download_bytes(&result2.hash)
        );

        // Collect chunks from both streams concurrently
        let (chunks1, chunks2) = futures::future::join(
            async {
                let mut result = Vec::new();
                let mut stream = stream1?;
                while let Some(chunk) = stream.next().await {
                    result.extend_from_slice(&chunk?);
                }
                Result::<Vec<u8>, StorageError>::Ok(result)
            },
            async {
                let mut result = Vec::new();
                let mut stream = stream2?;
                while let Some(chunk) = stream.next().await {
                    result.extend_from_slice(&chunk?);
                }
                Result::<Vec<u8>, StorageError>::Ok(result)
            },
        )
        .await;

        assert_eq!(data1, chunks1?);
        assert_eq!(data2, chunks2?);
        Ok(())
    }

    #[tokio::test]
    async fn test_download_chunk() -> Result<()> {
        let storage = FakeStorage::new();
        let data = vec![0u8; 3000]; // 3 chunks
        let result = storage.upload_bytes(bytes_to_stream(data.clone())).await?;

        for chunk_id in 0..3 {
            let chunk = storage.download_chunk(&result.hash, chunk_id).await?;
            let b = chunk_id as usize * CHUNK_SIZE;
            let e = (b + CHUNK_SIZE).min(data.len());
            let expected_chunk = &data[b..e];
            assert_eq!(chunk, expected_chunk);
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_download_chunk_non_existent_blob() {
        let storage = FakeStorage::new();
        let result = storage.download_chunk("non_existent_hash", 0).await;
        assert!(matches!(result, Err(StorageError::BlobNotFound(_))));
    }

    #[tokio::test]
    async fn test_download_chunk_out_of_bounds() -> Result<()> {
        let storage = FakeStorage::new();
        let data = vec![0u8; 3000]; // 3 chunks
        let result = storage.upload_bytes(bytes_to_stream(data)).await?;

        let err = storage.download_chunk(&result.hash, 3).await.unwrap_err();
        if let StorageError::ChunkNotFound(chunk_id, hash, _) = err {
            assert_eq!(chunk_id, "3");
            assert_eq!(hash, result.hash);
        } else {
            panic!("Expected ChunkNotFound error");
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_fake_failed_chunk_download() -> Result<()> {
        let storage = FakeStorage::new();
        let data = vec![0u8; 3000]; // 3 chunks
        let result = storage.upload_bytes(bytes_to_stream(data)).await?;

        let fail_chunk_index = 1;
        storage.fake_failed_chunks(&result.hash, vec![fail_chunk_index]);

        for chunk_id in 0..3 {
            let chunk_result = storage.download_chunk(&result.hash, chunk_id).await;
            if chunk_id == fail_chunk_index {
                let err = chunk_result.unwrap_err();
                if let StorageError::ChunkNotFound(chunk_id_str, hash, _) = err {
                    assert_eq!(chunk_id_str, "1");
                    assert_eq!(hash, result.hash);
                } else {
                    panic!("Expected ChunkNotFound error");
                }
            } else {
                assert!(
                    chunk_result.is_ok(),
                    "Expected chunk {} to download successfully",
                    chunk_id
                );
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_chunk_id_mapper() -> Result<()> {
        let storage = FakeStorage::new();
        let data = vec![0u8; 3000]; // 3 chunks
        let result = storage.upload_bytes(bytes_to_stream(data)).await?;

        let mapper = storage.chunk_id_mapper(&result.hash).await?;
        assert_eq!(mapper.index_to_id(0)?, 0);
        assert_eq!(mapper.index_to_id(1)?, 1);
        assert_eq!(mapper.index_to_id(2)?, 2);
        assert!(
            matches!(mapper.id_to_index(&3), Err(StorageError::ChunkNotFound(chunk_id, hash, _)) if hash == result.hash && chunk_id == "3"),
        );

        let res = storage.chunk_id_mapper("invalid").await;
        assert!(res.is_err(), "Expected error");
        assert!(matches!(res.err().unwrap(), StorageError::BlobNotFound(h) if h == "invalid"));
        Ok(())
    }

    #[tokio::test]
    async fn test_fake_failed_stream() -> Result<()> {
        struct TestCase {
            total_size: usize,
            fail_at: usize,
        }

        let test_cases = [
            TestCase {
                total_size: 3000, // ~3 chunks
                fail_at: 1500,    // Middle of second chunk
            },
            TestCase {
                total_size: 1000, // <1 chunk
                fail_at: 500,     // Middle of first chunk
            },
            TestCase {
                total_size: 5000, // ~5 chunks
                fail_at: 3072,    // End of third chunk
            },
            TestCase {
                total_size: 2048, // 2 chunks exactly
                fail_at: 1024,    // At first chunk boundary
            },
        ];

        for (i, test_case) in test_cases.iter().enumerate() {
            let storage = FakeStorage::new();
            let data = vec![i as u8; test_case.total_size];
            let result = storage.upload_bytes(bytes_to_stream(data.clone())).await?;

            storage.fake_failed_stream(&result.hash, test_case.fail_at);

            let mut stream = storage.download_bytes(&result.hash).await?;
            let mut downloaded = Vec::new();
            let mut error_occurred = false;

            while let Some(chunk_result) = stream.next().await {
                match chunk_result {
                    Ok(chunk) => downloaded.extend_from_slice(&chunk),
                    Err(_) => {
                        error_occurred = true;
                        break;
                    }
                }
            }

            assert!(
                error_occurred,
                "Case {}: Expected stream to fail at {} bytes",
                i, test_case.fail_at
            );
            assert_eq!(
                downloaded.len(),
                test_case.fail_at,
                "Case {}: Expected exactly {} bytes before failure",
                i,
                test_case.fail_at
            );
            assert_eq!(
                &data[..test_case.fail_at],
                &downloaded[..],
                "Case {}: Data mismatch before failure point",
                i
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn faked_failed_stream_after_upload_should_be_available() -> Result<()> {
        let storage = FakeStorage::new();
        let data = vec![0u8; 3000]; // 3 chunks
        let result = storage.upload_bytes(bytes_to_stream(data.clone())).await?;

        // First verify stream fails at 1500 bytes
        storage.fake_failed_stream(&result.hash, 1500);
        let mut stream = storage.download_bytes(&result.hash).await?;
        let mut downloaded = Vec::new();
        while let Some(chunk_result) = stream.next().await {
            match chunk_result {
                Ok(chunk) => downloaded.extend_from_slice(&chunk),
                Err(_) => break,
            }
        }
        assert_eq!(
            downloaded.len(),
            1500,
            "Stream should have failed at 1500 bytes"
        );

        // Now upload the same data again and verify stream works completely
        let new_result = storage.upload_bytes(bytes_to_stream(data.clone())).await?;
        let mut stream = storage.download_bytes(&new_result.hash).await?;
        let mut downloaded = Vec::with_capacity(3000);
        while let Some(chunk_result) = stream.next().await {
            downloaded.extend_from_slice(&chunk_result?);
        }
        assert_eq!(
            downloaded.len(),
            3000,
            "Stream should complete after re-upload"
        );
        assert_eq!(data, downloaded, "Downloaded data should match original");

        Ok(())
    }

    #[tokio::test]
    async fn test_upload_and_download_bytes_with_poll() -> Result<()> {
        let storage = FakeStorage::new();
        let data = vec![1u8; 3000];
        let result = storage.upload_bytes(bytes_to_stream(data.clone())).await?;

        let stream = storage.download_bytes(&result.hash).await?;
        let mut pinned = Box::pin(stream);
        let mut downloaded = Vec::new();
        let mut error_received = false;

        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);

        loop {
            match pinned.as_mut().poll_next(&mut cx) {
                Poll::Ready(Some(Ok(chunk))) => downloaded.extend_from_slice(&chunk),
                Poll::Ready(Some(Err(_))) => {
                    error_received = true;
                    break;
                }
                Poll::Ready(None) => break,
                Poll::Pending => panic!("Stream should not be pending in this test"),
            }
        }

        assert!(!error_received, "No error should be received");
        assert_eq!(data, downloaded);
        Ok(())
    }

    #[tokio::test]
    async fn test_upload_and_download_bytes_with_poll_failed_stream() -> Result<()> {
        let storage = FakeStorage::new();
        let data = vec![1u8; 3000];
        let result = storage.upload_bytes(bytes_to_stream(data.clone())).await?;

        storage.fake_failed_stream(&result.hash, 1500);

        let stream = storage.download_bytes(&result.hash).await?;
        let mut pinned = Box::pin(stream);
        let mut downloaded = Vec::new();
        let mut error_received = false;

        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);

        loop {
            match pinned.as_mut().poll_next(&mut cx) {
                Poll::Ready(Some(Ok(chunk))) => downloaded.extend_from_slice(&chunk),
                Poll::Ready(Some(Err(_))) => {
                    error_received = true;
                    break;
                }
                Poll::Ready(None) => break,
                Poll::Pending => panic!("Stream should not be pending in this test"),
            }
        }

        assert!(error_received, "Error should be received");
        assert_eq!(downloaded.len(), 1500, "Should receive exactly 1500 bytes");
        assert_eq!(
            &data[..1500],
            &downloaded[..],
            "Data mismatch before failure"
        );
        Ok(())
    }
}
