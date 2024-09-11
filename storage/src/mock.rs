// Copyright 2024 Entanglement Contributors
// SPDX-License-Identifier: Apache-2.0, MIT

use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use cid::Cid;
use futures::stream;
use multihash::{Code, MultihashDigest};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::storage::{ByteStream, Error as StorageError, Storage};

#[derive(Clone)]
pub struct FakeStorage {
    data: Arc<Mutex<HashMap<String, Vec<Bytes>>>>,
    fail_chunks: Arc<Mutex<HashMap<String, Vec<usize>>>>,
    fail_blobs: Arc<Mutex<HashMap<String, bool>>>,
}

impl FakeStorage {
    pub fn new() -> Self {
        FakeStorage {
            data: Arc::new(Mutex::new(HashMap::new())),
            fail_chunks: Arc::new(Mutex::new(HashMap::new())),
            fail_blobs: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn fake_failed_chunks(&self, hash: &str, chunks: Vec<usize>) {
        self.fail_chunks
            .lock()
            .unwrap()
            .insert(hash.to_string(), chunks);
    }

    pub fn fake_failed_download(&self, hash: &str) {
        self.fail_blobs
            .lock()
            .unwrap()
            .insert(hash.to_string(), true);
    }
}

#[async_trait]
impl Storage for FakeStorage {
    async fn upload_bytes(&self, bytes: impl Into<Bytes> + Send) -> Result<String> {
        let bytes = bytes.into();

        let mut hasher = Sha256::new();
        hasher.update(&bytes);
        let hash = hasher.finalize();

        let multihash = Code::Sha2_256.wrap(&hash).unwrap();

        let cid = Cid::new_v1(0x55, multihash);

        let hash_str = cid.to_string();

        let chunks = bytes.chunks(1024).map(Bytes::copy_from_slice).collect();
        self.data.lock().unwrap().insert(hash_str.clone(), chunks);

        Ok(hash_str)
    }

    async fn download_bytes(&self, hash: &str) -> Result<Bytes, StorageError> {
        if self.fail_blobs.lock().unwrap().get(hash).is_some() {
            return Err(StorageError::BlobNotFound(hash.to_string()));
        }
        self.data
            .lock()
            .unwrap()
            .get(hash)
            .map(|chunks| Bytes::from(chunks.concat()))
            .ok_or_else(|| StorageError::BlobNotFound(hash.to_string()))
    }

    async fn iter_chunks(&self, hash: &str) -> Result<ByteStream, StorageError> {
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

        let stream = stream::iter(chunks.into_iter().enumerate().map(move |(index, chunk)| {
            if fail_chunks.contains(&index) {
                Err(anyhow::anyhow!("Simulated chunk failure"))
            } else {
                Ok(chunk)
            }
        }));

        Ok(Box::pin(stream))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;

    #[tokio::test]
    async fn test_upload_and_download_bytes() -> Result<()> {
        let storage = FakeStorage::new();
        let data = b"Hello, world!".to_vec();
        let hash = storage.upload_bytes(data.clone()).await?;

        let downloaded = storage.download_bytes(&hash).await?;
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
        let hash = storage.upload_bytes(data).await?;

        storage.fake_failed_download(&hash);
        let result = storage.download_bytes(&hash).await;
        assert!(matches!(result, Err(StorageError::BlobNotFound(_))));
        Ok(())
    }

    #[tokio::test]
    async fn test_iter_chunks() -> Result<()> {
        let storage = FakeStorage::new();
        let data = (0..3000).map(|i| (i % 256) as u8).collect::<Vec<u8>>(); // 3 chunks with predictable content
        let hash = storage.upload_bytes(data.clone()).await?;

        let mut stream = storage.iter_chunks(&hash).await?;
        let mut chunk_count = 0;
        let mut total_bytes = 0;

        while let Some(chunk_result) = stream.next().await {
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

        let last_chunk_size = data.len() % 1024;
        assert_eq!(
            total_bytes % 1024,
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
        let hash = storage.upload_bytes(data).await?;

        let fail_chunk_index = 1; // We'll make the second chunk (index 1) fail
        storage.fake_failed_chunks(&hash, vec![fail_chunk_index]);

        let mut stream = storage.iter_chunks(&hash).await?;
        let mut chunk_results = Vec::new();

        while let Some(chunk_result) = stream.next().await {
            chunk_results.push(chunk_result);
        }

        assert_eq!(chunk_results.len(), 3, "Expected 3 chunk results");

        for (index, result) in chunk_results.iter().enumerate() {
            if index == fail_chunk_index {
                assert!(result.is_err(), "Expected chunk {} to fail", index);
                assert!(
                    matches!(result, Err(e) if e.to_string() == "Simulated chunk failure"),
                    "Unexpected error for chunk {}: {:?}",
                    index,
                    result
                );
            } else {
                assert!(result.is_ok(), "Expected chunk {} to succeed", index);
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_large_upload() -> Result<()> {
        let storage = FakeStorage::new();
        let data = vec![0u8; 10 * 1024 * 1024]; // 10 MB
        let hash = storage.upload_bytes(data.clone()).await?;

        let downloaded = storage.download_bytes(&hash).await?;
        assert_eq!(data.len(), downloaded.len());
        assert_eq!(data, downloaded);
        Ok(())
    }

    #[tokio::test]
    async fn test_empty_upload() -> Result<()> {
        let storage = FakeStorage::new();
        let data = vec![];
        let hash = storage.upload_bytes(data.clone()).await?;

        let downloaded = storage.download_bytes(&hash).await?;
        assert_eq!(data, downloaded);
        Ok(())
    }

    #[tokio::test]
    async fn test_concurrent_uploads() -> Result<()> {
        let storage = FakeStorage::new();
        let data1 = b"Data 1".to_vec();
        let data2 = b"Data 2".to_vec();

        let (hash1, hash2) = tokio::join!(
            storage.upload_bytes(data1.clone()),
            storage.upload_bytes(data2.clone())
        );

        let hash1 = hash1?;
        let hash2 = hash2?;

        assert_ne!(hash1, hash2);

        let (downloaded1, downloaded2) = tokio::join!(
            storage.download_bytes(&hash1),
            storage.download_bytes(&hash2)
        );

        assert_eq!(data1, downloaded1?);
        assert_eq!(data2, downloaded2?);
        Ok(())
    }
}
