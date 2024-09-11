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
