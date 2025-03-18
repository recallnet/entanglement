// Copyright 2024 Entanglement Contributors
// SPDX-License-Identifier: Apache-2.0, MIT

//! # Entanglement Storage Library
//!
//! The Entanglement Storage Library provides a unified interface for interacting with various storage backends.
//! It defines traits and implementations for uploading, downloading, and managing data in chunks. The library
//! supports both in-memory and persistent storage solutions, making it suitable for a wide range of applications.
//!
//! ## Modules
//!
//! - [`iroh`](iroh): Contains the implementation for the Iroh storage backend.
//! - [`storage`](storage): Defines the core traits and types for storage operations.
//! - [`mock`](mock): Provides a mock implementation of the storage traits for testing purposes.
//!
//! ## Traits
//!
//! - [`Storage`](storage::Storage): Represents a storage backend capable of uploading, downloading, and streaming data in chunks.
//! - [`ChunkId`](storage::ChunkId): A trait used to identify chunks.
//! - [`ChunkIdMapper`](storage::ChunkIdMapper): A trait for mapping chunk indices to chunk ids and vice versa.
//!
//! ## Error Handling
//!
//! The library uses the [`Error`](storage::Error) enum to represent various errors that can occur during storage operations.
//!
//! ## Usage
//!
//! To use the library, you need to implement the [`Storage`](storage::Storage) trait for your storage backend. The library provides
//! a default implementation for in-memory storage through the [`FakeStorage`](mock::FakeStorage) struct, which can be used for testing.
//!
//! ### Example
//!
//! ```rust
//! pub mod storage;
//! pub mod iroh;
//!
//! use crate::storage::Storage;
//! use crate::iroh::IrohStorage;
//! use bytes::Bytes;
//! use futures::{StreamExt, Stream};
//!
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     let storage = IrohStorage::new_in_memory().await?;
//!     let data = b"Hello, world!".to_vec();
//!     let data_clone = data.clone();
//!     // Create a stream from the data
//!     let data_stream = Box::pin(futures::stream::once(async move {
//!         Ok::<Bytes, std::io::Error>(Bytes::from(data_clone))
//!     }));
//!     let upload_result = storage.upload_bytes(data_stream).await?;
//!
//!     // Download the data as a single blob
//!     let mut stream = storage.download_bytes(&upload_result.hash).await?;
//!     let mut downloaded = Vec::with_capacity(stream.size_hint().0);
//!     while let Some(chunk) = stream.next().await {
//!         downloaded.extend_from_slice(&chunk?);
//!     }
//!     assert_eq!(data, downloaded);
//!
//!     // Iterate through chunks
//!     let mut chunk_stream = storage.iter_chunks(&upload_result.hash).await?;
//!     while let Some((chunk_id, chunk_result)) = chunk_stream.next().await {
//!         let chunk = chunk_result?;
//!         println!("Got chunk {}: {:?} bytes", chunk_id, chunk.len());
//!     }
//!
//!     Ok(())
//! }
//! ```

pub mod iroh;
pub mod storage;
pub use storage::*;

#[cfg(any(test, feature = "mock"))]
pub mod mock;
