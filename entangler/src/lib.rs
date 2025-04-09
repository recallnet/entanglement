// Copyright 2024 Entanglement Contributors
// SPDX-License-Identifier: Apache-2.0, MIT

//! Alpha Entanglement is a library for creating redundant data storage with automatic repair capabilities.
//! It implements Alpha Entanglement codes which provide robust error correction.
//!
//! # Example
//!
//! ```rust
//! use recall_entangler::{Config, Entangler};
//! use recall_entangler_storage::iroh::IrohStorage;
//! use tokio::fs::File;
//! use tokio_util::io::ReaderStream;
//! use anyhow::Result;
//!
//! #[tokio::main]
//! async fn main() -> Result<()> {
//!     // Create an in-memory IROH storage backend
//!     let storage = IrohStorage::new_in_memory().await?;
//!
//!     // Initialize entangler with configuration
//!     let entangler = Entangler::new(storage, Config::new(3, 3))?;
//!
//!     // Open Cargo.toml and create a stream
//!     let file = File::open("Cargo.toml").await?;
//!     let input_stream = ReaderStream::new(file);
//!
//!     // Upload and entangle data
//!     let result = entangler.upload(input_stream).await?;
//!
//!     println!("Original hash: {}", result.orig_hash);
//!     println!("Metadata hash: {}", result.metadata_hash);
//!
//!     // Download data (auto-repairs if chunks are corrupted/missing)
//!     let stream = entangler.download(&result.orig_hash, Some(&result.metadata_hash)).await?;
//!
//!     // Process the stream...
//!     Ok(())
//! }
//! ```
//!
//! The library provides:
//! - Configurable redundancy levels through `alpha` and `s` parameters
//! - Automatic repair of corrupted or missing data
//! - Streaming support for efficient processing
//! - Storage-agnostic interface for integration with various backends
//! - Range-based operations for partial data access

pub mod entangler;
pub use entangler::*;
pub mod metadata;
pub use metadata::Metadata;
pub mod config;
pub use config::*;
pub mod parity;

mod executer;
mod grid;
mod lattice;
mod repairer;
mod stream;

#[cfg(test)]
mod printer;
