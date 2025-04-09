# Alpha Entanglement

[![License](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue.svg)](LICENSE)

A robust Rust library that implements Alpha Entanglement codes for resilient distributed storage systems. This library provides configurable data redundancy, automatic repair capabilities, and a flexible storage interface for integrating with various distributed protocols like IROH and IPFS.

## Overview

Alpha Entanglement is an innovative error correction technique for distributed storage systems. It creates redundant parity chunks using strategic XOR operations across multiple "strands" or patterns, allowing for remarkable resilience against data loss even when significant portions of the original data are missing or corrupted.

Key features:

- **Robust Error Correction**: Recovers data when significant portions are lost or damaged
- **Configurable Redundancy**: Adjustable parameters to customize fault tolerance levels
- **Automatic Repair**: Self-healing capabilities for corrupted or missing chunks
- **Storage Agnostic**: Abstract interface that works with any storage backend
- **Streaming Support**: Efficiently processes data chunk-by-chunk
- **Range-based Operations**: Download and repair specific chunks without the entire dataset

## Technical Concept

Alpha Entanglement works by:

1. **Chunking**: Dividing input data into fixed-size chunks (1024 bytes by default)
2. **Grid Arrangement**: Organizing chunks in a 2D grid layout
3. **Strand Generation**: Creating up to three types of strands (Left, Horizontal, Right) that define XOR relationships
4. **Parity Creation**: Generating alpha parity chunks for each data chunk using these strand relationships
5. **Metadata Storage**: Preserving configuration and relationship information for recovery

## Installation

Add the library to your project:

```toml
[dependencies]
recall_entangler = { git = "https://github.com/recallnet/entanglement.git" }
recall_entangler_storage = { package = "recall_entangler_storage", git = "https://github.com/recallnet/entanglement.git" }
```

## Quick Start

```rust
use recall_entangler::{Config, Entangler};
use recall_entangler_storage::iroh::IrohStorage;
use tokio::fs::File;
use tokio_util::io::ReaderStream;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Create storage backend
    let storage = IrohStorage::new_in_memory().await?;
    
    // Initialize entangler with configuration
    let entangler = Entangler::new(storage, Config::new(3, 3))?;
    
    // Open file and create a stream
    let file = File::open("path/to/your/file.txt").await?;
    let input_stream = ReaderStream::new(file);
    
    // Upload and entangle data
    let result = entangler.upload(input_stream).await?;
    
    println!("Original hash: {}", result.orig_hash);
    println!("Metadata hash: {}", result.metadata_hash);
    
    // Download data (auto-repairs if chunks are corrupted/missing)
    let stream = entangler.download(&result.orig_hash, Some(&result.metadata_hash)).await?;
    
    // Process the stream...
    
    Ok(())
}
```

## CLI Usage

The package includes a command-line tool for file operations:

```
# Upload a file
entanglement upload --file path/to/myfile.txt --iroh-path /path/to/iroh/storage

# Download a file
entanglement download --hash <blob-hash> --metadata-hash <metadata-hash> --output path/to/output.txt --iroh-path /path/to/iroh/storage
```

## Advanced Usage

### Custom Storage Backends

You can implement the `Storage` trait for any storage system:

```rust
#[async_trait]
impl Storage for MyCustomStorage {
    type ChunkId = String;
    type ChunkIdMapper = MyChunkIdMapper;

    async fn upload_bytes(
        &self,
        bytes: impl Into<Bytes> + Send,
    ) -> Result<storage::UploadResult, StorageError> {
        // Implementation for uploading bytes
    }

    // Implement other required methods...
}
```

### Range-based Downloads

Download specific byte ranges or chunks:

```rust
// Download a range of bytes
let stream = entangler.download_range(&hash, range, Some(&metadata_hash)).await?;

// Download specific chunks
let stream = entangler.download_chunks(&hash, chunk_ids, Some(&metadata_hash)).await?;
```

## Configuration Parameters

The three key parameters allow fine-tuning the redundancy and recoverability:

- **alpha (α)**: Influences the total storage overhead. Higher values create more parity chunks, providing greater redundancy but requiring more storage space.
- **s**: Determines the grid height (number of rows). Affects the "spread" of relationships between chunks.

A typical starting configuration is α=3, s=5 which creates a balanced trade-off between redundancy and storage overhead.

## Performance

Alpha Entanglement provides remarkable resilience across various failure scenarios:

- **Single Chunk Loss**: Can repair any isolated missing chunk
- **Column/Row Loss**: Recovers from entire columns or rows of missing chunks
- **Island Patterns**: Handles large contiguous regions of lost chunks
- **Extreme Cases**: In some configurations, can reconstruct the entire dataset from a small fraction of available chunks

## Storage Backends

Currently implemented backends:

- **IROH**: Integrates with the IROH P2P storage system
- **Mock**: In-memory implementation for testing

## Building & Development

```bash
# Build
cargo build --release

# Run tests
cargo test --locked --workspace

# Generate docs
cargo doc --locked --no-deps --workspace --open
```

## Papers
- [Alpha Entanglement Codes: Practical Erasure Codes to Archive Data in Unreliable Environments](https://ieeexplore.ieee.org/document/8416482)
- [Snarl: entangled merkle trees for improved file availability and storage utilization](https://dl.acm.org/doi/abs/10.1145/3464298.3493397)

## License

Licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
- MIT license ([LICENSE](LICENSE) or http://opensource.org/licenses/MIT)

at your option.

## Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in the work by you, as defined in the Apache-2.0 license, shall be dual licensed as above, without any additional terms or conditions.