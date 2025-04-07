use bytes::{Bytes, BytesMut};
use jemalloc_ctl::{epoch, stats};
use jemallocator;
use recall_entangler::{Config, Entangler, CHUNK_SIZE};
use recall_entangler_storage::{mock::StubStorage, UploadResult};
use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::oneshot;
use tokio::time::sleep;

// Use jemalloc as the global allocator
#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

// Helper function to get current memory usage in bytes
fn get_memory_usage() -> u64 {
    epoch::advance().unwrap();

    let allocated = stats::allocated::read().unwrap() as u64;
    let resident = stats::resident::read().unwrap() as u64;

    // Return the larger of allocated or resident memory
    std::cmp::max(allocated, resident)
}

// Helper function to generate test data directly into a buffer
fn generate_test_data(size_mb: usize) -> Bytes {
    let total_size = size_mb * 1024 * 1024;
    let num_chunks = total_size / CHUNK_SIZE as usize;
    let mut buffer = BytesMut::with_capacity(total_size);
    let pattern_size = 256;

    for chunk_index in 0..num_chunks {
        for i in 0..CHUNK_SIZE as usize {
            // Create a varying pattern based on both position within chunk and chunk index
            let byte = ((i % pattern_size) as u8)
                .wrapping_add((chunk_index % 256) as u8)
                .wrapping_mul(7)
                .wrapping_add(13);
            buffer.extend_from_slice(&[byte]);
        }
    }

    buffer.freeze()
}

#[tokio::test]
async fn test_entanglement_memory_consumption() {
    // Create test data - 100MB in total
    let blob_size_mb = 100;
    let test_data = generate_test_data(blob_size_mb);
    let total_size = test_data.len() as u64;

    let mut storage = StubStorage::default();

    let mut upload_results = Vec::new();

    println!(
        "number of chunks: {}",
        test_data.len() / CHUNK_SIZE as usize
    );

    for i in 0..3 {
        upload_results.push(Ok(UploadResult {
            hash: format!("parity_hash_{}", i),
            size: total_size,
            info: HashMap::new(),
        }));
    }

    upload_results.push(Ok(UploadResult {
        hash: "metadata_hash".to_string(),
        size: 1024, // Small metadata size
        info: HashMap::new(),
    }));

    storage.stub_upload_bytes_sequence(upload_results);

    storage.stub_download_bytes(Some("test_hash".to_string()), Ok(test_data));

    storage.stub_get_blob_size(Some("test_hash".to_string()), Ok(total_size));

    let initial_memory = get_memory_usage();

    let entangler = Entangler::new(storage, Config::new(3, 3, 3)).unwrap();

    let (stop_tx, stop_rx) = oneshot::channel();

    // Spawn a task to monitor memory usage with higher frequency
    let memory_monitor = tokio::spawn(async move {
        let mut max_memory = 0;
        let mut stop_rx = stop_rx;
        loop {
            tokio::select! {
                _ = sleep(Duration::from_millis(10)) => {
                    let current = get_memory_usage();
                    max_memory = std::cmp::max(max_memory, current);
                }
                _ = &mut stop_rx => {
                    break;
                }
            }
        }
        max_memory
    });

    entangler
        .entangle_uploaded("test_hash".to_string())
        .await
        .unwrap();

    let _ = stop_tx.send(());

    let peak_memory = memory_monitor.await.unwrap();

    let memory_used = peak_memory - initial_memory;
    // 15% tolerance
    let memory_limit = (blob_size_mb as f64 * 1024.0 * 1024.0 * 1.15) as u64;

    assert!(
        memory_used < memory_limit,
        "Peak memory usage ({} MB) exceeded limit ({} MB)",
        memory_used / (1024 * 1024),
        memory_limit / (1024 * 1024)
    );

    println!("Memory Usage Statistics:");
    println!("Initial Memory: {} MB", initial_memory / (1024 * 1024));
    println!("Peak Memory: {} MB", peak_memory / (1024 * 1024));
    println!("Memory Used: {} MB", memory_used / (1024 * 1024));
}
