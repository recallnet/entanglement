// Copyright 2024 Entanglement Contributors
// SPDX-License-Identifier: Apache-2.0, MIT

use anyhow::Result;
use bytes::{BufMut, Bytes, BytesMut};
use futures::StreamExt;
use recall_entangler::{self, parity::StrandType, ChunkRange, Config, Entangler, Metadata};
use recall_entangler_storage::{
    self, iroh::IrohStorage, mock::FakeStorage, ByteStream, ChunkIdMapper, Error as StorageError,
    Storage,
};
use std::collections::HashSet;
use std::str::FromStr;

const HEIGHT: u64 = 5;
// we choose WIDTH to be multiple of HEIGHT to avoid complex strand wrapping calculations
const WIDTH: u64 = 10;
const NUM_CHUNKS: u64 = HEIGHT * WIDTH;
const CHUNK_SIZE: u64 = 1024;

// create Bytes of n 1024-sized chunks
fn create_bytes(n: u64) -> Bytes {
    let mut bytes = BytesMut::with_capacity((n * CHUNK_SIZE) as usize);
    for i in 0..n {
        let mut val = i as u8;
        for _ in 0..CHUNK_SIZE {
            bytes.put_u8(val);
            val = val.wrapping_add(1)
        }
    }
    bytes.freeze()
}

fn xor_chunks(chunk1: &[u8], chunk2: &[u8]) -> Bytes {
    let mut res = BytesMut::with_capacity(CHUNK_SIZE as usize);
    for i in 0..CHUNK_SIZE as usize {
        res.put_u8(chunk1[i] ^ chunk2[i]);
    }
    res.freeze()
}

fn new_entangler_from_node<S: iroh::blobs::store::Store>(
    node: &iroh::node::Node<S>,
) -> Result<Entangler<IrohStorage>, recall_entangler::Error> {
    let st = IrohStorage::from_client(node.client().clone());
    Entangler::new(st, Config::new(3, HEIGHT as u8, HEIGHT as u8))
}

async fn load_parity_data_to_node<S>(
    target_node: &iroh::node::Node<S>,
    source_node: &iroh::node::Node<S>,
    metadata_hash: &str,
) -> Result<()> {
    let metadata_hash = iroh::blobs::Hash::from_str(metadata_hash)?;

    let node_addr = source_node.net().node_addr().await?;
    target_node
        .blobs()
        .download(metadata_hash, node_addr.clone())
        .await?
        .await?;

    let metadata_bytes = target_node.blobs().read_to_bytes(metadata_hash).await?;
    let metadata: Metadata = serde_json::from_slice(&metadata_bytes)?;
    for parity_hash_str in metadata.parity_hashes.values() {
        let parity_hash = iroh::blobs::Hash::from_str(parity_hash_str)?;
        target_node
            .blobs()
            .download(parity_hash, node_addr.clone())
            .await?
            .await?;
    }
    Ok(())
}

// Use read_stream from entangler module instead
use recall_entangler::read_stream;

// Helper function to convert bytes to a stream for tests
fn bytes_to_stream<T>(bytes: T) -> storage::ByteStream
where
    T: Into<Bytes> + Send + 'static,
{
    Box::pin(futures::stream::once(async move {
        Ok::<Bytes, storage::Error>(bytes.into())
    }))
}

// Helper function to create a byte stream from bytes
fn create_byte_stream(bytes: Bytes) -> ByteStream {
    Box::pin(futures::stream::once(async move {
        Ok::<Bytes, StorageError>(bytes)
    }))
}

async fn load_metadata(hash: &str, storage: &impl Storage) -> Result<Metadata> {
    let metadata_bytes = storage.download_bytes(hash).await?;
    Ok(serde_json::from_slice(&read_stream(metadata_bytes).await?)?)
}

#[tokio::test]
async fn test_upload_bytes_to_iroh() -> Result<()> {
    let node = iroh::node::Node::memory().spawn().await?;
    let ent = new_entangler_from_node(&node)?;

    let bytes = create_bytes(NUM_CHUNKS);
    // Convert bytes to a stream for upload
    let byte_stream = create_byte_stream(bytes.clone());
    let result = ent.upload(byte_stream).await?;

    let data_hash = iroh::blobs::Hash::from_str(&result.orig_hash)?;
    let res = node.blobs().read_to_bytes(data_hash).await?;
    assert_eq!(res, bytes, "original data mismatch");

    let metadata_hash = iroh::blobs::Hash::from_str(&result.metadata_hash)?;
    let metadata_bytes = node.blobs().read_to_bytes(metadata_hash).await?;

    let metadata: Metadata = serde_json::from_slice(&metadata_bytes)?;
    assert_eq!(
        metadata.orig_hash, result.orig_hash,
        "metadata orig_hash mismatch"
    );
    assert_eq!(
        metadata.num_bytes,
        bytes.len() as u64,
        "metadata num_bytes mismatch"
    );
    assert_eq!(
        metadata.chunk_size, CHUNK_SIZE,
        "metadata chunk_size mismatch"
    );
    assert_eq!(metadata.s, HEIGHT as u8, "metadata s mismatch");
    assert_eq!(metadata.p, HEIGHT as u8, "metadata p mismatch");

    // Verify we have the expected number of upload results
    assert_eq!(result.upload_results.len(), 1 + 3 + 1); // original blob + 3 parity blobs + metadata

    for (strand_type, parity_hash) in &metadata.parity_hashes {
        let parity_hash = iroh::blobs::Hash::from_str(parity_hash)?;
        let parity = node.blobs().read_to_bytes(parity_hash).await?;
        let mut expected_parity =
            BytesMut::with_capacity(NUM_CHUNKS as usize * CHUNK_SIZE as usize);
        for i in 0..NUM_CHUNKS as usize {
            let chunk1 = &bytes[i * CHUNK_SIZE as usize..(i + 1) * CHUNK_SIZE as usize];
            let x = (i as u64 / HEIGHT + 1) % WIDTH;
            let y = ((i as u64 + HEIGHT) as i64 + strand_type.to_i64()) as u64 % HEIGHT;
            let i2 = (x * HEIGHT + y) as usize;
            let chunk2 = &bytes[i2 * CHUNK_SIZE as usize..(i2 + 1) * CHUNK_SIZE as usize];
            let expected_chunk = xor_chunks(chunk1, chunk2);
            expected_parity.extend_from_slice(&expected_chunk);
            let actual_chunk = &parity[i * CHUNK_SIZE as usize..(i + 1) * CHUNK_SIZE as usize];
            assert_eq!(
                actual_chunk, expected_chunk,
                "parity mismatch at chunk {} for strand {:?}",
                i, strand_type
            );
        }
        assert_eq!(parity, expected_parity);
    }
    Ok(())
}

#[tokio::test]
async fn test_download_bytes_from_iroh() -> Result<()> {
    let node = iroh::node::Node::memory().spawn().await?;
    let ent = new_entangler_from_node(&node)?;

    let bytes = create_bytes(NUM_CHUNKS);
    // Convert bytes to a stream for upload
    let byte_stream = create_byte_stream(bytes.clone());
    let result = ent.upload(byte_stream).await?;

    let stream = ent.download(&result.orig_hash, None).await?;
    let downloaded_bytes = read_stream(stream).await?;
    assert_eq!(downloaded_bytes, bytes, "downloaded data mismatch");
    Ok(())
}

#[tokio::test]
async fn if_blob_is_missing_and_no_provided_metadata_error() -> Result<()> {
    let node = iroh::node::Node::memory().spawn().await?;
    let ent = new_entangler_from_node(&node)?;

    let bytes = create_bytes(NUM_CHUNKS);
    let byte_stream = create_byte_stream(bytes.clone());
    let result = ent.upload(byte_stream).await?;

    let node_with_metadata = iroh::node::Node::memory().spawn().await?;
    load_parity_data_to_node(&node_with_metadata, &node, &result.metadata_hash).await?;

    let ent_with_metadata = new_entangler_from_node(&node_with_metadata)?;

    let download_result = ent_with_metadata.download(&result.orig_hash, None).await;
    assert!(download_result.is_err(), "expected download to fail");
    Ok(())
}

#[tokio::test]
async fn if_blob_is_missing_and_metadata_is_provided_error() -> Result<()> {
    let node = iroh::node::Node::memory().spawn().await?;
    let ent = new_entangler_from_node(&node)?;

    let bytes = create_bytes(NUM_CHUNKS);
    let byte_stream = create_byte_stream(bytes.clone());
    let result = ent.upload(byte_stream).await?;

    let node_with_metadata = iroh::node::Node::memory().spawn().await?;
    load_parity_data_to_node(&node_with_metadata, &node, &result.metadata_hash).await?;

    let ent_with_metadata = new_entangler_from_node(&node_with_metadata)?;

    let download_result = ent_with_metadata
        .download(&result.orig_hash, Some(&result.metadata_hash))
        .await;
    assert!(download_result.is_err(), "expected download to fail");
    Ok(())
}

#[tokio::test]
async fn if_chunk_is_missing_and_metadata_is_provided_should_repair() -> Result<()> {
    let mock_storage = FakeStorage::new();
    let ent = Entangler::new(
        mock_storage.clone(),
        Config::new(3, HEIGHT as u8, HEIGHT as u8),
    )?;

    let bytes = create_bytes(NUM_CHUNKS);
    let byte_stream = create_byte_stream(bytes.clone());
    let result = ent.upload(byte_stream).await?;

    mock_storage.fake_failed_download(&result.orig_hash);
    mock_storage.fake_failed_chunks(&result.orig_hash, vec![2]);

    let stream = ent
        .download(&result.orig_hash, Some(&result.metadata_hash))
        .await?;
    let downloaded_bytes = read_stream(stream).await?;
    assert_eq!(downloaded_bytes, bytes, "downloaded data mismatch");

    Ok(())
}

#[tokio::test]
async fn if_stream_fails_and_metadata_is_not_provided_should_error() -> Result<()> {
    let mock_storage = FakeStorage::new();
    let ent = Entangler::new(
        mock_storage.clone(),
        Config::new(3, HEIGHT as u8, HEIGHT as u8),
    )?;

    let bytes = create_bytes(2); // Creates 2048 bytes (2 chunks)
    let byte_stream = create_byte_stream(bytes.clone());
    let result = ent.upload(byte_stream).await?;

    // this simulates a failure during stream download after 1034 bytes
    mock_storage.fake_failed_stream(&result.orig_hash, (CHUNK_SIZE + 10) as usize);

    let mut stream = ent.download(&result.orig_hash, None).await?;
    let mut downloaded = Vec::new();
    let mut stream_failed = false;

    while let Some(chunk) = stream.next().await {
        match chunk {
            Ok(data) => downloaded.extend_from_slice(&data),
            Err(_) => {
                stream_failed = true;
                break;
            }
        }
    }

    assert!(stream_failed, "Stream should fail at 1034 bytes");
    assert_eq!(
        downloaded.len(),
        1034,
        "Should receive 1034 bytes before failure"
    );
    assert_eq!(
        &bytes[..1034],
        &downloaded[..],
        "Data before failure should match"
    );

    Ok(())
}

#[tokio::test]
async fn if_stream_fails_should_repair_and_continue_where_left_off() -> Result<()> {
    struct TestCase {
        name: &'static str,
        num_chunks: u64,
        fail_at: usize,
    }

    let test_cases = vec![
        TestCase {
            name: "fail in middle of second chunk",
            num_chunks: 2,
            fail_at: CHUNK_SIZE as usize + 10,
        },
        TestCase {
            name: "fail at chunk boundary",
            num_chunks: 3,
            fail_at: CHUNK_SIZE as usize,
        },
        TestCase {
            name: "fail near end of file",
            num_chunks: 2,
            fail_at: (2 * CHUNK_SIZE - 1) as usize,
        },
        TestCase {
            name: "fail near start of second chunk",
            num_chunks: 3,
            fail_at: CHUNK_SIZE as usize + 1,
        },
        TestCase {
            name: "fail at position 0",
            num_chunks: 3,
            fail_at: 0,
        },
        TestCase {
            name: "fail at the end",
            num_chunks: 3,
            fail_at: (3 * CHUNK_SIZE) as usize,
        },
    ];

    for case in test_cases {
        println!("Running test case: {}", case.name);
        let mock_storage = FakeStorage::new();
        let ent = Entangler::new(
            mock_storage.clone(),
            Config::new(3, HEIGHT as u8, HEIGHT as u8),
        )?;

        let bytes = create_bytes(case.num_chunks);
        let byte_stream = create_byte_stream(bytes.clone());
        let result = ent.upload(byte_stream).await?;

        // this simulates a failure during stream download
        mock_storage.fake_failed_stream(&result.orig_hash, case.fail_at);

        // provide metadata to repair the download
        let mut stream = ent
            .download(&result.orig_hash, Some(&result.metadata_hash))
            .await?;
        let mut downloaded = Vec::with_capacity(case.fail_at);
        let mut stream_failed = false;

        while let Some(chunk) = stream.next().await {
            match chunk {
                Ok(data) => {
                    downloaded.extend_from_slice(&data);
                }
                Err(_) => {
                    stream_failed = true;
                    break;
                }
            }
        }
        println!(
            "test: Stream completed, received {} bytes total",
            downloaded.len()
        );

        assert!(!stream_failed, "Stream should not fail");
        assert_eq!(downloaded, bytes, "downloaded data mismatch");
    }

    Ok(())
}

fn make_parity_unavailable(st: &FakeStorage, metadata: &Metadata, strand: StrandType) {
    let hash = &metadata.parity_hashes[&strand];
    st.fake_failed_download(hash);
    st.fake_failed_chunks(hash, (0..NUM_CHUNKS).collect());
}

//  0  5  . 15 20 25
//  1  .  .  . 21 26
//  .  .  .  .  . 27
//  3  .  .  . 23 28
//  4  9  . 19 24 29
fn get_chunk_island() -> Vec<u64> {
    vec![2, 6, 7, 8, 10, 11, 12, 13, 14, 16, 17, 18, 22]
}

//  0  5  . 15 20 25  .  .  .  .
//  1  .  .  . 21 26  .  .  . 46
//  .  .  .  .  . 27  .  . 42 47
//  3  .  .  . 23 28 33 38 43  .
//  4  9  . 19 24 29 34 39 44  .
fn get_several_chunk_islands() -> Vec<u64> {
    vec![
        2, 6, 7, 8, 10, 11, 12, 13, 14, 16, 17, 18, 22, 30, 31, 32, 35, 36, 37, 40, 41, 45, 48, 49,
    ]
}

fn get_all_chunks_but_strand_revolution(strand: StrandType) -> Vec<u64> {
    let excluded: HashSet<_> = match strand {
        //           20
        //        16
        //     12
        //   8
        // 4
        StrandType::Left => (0..HEIGHT).map(|x| (HEIGHT - 1) * (x + 1)).collect(),
        //
        //
        // 2 7 12 17 22 27 32 37 42 47
        //
        //
        StrandType::Horizontal => (0..WIDTH).map(|x| x * HEIGHT + 2).collect(),
        // 0
        //   6
        //     12
        //        18
        //           24
        StrandType::Right => (0..HEIGHT).map(|x| (HEIGHT + 1) * x).collect(),
    };

    (0..NUM_CHUNKS).filter(|x| !excluded.contains(x)).collect()
}

#[tokio::test]
async fn test_download_blob_and_repair_scenarios() -> Result<()> {
    struct TestCase {
        name: &'static str,
        setup: fn(&FakeStorage, &Metadata),
        should_succeed: bool,
    }

    let test_cases = vec![
        TestCase {
            name: "1 chunk is missing and only L strand is available",
            setup: |st, metadata| {
                st.fake_failed_chunks(&metadata.orig_hash, vec![2]);

                make_parity_unavailable(st, metadata, StrandType::Right);
                make_parity_unavailable(st, metadata, StrandType::Horizontal);
            },
            should_succeed: true,
        },
        TestCase {
            name: "1 chunk is missing and only R strand is available",
            setup: |st, metadata| {
                st.fake_failed_chunks(&metadata.orig_hash, vec![2]);

                make_parity_unavailable(st, metadata, StrandType::Left);
                make_parity_unavailable(st, metadata, StrandType::Horizontal);
            },
            should_succeed: true,
        },
        TestCase {
            name: "1 chunk is missing and only H strand is available",
            setup: |st, metadata| {
                st.fake_failed_chunks(&metadata.orig_hash, vec![2]);

                make_parity_unavailable(st, metadata, StrandType::Left);
                make_parity_unavailable(st, metadata, StrandType::Right);
            },
            should_succeed: true,
        },
        TestCase {
            name: "several disjoint chunks are missing",
            setup: |st, metadata| {
                st.fake_failed_chunks(&metadata.orig_hash, vec![1, 3, 10, 13, 21, 23]);

                make_parity_unavailable(st, metadata, StrandType::Left);
                make_parity_unavailable(st, metadata, StrandType::Horizontal);
            },
            should_succeed: true,
        },
        TestCase {
            name: "no parity is available, should fail",
            setup: |st, metadata| {
                st.fake_failed_chunks(&metadata.orig_hash, vec![2]);

                make_parity_unavailable(st, metadata, StrandType::Left);
                make_parity_unavailable(st, metadata, StrandType::Horizontal);
                make_parity_unavailable(st, metadata, StrandType::Right);
            },
            should_succeed: false,
        },
        TestCase {
            name: "an island of missing chunks (only left parity available)",
            setup: |st, metadata| {
                st.fake_failed_chunks(&metadata.orig_hash, get_chunk_island());

                make_parity_unavailable(st, metadata, StrandType::Horizontal);
                make_parity_unavailable(st, metadata, StrandType::Right);
            },
            should_succeed: true,
        },
        TestCase {
            name: "an island of missing chunks (only horizontal parity available)",
            setup: |st, metadata| {
                st.fake_failed_chunks(&metadata.orig_hash, get_chunk_island());

                make_parity_unavailable(st, metadata, StrandType::Left);
                make_parity_unavailable(st, metadata, StrandType::Right);
            },
            should_succeed: true,
        },
        TestCase {
            name: "an island of missing chunks (only right parity available)",
            setup: |st, metadata| {
                st.fake_failed_chunks(&metadata.orig_hash, get_chunk_island());

                make_parity_unavailable(st, metadata, StrandType::Left);
                make_parity_unavailable(st, metadata, StrandType::Horizontal);
            },
            should_succeed: true,
        },
        TestCase {
            name: "several islands of missing chunks (only left parity available)",
            setup: |st, metadata| {
                st.fake_failed_chunks(&metadata.orig_hash, get_several_chunk_islands());

                make_parity_unavailable(st, metadata, StrandType::Horizontal);
                make_parity_unavailable(st, metadata, StrandType::Right);
            },
            should_succeed: true,
        },
        TestCase {
            name: "several islands of missing chunks (only horizontal parity available)",
            setup: |st, metadata| {
                st.fake_failed_chunks(&metadata.orig_hash, get_several_chunk_islands());

                make_parity_unavailable(st, metadata, StrandType::Left);
                make_parity_unavailable(st, metadata, StrandType::Right);
            },
            should_succeed: true,
        },
        TestCase {
            name: "several islands of missing chunks (only right parity available)",
            setup: |st, metadata| {
                st.fake_failed_chunks(&metadata.orig_hash, get_several_chunk_islands());

                make_parity_unavailable(st, metadata, StrandType::Left);
                make_parity_unavailable(st, metadata, StrandType::Horizontal);
            },
            should_succeed: true,
        },
        TestCase {
            name: "only 1 chunk at (2, 2) is available and left parity is unavailable",
            setup: |st, metadata| {
                st.fake_failed_chunks(&metadata.orig_hash, (0..12).chain(13..CHUNK_SIZE).collect());

                make_parity_unavailable(st, metadata, StrandType::Left);
            },
            should_succeed: true,
        },
        TestCase {
            name: "only 1 chunk at (3, 2) is available and left parity is unavailable",
            setup: |st, metadata| {
                st.fake_failed_chunks(&metadata.orig_hash, (0..17).chain(18..CHUNK_SIZE).collect());

                make_parity_unavailable(st, metadata, StrandType::Left);
            },
            should_succeed: true,
        },
        TestCase {
            name: "only 1 chunk at (4, 1) is available and left parity is unavailable",
            setup: |st, metadata| {
                st.fake_failed_chunks(&metadata.orig_hash, (0..21).chain(22..CHUNK_SIZE).collect());

                make_parity_unavailable(st, metadata, StrandType::Left);
            },
            should_succeed: true,
        },
        TestCase {
            name: "only 1 chunk at (2, 2) is available and right parity is unavailable",
            setup: |st, metadata| {
                st.fake_failed_chunks(&metadata.orig_hash, (0..12).chain(13..CHUNK_SIZE).collect());

                make_parity_unavailable(st, metadata, StrandType::Right);
            },
            should_succeed: true,
        },
        TestCase {
            name: "only 1 chunk at (3, 2) is available and right parity is unavailable",
            setup: |st, metadata| {
                st.fake_failed_chunks(&metadata.orig_hash, (0..17).chain(18..CHUNK_SIZE).collect());

                make_parity_unavailable(st, metadata, StrandType::Right);
            },
            should_succeed: true,
        },
        TestCase {
            name: "only 1 chunk at (4, 1) is available and right parity is unavailable",
            setup: |st, metadata| {
                st.fake_failed_chunks(&metadata.orig_hash, (0..21).chain(22..CHUNK_SIZE).collect());

                make_parity_unavailable(st, metadata, StrandType::Right);
            },
            should_succeed: true,
        },
        TestCase {
            name: "only 1 chunk at (2, 2) is available and horizontal parity is unavailable",
            setup: |st, metadata| {
                st.fake_failed_chunks(&metadata.orig_hash, (0..12).chain(13..CHUNK_SIZE).collect());

                make_parity_unavailable(st, metadata, StrandType::Horizontal);
            },
            should_succeed: true,
        },
        TestCase {
            name: "only 1 chunk at (3, 2) is available and horizontal parity is unavailable",
            setup: |st, metadata| {
                st.fake_failed_chunks(&metadata.orig_hash, (0..17).chain(18..CHUNK_SIZE).collect());

                make_parity_unavailable(st, metadata, StrandType::Horizontal);
            },
            should_succeed: true,
        },
        TestCase {
            name: "only 1 chunk at (4, 1) is available and horizontal parity is unavailable",
            setup: |st, metadata| {
                st.fake_failed_chunks(&metadata.orig_hash, (0..21).chain(22..CHUNK_SIZE).collect());

                make_parity_unavailable(st, metadata, StrandType::Horizontal);
            },
            should_succeed: true,
        },
        TestCase {
            name: "only 1 chunk at (4, 1) is available, left parity and 1 strand revolution on horizontal parity",
            setup: |st, metadata| {
                st.fake_failed_chunks(&metadata.orig_hash, (0..21).chain(22..CHUNK_SIZE).collect());

                make_parity_unavailable(st, metadata, StrandType::Right);

                let h_parity_hash = &metadata.parity_hashes[&StrandType::Horizontal];
                st.fake_failed_download(h_parity_hash);
                st.fake_failed_chunks(h_parity_hash, get_all_chunks_but_strand_revolution(StrandType::Horizontal));
            },
            should_succeed: true,
        },
        TestCase {
            name: "only 1 chunk at (4, 1) is available, horizontal parity and 1 strand revolution on right parity",
            setup: |st, metadata| {
                st.fake_failed_chunks(&metadata.orig_hash, (0..21).chain(22..CHUNK_SIZE).collect());

                make_parity_unavailable(st, metadata, StrandType::Left);

                let r_parity_hash = &metadata.parity_hashes[&StrandType::Right];
                st.fake_failed_download(r_parity_hash);
                st.fake_failed_chunks(r_parity_hash, get_all_chunks_but_strand_revolution(StrandType::Right));
            },
            should_succeed: true,
        },
        TestCase {
            name: "only 1 chunk at (4, 1) is available, right parity and 1 strand revolution on left parity",
            setup: |st, metadata| {
                st.fake_failed_chunks(&metadata.orig_hash, (0..21).chain(22..CHUNK_SIZE).collect());

                make_parity_unavailable(st, metadata, StrandType::Horizontal);

                let l_parity_hash = &metadata.parity_hashes[&StrandType::Left];
                st.fake_failed_download(l_parity_hash);
                st.fake_failed_chunks(l_parity_hash, get_all_chunks_but_strand_revolution(StrandType::Left));
            },
            should_succeed: true,
        },
    ];

    for case in test_cases {
        for upload_method in ["upload", "entangle_uploaded"] {
            println!(
                "Running test case: {}. Upload method: {}",
                case.name, upload_method
            );

            let mock_storage = FakeStorage::new();
            let ent = Entangler::new(
                mock_storage.clone(),
                Config::new(3, HEIGHT as u8, HEIGHT as u8),
            )?;

            let bytes = create_bytes(NUM_CHUNKS);

            let result = if upload_method == "upload" {
                let upload_result = mock_storage
                    .upload_bytes(bytes_to_stream(bytes.clone()))
                    .await?;
                ent.entangle_uploaded(upload_result.hash.clone()).await?
            } else {
                let byte_stream = create_byte_stream(bytes.clone());
                ent.upload(byte_stream).await?
            };

            let metadata = load_metadata(&result.metadata_hash, &mock_storage).await?;

            mock_storage.fake_failed_download(&result.orig_hash);

            (case.setup)(&mock_storage, &metadata);

            let stream = ent
                .download(&result.orig_hash, Some(&result.metadata_hash))
                .await;

            if case.should_succeed {
                assert!(
                    stream.is_ok(),
                    "expected download to succeed for case: {}",
                    case.name
                );
                let downloaded_bytes = read_stream(stream.unwrap()).await?;
                assert_eq!(
                    downloaded_bytes, bytes,
                    "downloaded data mismatch for case: {}",
                    case.name
                );
            } else {
                assert!(
                    stream.is_err(),
                    "expected download to fail for case: {}",
                    case.name
                );
            }
        }
    }

    Ok(())
}

#[tokio::test]
async fn test_download_chunks_range_and_repair_scenarios() -> Result<()> {
    struct Req {
        name: &'static str,
        range: ChunkRange,
    }
    struct TestCase {
        name: &'static str,
        setup: fn(&FakeStorage, &Metadata),
        requests: Vec<Req>,
    }

    let test_cases = vec![
        TestCase {
            name: "1 chunk is missing and only L strand is available",
            setup: |st, metadata| {
                st.fake_failed_chunks(&metadata.orig_hash, vec![12]);

                make_parity_unavailable(st, metadata, StrandType::Right);
                make_parity_unavailable(st, metadata, StrandType::Horizontal);
            },
            requests: vec![
                Req {
                    name: "starting before and ending after the missing chunk",
                    range: ChunkRange::Between(11, 13),
                },
                Req {
                    name: "starting and ending at the missing chunk",
                    range: ChunkRange::Between(12, 12),
                },
                Req {
                    name: "starting at the missing chunk and ending after",
                    range: ChunkRange::Between(12, 14),
                },
                Req {
                    name: "starting before the missing chunk and ending at it",
                    range: ChunkRange::Between(11, 12),
                },
                Req {
                    name: "between range outside the missing chunk",
                    range: ChunkRange::Between(20, 40),
                },
                Req {
                    name: "range till after the missing chunk",
                    range: ChunkRange::Till(13),
                },
                Req {
                    name: "range till before the missing chunk",
                    range: ChunkRange::Till(10),
                },
                Req {
                    name: "range from before the missing chunk",
                    range: ChunkRange::From(11),
                },
                Req {
                    name: "range from after the missing chunk",
                    range: ChunkRange::From(13),
                },
            ],
        },
        TestCase {
            name: "a chunk column is missing and only horizontal strand is available",
            setup: |st, metadata| {
                st.fake_failed_chunks(&metadata.orig_hash, (10..10 + HEIGHT).collect());

                make_parity_unavailable(st, metadata, StrandType::Left);
                make_parity_unavailable(st, metadata, StrandType::Right);
            },
            requests: vec![
                Req {
                    name: "range within column",
                    range: ChunkRange::Between(11, 13),
                },
                Req {
                    name: "range enclosing column",
                    range: ChunkRange::Between(8, 16),
                },
                Req {
                    name: "range till middle of column",
                    range: ChunkRange::Till(12),
                },
                Req {
                    name: "range from middle of column",
                    range: ChunkRange::From(12),
                },
            ],
        },
        TestCase {
            name: "a horizontal stripe of chunks is missing and only R strand is available",
            setup: |st, metadata| {
                st.fake_failed_chunks(
                    &metadata.orig_hash,
                    vec![12, 13, 17, 18, 22, 23, 27, 28, 32, 33],
                );

                make_parity_unavailable(st, metadata, StrandType::Left);
                make_parity_unavailable(st, metadata, StrandType::Horizontal);
            },
            requests: vec![
                Req {
                    name: "range starting and ending on the missing stripe",
                    range: ChunkRange::Between(17, 28),
                },
                Req {
                    name: "range enclosing column",
                    range: ChunkRange::Between(10, 35),
                },
                Req {
                    name: "range till middle of the stripe",
                    range: ChunkRange::Till(23),
                },
                Req {
                    name: "range from middle of column",
                    range: ChunkRange::From(22),
                },
            ],
        },
    ];

    for case in test_cases {
        for req in &case.requests {
            for download_method in ["range", "chunks"] {
                println!(
                    "Running test case: {}, {}. Download method: {}",
                    case.name, req.name, download_method
                );

                let mock_storage = FakeStorage::new();
                let ent = Entangler::new(
                    mock_storage.clone(),
                    Config::new(3, HEIGHT as u8, HEIGHT as u8),
                )?;

                let bytes = create_bytes(NUM_CHUNKS);
                let byte_stream = create_byte_stream(bytes.clone());
                let result = ent.upload(byte_stream).await?;
                let metadata = load_metadata(&result.metadata_hash, &mock_storage).await?;

                mock_storage.fake_failed_download(&metadata.orig_hash);

                (case.setup)(&mock_storage, &metadata);

                let (first, last) = match req.range {
                    ChunkRange::From(first) => (first, NUM_CHUNKS - 1),
                    ChunkRange::Till(last) => (0, last),
                    ChunkRange::Between(first, last) => (first, last),
                };

                if download_method == "range" {
                    let stream = ent
                        .download_range(&result.orig_hash, req.range, Some(result.metadata_hash))
                        .await;
                    assert!(
                        stream.is_ok(),
                        "expected download_range to return stream for case: {}, {}, but got: {:?}",
                        case.name,
                        req.name,
                        stream.err().unwrap(),
                    );
                    let stream = stream?;
                    let downloaded_bytes = read_stream(stream).await;
                    assert!(
                        downloaded_bytes.is_ok(),
                        "expected download_range to succeed for case: {}, {}, but got: {:?}",
                        case.name,
                        req.name,
                        downloaded_bytes.err().unwrap(),
                    );
                    let expected_bytes = bytes
                        .slice((first * CHUNK_SIZE) as usize..((last + 1) * CHUNK_SIZE) as usize);
                    assert_eq!(
                        downloaded_bytes.unwrap(),
                        expected_bytes,
                        "downloaded data mismatch for case: {}",
                        case.name
                    );
                } else {
                    let mapper = mock_storage.chunk_id_mapper(&result.orig_hash).await?;
                    let chunks = (first..=last)
                        .map(|i| mapper.index_to_id(i).unwrap())
                        .collect();
                    let download_result = ent.download_chunks(
                        result.orig_hash.clone(),
                        chunks,
                        Some(result.metadata_hash),
                    );
                    assert!(
                        download_result.is_ok(),
                        "expected download_chunks to succeed for case: {}, {}",
                        case.name,
                        req.name,
                    );
                    let downloaded_chunks: Vec<_> = download_result?.collect().await;
                    let downloaded_chunks: Vec<_> = downloaded_chunks.into_iter().collect();
                    for (id, chunk) in downloaded_chunks {
                        let index = mapper.id_to_index(&id).unwrap();
                        if index >= first && index <= last {
                            assert_eq!(
                                chunk?.clone(),
                                &bytes[(index * CHUNK_SIZE) as usize
                                    ..((index + 1) * CHUNK_SIZE) as usize],
                                "downloaded chunk mismatch for case: {}, {}",
                                case.name,
                                req.name
                            );
                        }
                    }
                }
            }
        }
    }

    Ok(())
}

#[tokio::test]
async fn if_download_fails_it_should_upload_to_storage_after_repair() -> Result<()> {
    enum Method {
        Download,
        Range,
        Chunks,
    }

    impl std::fmt::Display for Method {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                Method::Download => write!(f, "download blob"),
                Method::Range => write!(f, "download range"),
                Method::Chunks => write!(f, "download chunks"),
            }
        }
    }

    struct TestCase {
        method: Method,
        always_repair: bool,
        expect_upload: bool,
    }

    let test_cases = vec![
        TestCase {
            method: Method::Download,
            always_repair: true,
            expect_upload: true,
        },
        TestCase {
            method: Method::Download,
            always_repair: false,
            expect_upload: true,
        },
        TestCase {
            method: Method::Range,
            always_repair: true,
            expect_upload: true,
        },
        TestCase {
            method: Method::Range,
            always_repair: false,
            expect_upload: false,
        },
        TestCase {
            method: Method::Chunks,
            always_repair: true,
            expect_upload: true,
        },
        TestCase {
            method: Method::Chunks,
            always_repair: false,
            expect_upload: false,
        },
    ];

    for t in test_cases {
        println!(
            "Running test case. Download method: {}. Always repair: {}, Expect upload: {}",
            t.method, t.always_repair, t.expect_upload
        );
        let bytes = create_bytes(NUM_CHUNKS);

        let storage = FakeStorage::new();
        let upload_result = storage.upload_bytes(bytes_to_stream(bytes.clone())).await?;

        let mut conf = Config::new(3, 3, 3);
        conf.always_repair = t.always_repair;
        let ent = Entangler::new(storage.clone(), conf).unwrap();
        let result = ent.entangle_uploaded(upload_result.hash.clone()).await?;
        let m_hash = result.metadata_hash;

        storage.fake_failed_download(&upload_result.hash);
        storage.fake_failed_chunks(&upload_result.hash, vec![1]);

        let res = storage.download_bytes(&upload_result.hash).await;
        assert!(matches!(res, Err(StorageError::BlobNotFound(_))));

        match t.method {
            Method::Download => {
                let stream = ent.download(&upload_result.hash, Some(&m_hash)).await?;
                let result = read_stream(stream).await;
                assert!(result.is_ok(), "Failed to download blob: {:?}", result);
            }
            Method::Range => {
                let stream = ent
                    .download_range(&upload_result.hash, ChunkRange::From(0), Some(m_hash))
                    .await;
                assert!(
                    stream.is_ok(),
                    "Failed to get range stream: {}",
                    upload_result.hash
                );
                let result = read_stream(stream.unwrap()).await;
                assert!(result.is_ok(), "Failed to download range: {:?}", result);
            }
            Method::Chunks => {
                let ids = vec![0, 1, 2];
                let stream =
                    ent.download_chunks(upload_result.hash.clone(), ids.clone(), Some(m_hash));
                assert!(
                    stream.is_ok(),
                    "Failed to get chunks stream: {}",
                    upload_result.hash
                );
                let mut stream = stream.unwrap();
                let mut index = 0;
                while let Some((id, res)) = stream.next().await {
                    assert!(
                        res.is_ok(),
                        "Failed to download chunk: {}. Err: {}",
                        id,
                        res.err().unwrap()
                    );
                    assert_eq!(
                        ids[index], id,
                        "Unexpected chunk id {} at pos {}",
                        id, index
                    );
                    index += 1;
                }
            }
        }

        let res = storage.download_bytes(&upload_result.hash).await;
        assert_eq!(
            res.is_ok(),
            t.expect_upload,
            "Unexpected upload result for method: {}",
            t.method
        );
    }

    Ok(())
}

#[tokio::test]
async fn test_upload_and_download_small_file() -> Result<()> {
    struct TestCase {
        name: &'static str,
        data: Bytes,
    }

    let test_cases = vec![
        TestCase {
            name: "less than 1024 bytes",
            data: Bytes::from_static(b"small chunk"),
        },
        TestCase {
            name: "1024 bytes",
            data: Bytes::from(vec![b'a'; 1024]),
        },
        TestCase {
            name: "less than 2048 bytes",
            data: Bytes::from(vec![b'b'; 1500]),
        },
        TestCase {
            name: "2048 bytes",
            data: Bytes::from(vec![b'c'; 2048]),
        },
        TestCase {
            name: "more than 2048 bytes",
            data: Bytes::from(vec![b'd'; 2500]),
        },
        TestCase {
            name: "3072 bytes",
            data: Bytes::from(vec![b'e'; 2048]),
        },
    ];

    for case in test_cases {
        for use_metadata in [true, false] {
            println!(
                "Running test case: {}. Using metadata: {}",
                case.name, use_metadata
            );

            let storage = FakeStorage::new();
            let ent = Entangler::new(storage.clone(), Config::new(3, 5, 5))?;

            let byte_stream = create_byte_stream(case.data.clone());
            let result = ent.upload(byte_stream).await?;

            let stream = storage.download_bytes(&result.orig_hash).await?;
            let download_result = read_stream(stream).await;
            assert!(
                download_result.is_ok(),
                "Failed to download blob: {:?}",
                download_result
            );

            let metadata = if use_metadata {
                Some(&result.metadata_hash)
            } else {
                None
            };
            let stream = ent
                .download(&result.orig_hash, metadata.map(|s| s.as_str()))
                .await?;
            let result = read_stream(stream).await;
            assert!(result.is_ok(), "Failed to download blob: {:?}", result);
            let downloaded_bytes = result.unwrap();
            assert_eq!(downloaded_bytes, case.data);
        }
    }

    Ok(())
}

// This test is ignored because repairing of small blobs is not implemented yet
#[tokio::test]
#[ignore]
async fn test_upload_and_repair_small_file() -> Result<()> {
    let storage = FakeStorage::new();
    let ent = Entangler::new(storage.clone(), Config::new(3, 5, 5))?;

    let bytes = Bytes::from_static(b"small chunk");
    let byte_stream = create_byte_stream(bytes.clone());
    let result = ent.upload(byte_stream).await?;

    storage.fake_failed_download(&result.orig_hash);
    storage.fake_failed_chunks(&result.orig_hash, vec![0]);

    let stream = ent
        .download(&result.orig_hash, Some(&result.metadata_hash))
        .await?;
    let download_result = read_stream(stream).await;
    assert!(
        download_result.is_ok(),
        "Failed to download blob: {:?}",
        download_result
    );
    assert_eq!(download_result.unwrap(), bytes);

    let stream = storage.download_bytes(&result.orig_hash).await?;
    let result = read_stream(stream).await;
    assert!(result.is_ok(), "Did not upload repaired blob: {:?}", result);
    assert_eq!(result.unwrap(), bytes);

    Ok(())
}

#[tokio::test]
async fn test_metadata_fields() -> Result<()> {
    let storage = FakeStorage::new();
    let ent = Entangler::new(storage.clone(), Config::new(3, HEIGHT as u8, HEIGHT as u8))?;
    let bytes = create_bytes(NUM_CHUNKS);

    for method in ["upload", "entangle_uploaded"] {
        println!("Running test case: {}", method);
        let result = if method == "upload" {
            let byte_stream = create_byte_stream(bytes.clone());
            ent.upload(byte_stream).await?
        } else {
            let upload_result = storage.upload_bytes(bytes_to_stream(bytes.clone())).await?;
            ent.entangle_uploaded(upload_result.hash.clone()).await?
        };

        let metadata = load_metadata(&result.metadata_hash, &storage).await?;

        assert_eq!(
            metadata.num_bytes,
            bytes.len() as u64,
            "Blob size should match input size"
        );

        assert_eq!(
            result.upload_results.len(),
            if method == "upload" { 5 } else { 4 },
            "Number of upload results should match"
        );

        for upload_result in result.upload_results {
            assert!(upload_result.size > 0, "Upload size should be set");
            assert!(
                upload_result.info.contains_key("tag"),
                "Upload info should contain tag"
            );
            assert_eq!(
                upload_result.info["tag"],
                format!("tag-{}", upload_result.hash),
                "Tag should match hash"
            );
        }
    }

    Ok(())
}
