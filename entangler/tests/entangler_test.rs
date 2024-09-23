// Copyright 2024 Entanglement Contributors
// SPDX-License-Identifier: Apache-2.0, MIT

use anyhow::Result;
use bytes::{BufMut, Bytes, BytesMut};
use entangler::{self, parity::StrandType, Entangler, Metadata};
use std::str::FromStr;
use storage::{self, mock::FakeStorage, Storage};

const HEIGHT: usize = 5;
// we choose WIDTH to be multiple of HEIGHT to avoid complex strand wrapping calculations
const WIDTH: usize = 10;
const NUM_CHUNKS: usize = HEIGHT * WIDTH;
const CHUNK_SIZE: usize = 1024;

// create Bytes of n 1024-sized chunks
fn create_bytes(n: usize) -> Bytes {
    let mut bytes = BytesMut::with_capacity(n * CHUNK_SIZE);
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
    let mut res = BytesMut::with_capacity(CHUNK_SIZE);
    for i in 0..CHUNK_SIZE {
        res.put_u8(chunk1[i] ^ chunk2[i]);
    }
    res.freeze()
}

fn new_entangler_from_node<S: iroh::blobs::store::Store>(
    node: &iroh::node::Node<S>,
) -> Result<Entangler<storage::iroh::IrohStorage>, entangler::Error> {
    let st = storage::iroh::IrohStorage::from_client(node.client().clone());
    Entangler::new(st, 3, HEIGHT as u8, HEIGHT as u8)
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
    for (_, parity_hash) in &metadata.parity_hashes {
        let parity_hash = iroh::blobs::Hash::from_str(parity_hash)?;
        target_node
            .blobs()
            .download(parity_hash, node_addr.clone())
            .await?
            .await?;
    }
    Ok(())
}

async fn load_metadata(hash: &str, storage: &impl Storage) -> Result<Metadata> {
    let metadata_bytes = storage.download_bytes(hash).await?;
    Ok(serde_json::from_slice(&metadata_bytes)?)
}

#[tokio::test]
async fn test_upload_bytes_to_iroh() -> Result<()> {
    let node = iroh::node::Node::memory().spawn().await?;
    let ent = new_entangler_from_node(&node)?;

    let bytes = create_bytes(NUM_CHUNKS);
    let hashes = ent.upload(bytes.clone()).await?;

    let data_hash = iroh::blobs::Hash::from_str(&hashes.0)?;
    let res = node.blobs().read_to_bytes(data_hash).await?;
    assert_eq!(res, bytes, "original data mismatch");

    let metadata_hash = iroh::blobs::Hash::from_str(&hashes.1)?;
    let metadata_bytes = node.blobs().read_to_bytes(metadata_hash).await?;

    let metadata: Metadata = serde_json::from_slice(&metadata_bytes)?;
    assert_eq!(metadata.orig_hash, hashes.0, "metadata orig_hash mismatch");
    assert_eq!(
        metadata.num_bytes,
        bytes.len() as u64,
        "metadata size mismatch"
    );
    assert_eq!(
        metadata.chunk_size, CHUNK_SIZE as u64,
        "metadata chunk_size mismatch"
    );
    assert_eq!(metadata.s, HEIGHT as u8, "metadata s mismatch");
    assert_eq!(metadata.p, HEIGHT as u8, "metadata p mismatch");

    for (strand_type, parity_hash) in &metadata.parity_hashes {
        let parity_hash = iroh::blobs::Hash::from_str(parity_hash)?;
        let parity = node.blobs().read_to_bytes(parity_hash).await?;
        let mut expected_parity = BytesMut::with_capacity(NUM_CHUNKS * CHUNK_SIZE);
        for i in 0..NUM_CHUNKS {
            let chunk1 = &bytes[i * CHUNK_SIZE..(i + 1) * CHUNK_SIZE];
            let x = (i / HEIGHT + 1) % WIDTH;
            let y = ((i + HEIGHT) as i64 + strand_type.to_i64()) as usize % HEIGHT;
            let i2 = x * HEIGHT + y;
            let chunk2 = &bytes[i2 * CHUNK_SIZE..(i2 + 1) * CHUNK_SIZE];
            let expected_chunk = xor_chunks(chunk1, chunk2);
            expected_parity.extend_from_slice(&expected_chunk);
            let actual_chunk = &parity[i * CHUNK_SIZE..(i + 1) * CHUNK_SIZE];
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
    let hashes = ent.upload(bytes.clone()).await?;

    let downloaded_bytes = ent.download(&hashes.0, None).await?;
    assert_eq!(downloaded_bytes, bytes, "downloaded data mismatch");
    Ok(())
}

#[tokio::test]
async fn if_blob_is_missing_and_no_provided_metadata_error() -> Result<()> {
    let node = iroh::node::Node::memory().spawn().await?;
    let ent = new_entangler_from_node(&node)?;

    let bytes = create_bytes(NUM_CHUNKS);
    let hashes = ent.upload(bytes.clone()).await?;

    let node_with_metadata = iroh::node::Node::memory().spawn().await?;
    load_parity_data_to_node(&node_with_metadata, &node, &hashes.1).await?;

    let ent_with_metadata = new_entangler_from_node(&node_with_metadata)?;

    let result = ent_with_metadata.download(&hashes.0, None).await;
    assert!(result.is_err(), "expected download to fail");
    Ok(())
}

#[tokio::test]
async fn if_blob_is_missing_and_metadata_is_provided_error() -> Result<()> {
    let node = iroh::node::Node::memory().spawn().await?;
    let ent = new_entangler_from_node(&node)?;

    let bytes = create_bytes(NUM_CHUNKS);
    let hashes = ent.upload(bytes.clone()).await?;

    let node_with_metadata = iroh::node::Node::memory().spawn().await?;
    load_parity_data_to_node(&node_with_metadata, &node, &hashes.1).await?;

    let ent_with_metadata = new_entangler_from_node(&node_with_metadata)?;

    let result = ent_with_metadata.download(&hashes.0, Some(&hashes.1)).await;
    assert!(result.is_err(), "expected download to fail");
    Ok(())
}

#[tokio::test]
async fn if_chunk_is_missing_and_metadata_is_provided_should_repair() -> Result<()> {
    let mock_storage = FakeStorage::new();
    let ent = Entangler::new(mock_storage.clone(), 3, HEIGHT as u8, HEIGHT as u8)?;

    let bytes = create_bytes(NUM_CHUNKS);
    let hashes = ent.upload(bytes.clone()).await?;

    mock_storage.fake_failed_download(&hashes.0);
    mock_storage.fake_failed_chunks(&hashes.0, vec![2]);

    let result = ent.download(&hashes.0, Some(&hashes.1)).await;
    assert!(!result.is_err(), "expected download to succeed");
    let downloaded_bytes = result?;
    assert_eq!(downloaded_bytes, bytes, "downloaded data mismatch");

    Ok(())
}

#[tokio::test]
async fn test_entangler_repair_scenarios() -> Result<()> {
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

                st.fake_failed_download(&metadata.parity_hashes[&StrandType::Right]);
                st.fake_failed_download(&metadata.parity_hashes[&StrandType::Horizontal]);
            },
            should_succeed: true,
        },
        TestCase {
            name: "1 chunk is missing and only R strand is available",
            setup: |st, metadata| {
                st.fake_failed_chunks(&metadata.orig_hash, vec![2]);

                st.fake_failed_download(&metadata.parity_hashes[&StrandType::Left]);
                st.fake_failed_download(&metadata.parity_hashes[&StrandType::Horizontal]);
            },
            should_succeed: true,
        },
        TestCase {
            name: "1 chunk is missing and only H strand is available",
            setup: |st, metadata| {
                st.fake_failed_chunks(&metadata.orig_hash, vec![2]);

                st.fake_failed_download(&metadata.parity_hashes[&StrandType::Left]);
                st.fake_failed_download(&metadata.parity_hashes[&StrandType::Right]);
            },
            should_succeed: true,
        },
        TestCase {
            name: "several disjoint chunks are missing",
            setup: |st, metadata| {
                st.fake_failed_chunks(&metadata.orig_hash, vec![1, 3, 10, 13, 21, 23]);

                st.fake_failed_download(&metadata.parity_hashes[&StrandType::Left]);
                st.fake_failed_download(&metadata.parity_hashes[&StrandType::Horizontal]);
            },
            should_succeed: true,
        },
        TestCase {
            name: "no parity is available, should fail",
            setup: |st, metadata| {
                st.fake_failed_chunks(&metadata.orig_hash, vec![2]);

                st.fake_failed_download(&metadata.parity_hashes[&StrandType::Left]);
                st.fake_failed_download(&metadata.parity_hashes[&StrandType::Right]);
                st.fake_failed_download(&metadata.parity_hashes[&StrandType::Horizontal]);
            },
            should_succeed: false,
        },
        /*TestCase {
            name: "an island of missing chunks",
            setup: |st, metadata| {
                //  0  5  .  5 20 25
                //  1  .  .  . 21 26
                //  .  .  .  .  . 27
                //  3  .  .  . 23 28
                //  4  9  . 19 24 29
                st.fake_failed_chunks(
                    &metadata.orig_hash,
                    vec![2, 6, 7, 8, 10, 11, 12, 13, 14, 16, 17, 18, 22],
                );

                st.fake_failed_download(&metadata.parity_hashes[&StrandType::Left]);
                st.fake_failed_download(&metadata.parity_hashes[&StrandType::Horizontal]);
            },
            should_succeed: true,
        },
        TestCase {
            name: "several islands of missing chunks",
            setup: |st, metadata| {
                //  0  5  . 15 20 25  .  .  .  .
                //  1  .  .  . 21 26  .  .  . 46
                //  .  .  .  .  . 27  .  . 42 47
                //  3  .  .  . 23 28 33 38 43  .
                //  4  9  . 19 24 29 34 39 44  .
                st.fake_failed_chunks(
                    &metadata.orig_hash,
                    vec![
                        2, 6, 7, 8, 10, 11, 12, 13, 14, 16, 17, 18, 22, 30, 31, 32, 35, 36, 37, 40,
                        41, 45, 48, 49,
                    ],
                );

                st.fake_failed_download(&metadata.parity_hashes[&StrandType::Horizontal]);
                st.fake_failed_download(&metadata.parity_hashes[&StrandType::Right]);
            },
            should_succeed: true,
        },
        TestCase {
            name: "only 1 chunk is available",
            setup: |st, metadata| {
                st.fake_failed_chunks(&metadata.orig_hash, (0..12).chain(13..CHUNK_SIZE).collect());

                st.fake_failed_download(&metadata.parity_hashes[&StrandType::Left]);
            },
            should_succeed: true,
        },*/
    ];

    for case in test_cases {
        println!("Running test case: {}", case.name);

        let mock_storage = FakeStorage::new();
        let ent = Entangler::new(mock_storage.clone(), 3, HEIGHT as u8, HEIGHT as u8)?;

        let bytes = create_bytes(NUM_CHUNKS);
        let hashes = ent.upload(bytes.clone()).await?;
        let metadata = load_metadata(&hashes.1, &mock_storage).await?;

        mock_storage.fake_failed_download(&metadata.orig_hash);

        (case.setup)(&mock_storage, &metadata);

        let result = ent.download(&hashes.0, Some(&hashes.1)).await;

        if case.should_succeed {
            assert!(
                !result.is_err(),
                "expected download to succeed for case: {}",
                case.name
            );
            let downloaded_bytes = result?;
            assert_eq!(
                downloaded_bytes, bytes,
                "downloaded data mismatch for case: {}",
                case.name
            );
        } else {
            assert!(
                result.is_err(),
                "expected download to fail for case: {}",
                case.name
            );
        }
    }

    Ok(())
}
