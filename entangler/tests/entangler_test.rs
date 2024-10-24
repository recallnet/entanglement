// Copyright 2024 Entanglement Contributors
// SPDX-License-Identifier: Apache-2.0, MIT

use anyhow::Result;
use bytes::{BufMut, Bytes, BytesMut};
use entangler::{self, parity::StrandType, ChunkRange, Entangler, Metadata};
use std::collections::HashSet;
use std::str::FromStr;
use storage::{self, mock::FakeStorage, ChunkIdMapper, Storage};

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
    for parity_hash in metadata.parity_hashes.values() {
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
        metadata.chunk_size, CHUNK_SIZE,
        "metadata chunk_size mismatch"
    );
    assert_eq!(metadata.s, HEIGHT as u8, "metadata s mismatch");
    assert_eq!(metadata.p, HEIGHT as u8, "metadata p mismatch");

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
    assert!(result.is_ok(), "expected download to succeed");
    let downloaded_bytes = result?;
    assert_eq!(downloaded_bytes, bytes, "downloaded data mismatch");

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
            let ent = Entangler::new(mock_storage.clone(), 3, HEIGHT as u8, HEIGHT as u8)?;

            let bytes = create_bytes(NUM_CHUNKS);

            let hashes = if upload_method == "upload" {
                let hash = mock_storage.upload_bytes(bytes.clone()).await?;
                let metadata_hash = ent.entangle_uploaded(hash.clone()).await?;
                (hash, metadata_hash)
            } else {
                ent.upload(bytes.clone()).await?
            };

            let metadata = load_metadata(&hashes.1, &mock_storage).await?;

            mock_storage.fake_failed_download(&metadata.orig_hash);

            (case.setup)(&mock_storage, &metadata);

            let result = ent.download(&hashes.0, Some(&hashes.1)).await;

            if case.should_succeed {
                assert!(
                    result.is_ok(),
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
                let ent = Entangler::new(mock_storage.clone(), 3, HEIGHT as u8, HEIGHT as u8)?;

                let bytes = create_bytes(NUM_CHUNKS);

                let hashes = ent.upload(bytes.clone()).await?;
                let metadata = load_metadata(&hashes.1, &mock_storage).await?;

                mock_storage.fake_failed_download(&metadata.orig_hash);

                (case.setup)(&mock_storage, &metadata);

                let (first, last) = match req.range {
                    ChunkRange::From(first) => (first, NUM_CHUNKS - 1),
                    ChunkRange::Till(last) => (0, last),
                    ChunkRange::Between(first, last) => (first, last),
                };

                if download_method == "range" {
                    let result = ent
                        .download_range(&hashes.0, req.range, Some(&hashes.1))
                        .await;
                    assert!(
                        result.is_ok(),
                        "expected download_range to succeed for case: {}, {}, but got: {:?}",
                        case.name,
                        req.name,
                        result.err().unwrap(),
                    );
                    let downloaded_bytes = result?;
                    let expected_bytes = bytes
                        .slice((first * CHUNK_SIZE) as usize..((last + 1) * CHUNK_SIZE) as usize);
                    assert_eq!(
                        downloaded_bytes, expected_bytes,
                        "downloaded data mismatch for case: {}",
                        case.name
                    );
                } else {
                    let mapper = mock_storage.chunk_id_mapper(&hashes.0).await?;
                    let chunks = (first..=last)
                        .map(|i| mapper.index_to_id(i).unwrap())
                        .collect();
                    let result = ent
                        .download_chunks(&hashes.0, chunks, Some(&hashes.1))
                        .await;
                    assert!(
                        result.is_ok(),
                        "expected download_chunks to succeed for case: {}, {}",
                        case.name,
                        req.name,
                    );
                    let downloaded_chunks = result?;
                    for i in first..=last {
                        let id = mapper.index_to_id(i).unwrap();
                        assert_eq!(
                            downloaded_chunks[&id],
                            &bytes[(i * CHUNK_SIZE) as usize..((i + 1) * CHUNK_SIZE) as usize],
                            "downloaded chunk mismatch for case: {}, {}",
                            case.name,
                            req.name
                        );
                    }
                }
            }
        }
    }

    Ok(())
}
