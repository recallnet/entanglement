// Copyright 2024 Entanglement Contributors
// SPDX-License-Identifier: Apache-2.0, MIT

use bytes::{BufMut, Bytes, BytesMut};
use entangler;
use std::str::FromStr;
use storage;

const HEIGHT: usize = 3;
const WIDTH: usize = 6;
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

#[tokio::test]
async fn test_upload_bytes_to_iroh() {
    let node = iroh::node::Node::memory().spawn().await.unwrap();
    let st = storage::iroh::IrohStorage::from_client(node.client().clone());
    let ent = entangler::Entangler::new(st, 3, HEIGHT as u8, HEIGHT as u8).unwrap();

    let bytes = create_bytes(NUM_CHUNKS);
    let hashes = ent.upload_bytes(bytes.clone()).await.unwrap();

    let data_hash = iroh::blobs::Hash::from_str(&hashes.0).unwrap();
    let res = node.blobs().read_to_bytes(data_hash).await.unwrap();
    assert_eq!(res, bytes, "original data mismatch");

    let metadata_hash = iroh::blobs::Hash::from_str(&hashes.1).unwrap();
    let metadata_bytes = node.blobs().read_to_bytes(metadata_hash).await.unwrap();

    let metadata: entangler::Metadata = serde_json::from_slice(&metadata_bytes).unwrap();
    assert_eq!(metadata.orig_hash, hashes.0, "metadata orig_hash mismatch");

    for (strand_type, parity_hash) in &metadata.parity_hashes {
        let parity_hash = iroh::blobs::Hash::from_str(parity_hash).unwrap();
        let parity = node.blobs().read_to_bytes(parity_hash).await.unwrap();
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
}

#[tokio::test]
async fn test_download_bytes_from_iroh() {
    let node = iroh::node::Node::memory().spawn().await.unwrap();
    let st = storage::iroh::IrohStorage::from_client(node.client().clone());
    let ent = entangler::Entangler::new(st, 3, HEIGHT as u8, HEIGHT as u8).unwrap();

    let bytes = create_bytes(NUM_CHUNKS);
    let hashes = ent.upload_bytes(bytes.clone()).await.unwrap();

    let downloaded_bytes = ent.download_bytes(&hashes.0).await.unwrap();
    assert_eq!(downloaded_bytes, bytes, "downloaded data mismatch");
}
