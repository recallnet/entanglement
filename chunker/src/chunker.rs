// Copyright 2024 Entanglement Contributors
// SPDX-License-Identifier: Apache-2.0, MIT

use anyhow::Result;
use bao;
use bytes::Bytes;

use crate::tree::*;

const CHUNK_SIZE: usize = 1024;
const HASH_SIZE: usize = 32;
const HASH_PAIR_SIZE: usize = HASH_SIZE * 2;

/// Chunker is a struct that handles the chunking and tree building process for a given input data.
pub struct Chunker {
    chunk_size: usize,
    tree_height: usize,
    visit_count: usize,
    chunks: Bytes,
    outboard: Bytes,
}

impl Chunker {
    /// Creates a new instance of Chunker with default values.
    pub fn new() -> Self {
        Self {
            chunk_size: CHUNK_SIZE,
            tree_height: 0,
            visit_count: 0,
            chunks: Bytes::new(),
            outboard: Bytes::new(),
        }
    }

    /// Chunks the input data and builds a tree structure.
    ///
    /// # Arguments
    ///
    /// * `data` - The input data as a `Bytes` object.
    ///
    /// # Returns
    ///
    /// * `Result<Node>` - The root node of the built tree.
    pub fn chunk(&mut self, data: Bytes) -> Result<Node> {
        let (outboard, root_hash) = bao::encode::outboard(&data);
        let chunk_num = ((data.len() as f64) / CHUNK_SIZE as f64).ceil() as usize;
        self.tree_height = (chunk_num as f64).log2().ceil() as usize + 1;
        self.chunks = data;
        self.outboard = Bytes::from(outboard).split_off(8);
        self.visit_count = 0;

        let hash_bytes = Bytes::from(Vec::from(root_hash.as_bytes()));
        let tree = self.build_tree(0, 0, chunk_num, hash_bytes)?;
        Ok(tree)
    }

    /// Recursively builds the tree structure.
    ///
    /// # Arguments
    ///
    /// * `depth` - The current depth in the tree.
    /// * `chunk_index` - The index of the left-most chunk.
    /// * `rem_chunks` - The remaining number of chunks to process.
    /// * `hash` - The hash of the current node.
    ///
    /// # Returns
    ///
    /// * `Result<Node>` - The built tree node.
    fn build_tree(
        &mut self,
        depth: usize,
        chunk_index: usize,
        rem_chunks: usize,
        hash: Bytes,
    ) -> Result<Node> {
        // zero can happen only if the input data is empty
        if rem_chunks <= 1 {
            let start = chunk_index * self.chunk_size;
            let end = std::cmp::min(start + self.chunk_size, self.chunks.len());
            let data = self.chunks.slice(start..end);
            Ok(Node {
                hash,
                content: NodeContent::Leaf { data },
            })
        } else {
            let hash_offset = (self.visit_count) * HASH_PAIR_SIZE;
            let mut left_hash = self
                .outboard
                .slice(hash_offset..hash_offset + HASH_PAIR_SIZE);
            let right_hash = left_hash.split_off(HASH_SIZE);

            self.visit_count += 1;

            let sub_size = (1 << (self.tree_height - depth - 1)) / 2;
            let left_child = self.build_tree(depth + 1, chunk_index, sub_size, left_hash)?;
            let right_child = self.build_tree(
                depth + 1,
                chunk_index + sub_size,
                rem_chunks - sub_size,
                right_hash,
            )?;

            Ok(Node {
                hash,
                content: NodeContent::Parent {
                    left: Box::new(left_child),
                    right: Box::new(right_child),
                },
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_data(chunk_count: usize) -> Bytes {
        let mut data = Vec::new();
        for i in 0..chunk_count {
            let chunk = vec![b'a' + i as u8; CHUNK_SIZE];
            data.extend_from_slice(&chunk);
        }
        Bytes::from(data)
    }

    fn to_blake3(bytes: Bytes) -> blake3::Hash {
        blake3::hash(bytes.as_ref())
    }

    fn make_chunk_of(byte: u8) -> Bytes {
        Bytes::from(vec![byte; CHUNK_SIZE])
    }

    fn hash_for_chunk(ind: usize) -> Bytes {
        hash_for_chunk_data(ind, &make_chunk_of(b'a' + ind as u8), false)
    }

    fn hash_for_chunk_data(ind: usize, data: &[u8], is_root: bool) -> Bytes {
        let mut hasher = blake3::guts::ChunkState::new(ind as u64);
        hasher.update(data);
        let h = hasher.finalize(is_root);
        Bytes::from(Vec::from(h.as_bytes()))
    }

    fn slice_to_array(slice: &[u8]) -> [u8; 32] {
        let mut array = [0u8; 32];
        array.copy_from_slice(slice);
        array
    }

    fn hash_for_parent(left: &Bytes, right: &Bytes, is_root: bool) -> Bytes {
        let left_hash = blake3::Hash::from_bytes(slice_to_array(left.as_ref()));
        let right_hash = blake3::Hash::from_bytes(slice_to_array(right.as_ref()));
        let parent_hash = blake3::guts::parent_cv(&left_hash, &right_hash, is_root);
        Bytes::from(Vec::from(parent_hash.as_bytes()))
    }

    #[test]
    fn test_no_data() {
        let data = Bytes::new();
        let mut chunker = Chunker::new();
        let tree = chunker.chunk(data.clone()).unwrap();

        assert_eq!(tree.content, NodeContent::Leaf { data: data });
        assert_eq!(tree.hash, hash_for_chunk_data(0, &[], true));
    }

    #[test]
    fn test_one_chunk() {
        let data = create_test_data(1);
        let mut chunker = Chunker::new();
        let tree = chunker.chunk(data).unwrap();

        assert_eq!(
            tree.content,
            NodeContent::Leaf {
                data: make_chunk_of(b'a')
            }
        );

        assert_eq!(
            tree.hash.as_ref(),
            to_blake3(make_chunk_of(b'a')).as_bytes(),
            "hashes don't match"
        );
    }

    #[test]
    fn test_two_chunks() {
        let data = create_test_data(2);
        let mut chunker = Chunker::new();
        let tree = chunker.chunk(data).unwrap();

        if let NodeContent::Parent { left, right } = tree.content {
            assert_eq!(
                left.content,
                NodeContent::Leaf {
                    data: make_chunk_of(b'a')
                }
            );
            assert_eq!(
                right.content,
                NodeContent::Leaf {
                    data: make_chunk_of(b'b')
                }
            );
        } else {
            panic!("Expected an internal node");
        }
    }

    #[test]
    fn test_four_chunks() {
        let data = create_test_data(4);
        let mut chunker = Chunker::new();
        let tree = chunker.chunk(data).unwrap();

        if let NodeContent::Parent { left, right } = tree.content {
            if let NodeContent::Parent {
                left: left_left,
                right: left_right,
            } = left.content
            {
                assert_eq!(
                    left_left.content,
                    NodeContent::Leaf {
                        data: make_chunk_of(b'a')
                    }
                );
                assert_eq!(
                    left_right.content,
                    NodeContent::Leaf {
                        data: make_chunk_of(b'b')
                    }
                );

                assert_eq!(left_left.hash, hash_for_chunk(0), "hashes don't match");
                assert_eq!(left_right.hash, hash_for_chunk(1), "hashes don't match");
                let left_hash = hash_for_parent(&left_left.hash, &left_right.hash, false);
                assert_eq!(left_hash, left.hash, "hashes don't match");
            } else {
                panic!("Expected an internal node");
            }

            if let NodeContent::Parent {
                left: right_left,
                right: right_right,
            } = right.content
            {
                assert_eq!(
                    right_left.content,
                    NodeContent::Leaf {
                        data: make_chunk_of(b'c')
                    }
                );
                assert_eq!(
                    right_right.content,
                    NodeContent::Leaf {
                        data: make_chunk_of(b'd')
                    }
                );

                assert_eq!(right_left.hash, hash_for_chunk(2), "hashes don't match");
                assert_eq!(right_right.hash, hash_for_chunk(3), "hashes don't match");
                let right_hash = hash_for_parent(&right_left.hash, &right_right.hash, false);
                assert_eq!(right_hash, right.hash, "hashes don't match");
            } else {
                panic!("Expected an internal node");
            }

            let root_hash = hash_for_parent(&left.hash, &right.hash, true);
            assert_eq!(root_hash, tree.hash, "hashes don't match");
        } else {
            panic!("Expected an internal node");
        }
    }

    #[test]
    fn incomplete_first_chunk() {
        let mut data = create_test_data(1);
        data.truncate(CHUNK_SIZE / 4);

        let mut chunker = Chunker::new();
        let tree = chunker.chunk(data.clone()).unwrap();

        assert_eq!(tree.content, NodeContent::Leaf { data: data.clone() });
        assert_eq!(
            tree.hash,
            hash_for_chunk_data(0, &[b'a'; CHUNK_SIZE / 4], true)
        );
    }

    #[test]
    fn test_incomplete_last_chunk() {
        let mut data = create_test_data(2);
        data.truncate(CHUNK_SIZE + CHUNK_SIZE / 2);

        let mut chunker = Chunker::new();
        let tree = chunker.chunk(data.clone()).unwrap();

        if let NodeContent::Parent { left, right } = tree.content {
            assert_eq!(
                left.content,
                NodeContent::Leaf {
                    data: make_chunk_of(b'a')
                }
            );
            assert_eq!(
                right.content,
                NodeContent::Leaf {
                    data: make_chunk_of(b'b').split_to(CHUNK_SIZE / 2)
                }
            );
        } else {
            panic!("Expected an internal node");
        }
    }

    #[test]
    fn test_power_of_two_plus_one_num_chunks() {
        let data = create_test_data(5);
        let mut chunker = Chunker::new();
        let tree = chunker.chunk(data).unwrap();

        if let NodeContent::Parent { left: l, right: r } = tree.content {
            if let NodeContent::Parent {
                left: l_l,
                right: l_r,
            } = l.content
            {
                if let NodeContent::Parent {
                    left: l_l_l,
                    right: l_l_r,
                } = l_l.content
                {
                    assert_eq!(
                        l_l_l.content,
                        NodeContent::Leaf {
                            data: make_chunk_of(b'a')
                        }
                    );
                    assert_eq!(
                        l_l_r.content,
                        NodeContent::Leaf {
                            data: make_chunk_of(b'b')
                        }
                    );

                    assert_eq!(l_l_l.hash, hash_for_chunk(0), "hashes don't match");
                    assert_eq!(l_l_r.hash, hash_for_chunk(1), "hashes don't match");
                    let l_h = hash_for_parent(&l_l_l.hash, &l_l_r.hash, false);
                    assert_eq!(l_h, l_l.hash, "hashes don't match");
                }

                if let NodeContent::Parent {
                    left: l_r_l,
                    right: l_r_r,
                } = l_r.content
                {
                    assert_eq!(
                        l_r_l.content,
                        NodeContent::Leaf {
                            data: make_chunk_of(b'c')
                        }
                    );
                    assert_eq!(
                        l_r_r.content,
                        NodeContent::Leaf {
                            data: make_chunk_of(b'd')
                        }
                    );

                    assert_eq!(l_r_l.hash, hash_for_chunk(2), "hashes don't match");
                    assert_eq!(l_r_r.hash, hash_for_chunk(3), "hashes don't match");
                    let l_h = hash_for_parent(&l_r_l.hash, &l_r_r.hash, false);
                    assert_eq!(l_h, l_r.hash, "hashes don't match");
                }
            } else {
                panic!("Expected an internal node");
            }

            assert_eq!(
                r.content,
                NodeContent::Leaf {
                    data: make_chunk_of(b'e')
                }
            );

            let root_hash = hash_for_parent(&l.hash, &r.hash, true);
            assert_eq!(root_hash, tree.hash, "hashes don't match");
        } else {
            panic!("Expected an internal node");
        }
    }

    #[test]
    fn test_power_of_two_plus_one_number_of_chunks() {
        let data = create_test_data(7);
        let mut chunker = Chunker::new();
        let tree = chunker.chunk(data).unwrap();

        if let NodeContent::Parent { left: l, right: r } = tree.content {
            if let NodeContent::Parent {
                left: l_l,
                right: l_r,
            } = l.content
            {
                if let NodeContent::Parent {
                    left: l_l_l,
                    right: l_l_r,
                } = l_l.content
                {
                    assert_eq!(
                        l_l_l.content,
                        NodeContent::Leaf {
                            data: make_chunk_of(b'a')
                        }
                    );
                    assert_eq!(
                        l_l_r.content,
                        NodeContent::Leaf {
                            data: make_chunk_of(b'b')
                        }
                    );

                    assert_eq!(l_l_l.hash, hash_for_chunk(0), "hashes don't match");
                    assert_eq!(l_l_r.hash, hash_for_chunk(1), "hashes don't match");
                    let l_h = hash_for_parent(&l_l_l.hash, &l_l_r.hash, false);
                    assert_eq!(l_h, l_l.hash, "hashes don't match");
                }

                if let NodeContent::Parent {
                    left: l_r_l,
                    right: l_r_r,
                } = l_r.content
                {
                    assert_eq!(
                        l_r_l.content,
                        NodeContent::Leaf {
                            data: make_chunk_of(b'c')
                        }
                    );
                    assert_eq!(
                        l_r_r.content,
                        NodeContent::Leaf {
                            data: make_chunk_of(b'd')
                        }
                    );

                    assert_eq!(l_r_l.hash, hash_for_chunk(2), "hashes don't match");
                    assert_eq!(l_r_r.hash, hash_for_chunk(3), "hashes don't match");
                    let l_h = hash_for_parent(&l_r_l.hash, &l_r_r.hash, false);
                    assert_eq!(l_h, l_r.hash, "hashes don't match");
                }
            } else {
                panic!("Expected an internal node");
            }

            if let NodeContent::Parent {
                left: r_l,
                right: r_r,
            } = r.content
            {
                if let NodeContent::Parent {
                    left: r_l_l,
                    right: r_l_r,
                } = r_l.content
                {
                    assert_eq!(
                        r_l_l.content,
                        NodeContent::Leaf {
                            data: make_chunk_of(b'e')
                        }
                    );
                    assert_eq!(
                        r_l_r.content,
                        NodeContent::Leaf {
                            data: make_chunk_of(b'f')
                        }
                    );

                    assert_eq!(r_l_l.hash, hash_for_chunk(4), "hashes don't match");
                    assert_eq!(r_l_r.hash, hash_for_chunk(5), "hashes don't match");
                    let r_h = hash_for_parent(&r_l_l.hash, &r_l_r.hash, false);
                    assert_eq!(r_h, r_l.hash, "hashes don't match");

                    assert_eq!(
                        r_r.content,
                        NodeContent::Leaf {
                            data: make_chunk_of(b'g')
                        }
                    );

                    let r_hash = hash_for_parent(&r_l.hash, &r_r.hash, false);
                    assert_eq!(r_hash, r.hash, "hashes don't match");

                    let root_hash = hash_for_parent(&l.hash, &r.hash, true);
                    assert_eq!(root_hash, tree.hash, "hashes don't match");
                } else {
                    panic!("Expected an internal node");
                }
            } else {
                panic!("Expected an internal node");
            }
        } else {
            panic!("Expected an internal node");
        }
    }
}
