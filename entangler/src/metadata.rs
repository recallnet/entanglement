// Copyright 2024 Entanglement Contributors
// SPDX-License-Identifier: Apache-2.0, MIT

use serde::{Deserialize, Serialize};

/// Metadata struct that holds information about the original blob and the parity blobs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Metadata {
    pub orig_hash: String,
    pub parity_hashes: Vec<String>,
    pub num_bytes: u64,
    pub chunk_size: u64,
    pub s: u8,
    pub p: u8,
}
