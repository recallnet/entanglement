// Copyright 2024 Entanglement Contributors
// SPDX-License-Identifier: Apache-2.0, MIT

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::parity::StrandType;

/// Blob struct that holds information about the blob.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlobData {
    pub hash: String,
    pub size: u64,
    pub info: HashMap<String, String>,
}

/// Metadata struct that holds information about the original blob and the parity blobs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Metadata {
    pub blob: BlobData,
    pub parities: HashMap<StrandType, BlobData>,
    pub chunk_size: u64,
    pub s: u8,
    pub p: u8,
}
