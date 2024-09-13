// Copyright 2024 Entanglement Contributors
// SPDX-License-Identifier: Apache-2.0, MIT

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::parity::StrandType;

#[derive(Debug, Serialize, Deserialize)]
pub struct Metadata {
    pub orig_hash: String,
    pub parity_hashes: HashMap<StrandType, String>,
    pub num_bytes: u64,
    pub chunk_size: u64,
    pub s: u8,
    pub p: u8,
}
