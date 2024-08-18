// Copyright 2024 Entanglement Contributors
// SPDX-License-Identifier: Apache-2.0, MIT

use bytes::Bytes;
use thiserror::Error;

use crate::grid::Grid;
use anyhow::Result;

#[derive(Debug, Clone, Copy)]
pub enum StrandType {
    Left,
    Horizontal,
    Right,
}

impl StrandType {
    pub fn to_i64(self) -> i64 {
        match self {
            StrandType::Left => -1,
            StrandType::Horizontal => 0,
            StrandType::Right => 1,
        }
    }
}

pub struct Lattice {
    orig_grid: Grid,
    parity_grids: Vec<ParityGrid>,
}

pub struct ParityGrid {
    pub grid: Grid,
    pub strand_type: StrandType,
}

impl Lattice {
    pub fn new(orig_grid: Grid, parity_grids: Vec<ParityGrid>) -> Self {
        Self {
            orig_grid,
            parity_grids,
        }
    }

    pub fn get_parities(&self) -> &Vec<ParityGrid> {
        &self.parity_grids
    }
}
