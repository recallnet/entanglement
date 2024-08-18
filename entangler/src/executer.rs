// Copyright 2024 Entanglement Contributors
// SPDX-License-Identifier: Apache-2.0, MIT

use crate::grid::{self, Grid};
use crate::lattice::{Lattice, ParityGrid, StrandType};
use anyhow::Result;

use bytes::Bytes;

pub struct Executer {
    alpha: u8,
    s: u8,
    p: u8,
}

impl Executer {
    pub fn new(alpha: u8, s: u8, p: u8) -> Self {
        Self { alpha, s, p }
    }

    pub fn execute(&self, grid: grid::Grid) -> Result<Lattice> {
        let mut parity_grids = Vec::with_capacity(self.alpha as usize);
        let strand_types = vec![StrandType::Left, StrandType::Horizontal, StrandType::Right];
        for i in 0..self.alpha as usize {
            parity_grids.push(create_parity_grid(&grid, strand_types[i])?);
        }
        return Ok(Lattice::new(grid, parity_grids));
    }
}

fn entangle_chunks(chunk1: &Bytes, chunk2: &Bytes) -> Bytes {
    let mut chunk = Vec::with_capacity(chunk1.len());
    for i in 0..chunk1.len() {
        chunk.push(chunk1[i] ^ chunk2[i]);
    }
    Bytes::from(chunk)
}

fn create_parity_grid(grid: &Grid, strand_type: StrandType) -> Result<ParityGrid> {
    let mut parity_grid = Grid::with_size(grid.get_width(), grid.get_height())?;
    for x in 0..grid.get_width() {
        for y in 0..grid.get_height() {
            let x = x as i64;
            let y = y as i64;
            let parity = entangle_chunks(
                grid.get_cell(x, y),
                grid.get_cell(x + 1, y + strand_type.to_i64()),
            );
            parity_grid.set_cell(x, y, parity);
        }
    }
    Ok(ParityGrid {
        grid: parity_grid,
        strand_type,
    })
}
