// Copyright 2024 Entanglement Contributors
// SPDX-License-Identifier: Apache-2.0, MIT

use crate::grid::{self, Grid, GridBuilder};
use crate::lattice::{Lattice, ParityGrid, StrandType};
use anyhow::Result;

use bytes::Bytes;

/// The executer is responsible for creating a lattice from the given grid.
pub struct Executer {
    alpha: u8,
}

impl Executer {
    /// Create a new executer with the given alpha.
    /// The alpha is the number of parity grids to create.
    pub fn new(alpha: u8) -> Self {
        Self { alpha }
    }

    /// Executes the entanglement process on the given grid.
    /// It creates a lattice with the original grid and the parity grids.
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

/// Create a parity grid from the given grid and strand type.
/// The parity grid is created by entangling each cell with the next cell along the strand.
/// The last cells along x axis are entangled with the first cells forming toroidal lattice.
fn create_parity_grid(grid: &Grid, strand_type: StrandType) -> Result<ParityGrid> {
    let mut parity_grid = GridBuilder::new(grid.get_num_items(), grid.get_height())?;
    for x in 0..grid.get_width() {
        for y in 0..grid.get_height() {
            let mut xi = x as i64;
            let mut yi = y as i64;
            if let Some(cell) = grid.try_get_cell(xi, yi) {
                xi += 1;
                yi += strand_type.to_i64();
                if let Some(pair) = grid.try_get_cell(xi, yi) {
                    parity_grid.set_cell(x, y, entangle_chunks(cell, pair));
                } else {
                    // we need LW size. At the moment we assume it's square with side equal to grid's height
                    let lw_size = grid.get_height() as i64;
                    // calculate the number of steps to go along the strand
                    let steps = lw_size - (xi % lw_size);
                    let pair = grid.get_cell(xi + steps, yi + steps * strand_type.to_i64());
                    parity_grid.set_cell(x, y, entangle_chunks(cell, pair));
                }
            }
        }
    }
    Ok(ParityGrid {
        grid: parity_grid.build(),
        strand_type,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::grid::Grid;
    use bytes::Bytes;
    use std::str;

    const WIDTH: usize = 3;
    const HEIGHT: usize = 3;

    fn create_chunks() -> Vec<Bytes> {
        vec![
            Bytes::from("a"),
            Bytes::from("b"),
            Bytes::from("c"),
            Bytes::from("d"),
            Bytes::from("e"),
            Bytes::from("f"),
            Bytes::from("g"),
            Bytes::from("h"),
            Bytes::from("i"),
        ]
    }

    fn entangle(s1: &str, s2: &str) -> Bytes {
        entangle_chunks(&Bytes::from(s1.to_string()), &Bytes::from(s2.to_string()))
    }

    fn entangle_by_pos(x: usize, y: usize, dy: i64) -> Bytes {
        const CHARS: &str = "abcdefghi";
        let pos1 = x * HEIGHT + y;
        // calculate y of the second cell relative to y of the first cell
        // we add HEIGHT in case y is negative
        let y2 = ((y + HEIGHT) as i64 + dy) % HEIGHT as i64;
        let pos2 = ((x + 1) % WIDTH) * HEIGHT + y2 as usize;
        let ch1 = &CHARS[pos1..pos1 + 1];
        let ch2 = &CHARS[pos2..pos2 + 1];
        entangle(ch1, ch2)
    }

    fn assert_parity_grid(parity_grid: &ParityGrid) {
        let dy = parity_grid.strand_type.to_i64();

        let g = &parity_grid.grid;
        assert_eq!(g.get_cell(0, 0), &entangle_by_pos(0, 0, dy));
        assert_eq!(g.get_cell(0, 1), &entangle_by_pos(0, 1, dy));
        assert_eq!(g.get_cell(0, 2), &entangle_by_pos(0, 2, dy));
        assert_eq!(g.get_cell(1, 0), &entangle_by_pos(1, 0, dy));
        assert_eq!(g.get_cell(1, 1), &entangle_by_pos(1, 1, dy));
        assert_eq!(g.get_cell(1, 2), &entangle_by_pos(1, 2, dy));
        assert_eq!(g.get_cell(2, 0), &entangle_by_pos(2, 0, dy));
        assert_eq!(g.get_cell(2, 1), &entangle_by_pos(2, 1, dy));
        assert_eq!(g.get_cell(2, 2), &entangle_by_pos(2, 2, dy));
    }

    #[test]
    fn test_entangle_chunks() {
        let chunk1 = Bytes::from(vec![0b00000000, 0b11110000]);
        let chunk2 = Bytes::from(vec![0b10101010, 0b00001111]);
        let expected = Bytes::from(vec![0b10101010, 0b11111111]);
        let result = entangle_chunks(&chunk1, &chunk2);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_create_parity_grid() {
        let grid = Grid::new(create_chunks(), 3).unwrap();

        let parity_grid = create_parity_grid(&grid, StrandType::Left).unwrap();
        assert_parity_grid(&parity_grid);

        let parity_grid = create_parity_grid(&grid, StrandType::Horizontal).unwrap();
        assert_parity_grid(&parity_grid);

        let parity_grid = create_parity_grid(&grid, StrandType::Right).unwrap();
        assert_parity_grid(&parity_grid);
    }

    #[test]
    fn test_execute() {
        let grid = Grid::new(create_chunks(), 3).unwrap();

        let executer = Executer::new(3);
        let lattice = executer.execute(grid).unwrap();

        let orig_grid = lattice.get_orig_grid();
        assert_eq!(orig_grid.get_cell(0, 0), &Bytes::from("a"));
        assert_eq!(orig_grid.get_cell(0, 1), &Bytes::from("b"));
        assert_eq!(orig_grid.get_cell(0, 2), &Bytes::from("c"));
        assert_eq!(orig_grid.get_cell(1, 0), &Bytes::from("d"));
        assert_eq!(orig_grid.get_cell(1, 1), &Bytes::from("e"));
        assert_eq!(orig_grid.get_cell(1, 2), &Bytes::from("f"));
        assert_eq!(orig_grid.get_cell(2, 0), &Bytes::from("g"));
        assert_eq!(orig_grid.get_cell(2, 1), &Bytes::from("h"));
        assert_eq!(orig_grid.get_cell(2, 2), &Bytes::from("i"));

        assert_eq!(lattice.get_parities().len(), 3);

        let lh_strand = lattice.get_parities().get(0).unwrap();
        assert_parity_grid(&lh_strand);

        let h_strand = lattice.get_parities().get(1).unwrap();
        assert_parity_grid(&h_strand);

        let rh_strand = lattice.get_parities().get(2).unwrap();
        assert_parity_grid(&rh_strand);
    }

    #[test]
    fn test_execute_with_unaligned_leap_window() {
        let grid = Grid::new(create_chunks(), 4).unwrap();

        let executer = Executer::new(3);
        let lattice = executer.execute(grid).unwrap();

        let orig_grid = lattice.get_orig_grid();
        assert_eq!(orig_grid.get_cell(0, 0), &Bytes::from("a"));
        assert_eq!(orig_grid.get_cell(0, 1), &Bytes::from("b"));
        assert_eq!(orig_grid.get_cell(0, 2), &Bytes::from("c"));
        assert_eq!(orig_grid.get_cell(0, 3), &Bytes::from("d"));

        assert_eq!(orig_grid.get_cell(1, 0), &Bytes::from("e"));
        assert_eq!(orig_grid.get_cell(1, 1), &Bytes::from("f"));
        assert_eq!(orig_grid.get_cell(1, 2), &Bytes::from("g"));
        assert_eq!(orig_grid.get_cell(1, 3), &Bytes::from("h"));

        assert_eq!(orig_grid.get_cell(2, 0), &Bytes::from("i"));

        assert_eq!(lattice.get_parities().len(), 3);

        // a e i .
        // b f . .
        // c g . .
        // d h . .
        let lh_strand = lattice.get_parities().get(0).unwrap();
        assert_eq!(lh_strand.strand_type, StrandType::Left);
        assert_eq!(lh_strand.grid.get_cell(0, 0), &entangle("a", "h"));
        assert_eq!(lh_strand.grid.get_cell(0, 1), &entangle("b", "e"));
        assert_eq!(lh_strand.grid.get_cell(0, 2), &entangle("c", "f"));
        assert_eq!(lh_strand.grid.get_cell(0, 3), &entangle("d", "g"));
        assert_eq!(lh_strand.grid.get_cell(1, 0), &entangle("e", "b"));
        assert_eq!(lh_strand.grid.get_cell(1, 1), &entangle("f", "i"));
        assert_eq!(lh_strand.grid.get_cell(1, 2), &entangle("g", "d"));
        assert_eq!(lh_strand.grid.get_cell(1, 3), &entangle("h", "a"));
        assert_eq!(lh_strand.grid.get_cell(2, 0), &entangle("i", "c"));

        let h_strand = lattice.get_parities().get(1).unwrap();
        assert_eq!(h_strand.strand_type, StrandType::Horizontal);
        assert_eq!(h_strand.grid.get_cell(0, 0), &entangle("a", "e"));
        assert_eq!(h_strand.grid.get_cell(0, 1), &entangle("b", "f"));
        assert_eq!(h_strand.grid.get_cell(0, 2), &entangle("c", "g"));
        assert_eq!(h_strand.grid.get_cell(0, 3), &entangle("d", "h"));
        assert_eq!(h_strand.grid.get_cell(1, 0), &entangle("e", "i"));
        assert_eq!(h_strand.grid.get_cell(1, 1), &entangle("f", "b"));
        assert_eq!(h_strand.grid.get_cell(1, 2), &entangle("g", "c"));
        assert_eq!(h_strand.grid.get_cell(1, 3), &entangle("h", "d"));
        assert_eq!(h_strand.grid.get_cell(2, 0), &entangle("i", "a"));

        let rh_strand = lattice.get_parities().get(2).unwrap();
        assert_eq!(rh_strand.strand_type, StrandType::Right);
        assert_eq!(rh_strand.grid.get_cell(0, 0), &entangle("a", "f"));
        assert_eq!(rh_strand.grid.get_cell(0, 1), &entangle("b", "g"));
        assert_eq!(rh_strand.grid.get_cell(0, 2), &entangle("c", "h"));
        assert_eq!(rh_strand.grid.get_cell(0, 3), &entangle("d", "e"));
        assert_eq!(rh_strand.grid.get_cell(1, 0), &entangle("e", "d"));
        assert_eq!(rh_strand.grid.get_cell(1, 1), &entangle("f", "a"));
        assert_eq!(rh_strand.grid.get_cell(1, 2), &entangle("g", "b"));
        assert_eq!(rh_strand.grid.get_cell(1, 3), &entangle("h", "i"));
        assert_eq!(rh_strand.grid.get_cell(2, 0), &entangle("i", "c"));
    }
}
