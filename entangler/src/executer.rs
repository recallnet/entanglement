// Copyright 2024 Entanglement Contributors
// SPDX-License-Identifier: Apache-2.0, MIT

use crate::grid::{self, Grid, Pos};
use crate::parity::{ParityGrid, StrandType};
use anyhow::Result;

use bytes::Bytes;

/// The executer executes the entanglement process on the given grid.
/// It creates `alpha` parity grids.
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
    /// It creates `alpha` parity grids.
    pub fn execute(&self, grid: grid::Grid) -> Result<Vec<ParityGrid>> {
        let mut parity_grids = Vec::with_capacity(self.alpha as usize);
        let strand_types = vec![StrandType::Left, StrandType::Horizontal, StrandType::Right];
        for i in 0..self.alpha as usize {
            parity_grids.push(create_parity_grid(&grid, strand_types[i])?);
        }
        return Ok(parity_grids);
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
    let mut parity_grid = Grid::new_empty(grid.get_num_items(), grid.get_height())?;
    for x in 0..grid.get_width() {
        for y in 0..grid.get_height() {
            let pos = Pos::new(x, y);
            if let Some(cell) = grid.try_get_cell(pos) {
                let next_pos = pos + strand_type;
                if let Some(pair) = grid.try_get_cell(next_pos) {
                    parity_grid.set_cell(pos, entangle_chunks(cell, pair));
                } else {
                    // we need LW size. At the moment we assume it's square with side equal to grid's height
                    let lw_size = grid.get_height();
                    // calculate the number of steps to go along the strand
                    let steps = lw_size - (pos.x as usize % lw_size);
                    let pair = grid.get_cell(pos.near(strand_type.into(), steps));
                    parity_grid.set_cell(pos, entangle_chunks(cell, pair));
                }
            }
        }
    }
    Ok(ParityGrid {
        grid: parity_grid,
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
        assert_eq!(g.get_cell(Pos::new(0, 0)), &entangle_by_pos(0, 0, dy));
        assert_eq!(g.get_cell(Pos::new(0, 1)), &entangle_by_pos(0, 1, dy));
        assert_eq!(g.get_cell(Pos::new(0, 2)), &entangle_by_pos(0, 2, dy));
        assert_eq!(g.get_cell(Pos::new(1, 0)), &entangle_by_pos(1, 0, dy));
        assert_eq!(g.get_cell(Pos::new(1, 1)), &entangle_by_pos(1, 1, dy));
        assert_eq!(g.get_cell(Pos::new(1, 2)), &entangle_by_pos(1, 2, dy));
        assert_eq!(g.get_cell(Pos::new(2, 0)), &entangle_by_pos(2, 0, dy));
        assert_eq!(g.get_cell(Pos::new(2, 1)), &entangle_by_pos(2, 1, dy));
        assert_eq!(g.get_cell(Pos::new(2, 2)), &entangle_by_pos(2, 2, dy));
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
        let parities = executer.execute(grid.clone()).unwrap();

        assert_eq!(grid.get_cell(Pos::new(0, 0)), &Bytes::from("a"));
        assert_eq!(grid.get_cell(Pos::new(0, 1)), &Bytes::from("b"));
        assert_eq!(grid.get_cell(Pos::new(0, 2)), &Bytes::from("c"));
        assert_eq!(grid.get_cell(Pos::new(1, 0)), &Bytes::from("d"));
        assert_eq!(grid.get_cell(Pos::new(1, 1)), &Bytes::from("e"));
        assert_eq!(grid.get_cell(Pos::new(1, 2)), &Bytes::from("f"));
        assert_eq!(grid.get_cell(Pos::new(2, 0)), &Bytes::from("g"));
        assert_eq!(grid.get_cell(Pos::new(2, 1)), &Bytes::from("h"));
        assert_eq!(grid.get_cell(Pos::new(2, 2)), &Bytes::from("i"));

        assert_eq!(parities.len(), 3);

        let lh_strand = parities.get(0).unwrap();
        assert_parity_grid(&lh_strand);

        let h_strand = parities.get(1).unwrap();
        assert_parity_grid(&h_strand);

        let rh_strand = parities.get(2).unwrap();
        assert_parity_grid(&rh_strand);
    }

    #[test]
    fn test_execute_with_unaligned_leap_window() {
        let grid = Grid::new(create_chunks(), 4).unwrap();

        let executer = Executer::new(3);
        let parities = executer.execute(grid.clone()).unwrap();

        assert_eq!(grid.get_cell(Pos::new(0, 0)), &Bytes::from("a"));
        assert_eq!(grid.get_cell(Pos::new(0, 1)), &Bytes::from("b"));
        assert_eq!(grid.get_cell(Pos::new(0, 2)), &Bytes::from("c"));
        assert_eq!(grid.get_cell(Pos::new(0, 3)), &Bytes::from("d"));

        assert_eq!(grid.get_cell(Pos::new(1, 0)), &Bytes::from("e"));
        assert_eq!(grid.get_cell(Pos::new(1, 1)), &Bytes::from("f"));
        assert_eq!(grid.get_cell(Pos::new(1, 2)), &Bytes::from("g"));
        assert_eq!(grid.get_cell(Pos::new(1, 3)), &Bytes::from("h"));

        assert_eq!(grid.get_cell(Pos::new(2, 0)), &Bytes::from("i"));

        assert_eq!(parities.len(), 3);

        let lh_strand = parities.get(0).unwrap();
        assert_eq!(lh_strand.strand_type, StrandType::Left);
        assert_eq!(lh_strand.grid.get_cell(Pos::new(0, 0)), &entangle("a", "h"));
        assert_eq!(lh_strand.grid.get_cell(Pos::new(0, 1)), &entangle("b", "e"));
        assert_eq!(lh_strand.grid.get_cell(Pos::new(0, 2)), &entangle("c", "f"));
        assert_eq!(lh_strand.grid.get_cell(Pos::new(0, 3)), &entangle("d", "g"));
        assert_eq!(lh_strand.grid.get_cell(Pos::new(1, 0)), &entangle("e", "b"));
        assert_eq!(lh_strand.grid.get_cell(Pos::new(1, 1)), &entangle("f", "i"));
        assert_eq!(lh_strand.grid.get_cell(Pos::new(1, 2)), &entangle("g", "d"));
        assert_eq!(lh_strand.grid.get_cell(Pos::new(1, 3)), &entangle("h", "a"));
        assert_eq!(lh_strand.grid.get_cell(Pos::new(2, 0)), &entangle("i", "c"));

        let h_strand = parities.get(1).unwrap();
        assert_eq!(h_strand.strand_type, StrandType::Horizontal);
        assert_eq!(h_strand.grid.get_cell(Pos::new(0, 0)), &entangle("a", "e"));
        assert_eq!(h_strand.grid.get_cell(Pos::new(0, 1)), &entangle("b", "f"));
        assert_eq!(h_strand.grid.get_cell(Pos::new(0, 2)), &entangle("c", "g"));
        assert_eq!(h_strand.grid.get_cell(Pos::new(0, 3)), &entangle("d", "h"));
        assert_eq!(h_strand.grid.get_cell(Pos::new(1, 0)), &entangle("e", "i"));
        assert_eq!(h_strand.grid.get_cell(Pos::new(1, 1)), &entangle("f", "b"));
        assert_eq!(h_strand.grid.get_cell(Pos::new(1, 2)), &entangle("g", "c"));
        assert_eq!(h_strand.grid.get_cell(Pos::new(1, 3)), &entangle("h", "d"));
        assert_eq!(h_strand.grid.get_cell(Pos::new(2, 0)), &entangle("i", "a"));

        let rh_strand = parities.get(2).unwrap();
        assert_eq!(rh_strand.strand_type, StrandType::Right);
        assert_eq!(rh_strand.grid.get_cell(Pos::new(0, 0)), &entangle("a", "f"));
        assert_eq!(rh_strand.grid.get_cell(Pos::new(0, 1)), &entangle("b", "g"));
        assert_eq!(rh_strand.grid.get_cell(Pos::new(0, 2)), &entangle("c", "h"));
        assert_eq!(rh_strand.grid.get_cell(Pos::new(0, 3)), &entangle("d", "e"));
        assert_eq!(rh_strand.grid.get_cell(Pos::new(1, 0)), &entangle("e", "d"));
        assert_eq!(rh_strand.grid.get_cell(Pos::new(1, 1)), &entangle("f", "a"));
        assert_eq!(rh_strand.grid.get_cell(Pos::new(1, 2)), &entangle("g", "b"));
        assert_eq!(rh_strand.grid.get_cell(Pos::new(1, 3)), &entangle("h", "i"));
        assert_eq!(rh_strand.grid.get_cell(Pos::new(2, 0)), &entangle("i", "c"));
    }

    #[test]
    fn test_execute_with_large_data() {
        // we choose 18 chunks of 8 bytes each and create a grid with 3x6 cells
        // so that there are no holes in the grid and we can safely call get_cell
        let chunks = vec![
            Bytes::from(vec![0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF]),
            Bytes::from(vec![0x10, 0x32, 0x54, 0x76, 0x98, 0xBA, 0xDC, 0xFE]),
            Bytes::from(vec![0xA1, 0xB2, 0xC3, 0xD4, 0xE5, 0xF6, 0x07, 0x18]),
            Bytes::from(vec![0x2A, 0x3B, 0x4C, 0x5D, 0x6E, 0x7F, 0x80, 0x91]),
            Bytes::from(vec![0x19, 0x28, 0x37, 0x46, 0x55, 0x64, 0x73, 0x82]),
            Bytes::from(vec![0x91, 0xA2, 0xB3, 0xC4, 0xD5, 0xE6, 0xF7, 0x08]),
            Bytes::from(vec![0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF, 0x00, 0x11]),
            Bytes::from(vec![0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99]),
            Bytes::from(vec![0xAB, 0xCD, 0xEF, 0x01, 0x23, 0x45, 0x67, 0x89]),
            Bytes::from(vec![0xFE, 0xDC, 0xBA, 0x98, 0x76, 0x54, 0x32, 0x10]),
            Bytes::from(vec![0x13, 0x57, 0x9B, 0xDF, 0x24, 0x68, 0xAC, 0xE0]),
            Bytes::from(vec![0xF1, 0xE2, 0xD3, 0xC4, 0xB5, 0xA6, 0x97, 0x88]),
            Bytes::from(vec![0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88]),
            Bytes::from(vec![0x99, 0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF, 0x00]),
            Bytes::from(vec![0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC, 0xDE, 0xF0]),
            Bytes::from(vec![0x0F, 0x1E, 0x2D, 0x3C, 0x4B, 0x5A, 0x69, 0x78]),
            Bytes::from(vec![0x87, 0x96, 0xA5, 0xB4, 0xC3, 0xD2, 0xE1, 0xF0]),
            Bytes::from(vec![0x1A, 0x2B, 0x3C, 0x4D, 0x5E, 0x6F, 0x70, 0x81]),
        ];
        const HEIGHT: usize = 3;
        const WIDTH: usize = 6;
        let grid = Grid::new(chunks.clone(), HEIGHT).unwrap();

        let executer = Executer::new(3);
        let parities = executer.execute(grid.clone()).unwrap();

        assert_eq!(parities.len(), HEIGHT);

        for parity_grid in parities {
            let assembled_data = parity_grid.grid.assemble_data();
            assert_eq!(assembled_data.len(), WIDTH * HEIGHT * 8);

            for x in 0..WIDTH as i64 {
                for y in 0..HEIGHT as i64 {
                    let pos = Pos::new(x, y);
                    let orig_cell1 = grid.get_cell(pos);
                    let orig_cell2 = grid.get_cell(pos + parity_grid.strand_type);
                    let expected = entangle_chunks(orig_cell1, orig_cell2);
                    let cell = parity_grid.grid.get_cell(pos);
                    assert_eq!(
                        cell,
                        &expected,
                        "Parity grid mismatch at coordinate ({}, {})\n\
                         Parity type: {:?}\n\
                         Actual parity cell value: {:?}\n\
                         Expected parity cell value: {:?}\n\
                         Original grid cell 1 at {:?}: {:?}\n\
                         Original grid cell 2 at {:?}: {:?}",
                        x,
                        y,
                        parity_grid.strand_type,
                        cell,
                        expected,
                        pos,
                        orig_cell1,
                        pos + parity_grid.strand_type,
                        orig_cell2
                    );
                }
            }
        }
    }
}
