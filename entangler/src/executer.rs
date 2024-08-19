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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::grid::Grid;
    use bytes::Bytes;
    use std::str;

    const WIDTH: usize = 3;
    const HEIGHT: usize = 3;

    fn create_chunks() -> Vec<Bytes> {
        let data = vec![
            Bytes::from("a"),
            Bytes::from("b"),
            Bytes::from("c"),
            Bytes::from("d"),
            Bytes::from("e"),
            Bytes::from("f"),
            Bytes::from("g"),
            Bytes::from("h"),
            Bytes::from("i"),
        ];
        data
    }

    fn entangle(s1: &str, s2: &str) -> Bytes {
        entangle_chunks(&Bytes::from(s1.to_string()), &Bytes::from(s2.to_string()))
    }

    fn ent_by_ind(x: usize, y: usize, dy: i64) -> Bytes {
        const CHARS: &str = "abcdefghi";
        let pos1 = x * HEIGHT + y;
        let y2 = ((y + HEIGHT) as i64 + dy) % HEIGHT as i64;
        let pos2 = ((x + 1) % WIDTH) * HEIGHT + y2 as usize;
        let ch1 = &CHARS[pos1..pos1 + 1];
        let ch2 = &CHARS[pos2..pos2 + 1];
        entangle(ch1, ch2)
    }

    fn assert_parity_grid(parity_grid: &ParityGrid) {
        let dy = parity_grid.strand_type.to_i64();

        let g = &parity_grid.grid;
        assert_eq!(g.get_cell(0, 0), &ent_by_ind(0, 0, dy));
        assert_eq!(g.get_cell(0, 1), &ent_by_ind(0, 1, dy));
        assert_eq!(g.get_cell(0, 2), &ent_by_ind(0, 2, dy));
        assert_eq!(g.get_cell(1, 0), &ent_by_ind(1, 0, dy));
        assert_eq!(g.get_cell(1, 1), &ent_by_ind(1, 1, dy));
        assert_eq!(g.get_cell(1, 2), &ent_by_ind(1, 2, dy));
        assert_eq!(g.get_cell(2, 0), &ent_by_ind(2, 0, dy));
        assert_eq!(g.get_cell(2, 1), &ent_by_ind(2, 1, dy));
        assert_eq!(g.get_cell(2, 2), &ent_by_ind(2, 2, dy));
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
        let grid = Grid::new(create_chunks(), 3, 3).unwrap();

        let parity_grid = create_parity_grid(&grid, StrandType::Left).unwrap();
        assert_parity_grid(&parity_grid);

        let parity_grid = create_parity_grid(&grid, StrandType::Horizontal).unwrap();
        assert_parity_grid(&parity_grid);

        let parity_grid = create_parity_grid(&grid, StrandType::Right).unwrap();
        assert_parity_grid(&parity_grid);
    }

    #[test]
    fn test_execute() {
        let grid = Grid::new(create_chunks(), 3, 3).unwrap();

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
}
