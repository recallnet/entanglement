// Copyright 2024 Entanglement Contributors
// SPDX-License-Identifier: Apache-2.0, MIT

use crate::grid::Grid;
use bytes::Bytes;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
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

pub struct ParityGrid {
    pub grid: Grid,
    pub strand_type: StrandType,
}

impl ParityGrid {
    pub fn get_pair_at(&self, x: u64, y: u64) -> Option<(&Bytes, &Bytes)> {
        match self.grid.try_get_cell(x as i64, y as i64) {
            Some(cell) => {
                for i in 1..self.grid.get_height() as i64 + 1 {
                    let prev_x = x as i64 - i;
                    let prev_y = y as i64 - self.strand_type.to_i64() * i;
                    if let Some(prev_cell) = self.grid.try_get_cell(prev_x, prev_y) {
                        return Some((prev_cell, cell));
                    }
                }
                None
            }
            None => None,
        }
    }

    pub fn get_pair_for(&self, index: u64) -> Option<(&Bytes, &Bytes)> {
        let x = index / self.grid.get_height() as u64;
        let y = index % self.grid.get_height() as u64;
        self.get_pair_at(x, y)
    }
}

pub struct Lattice {
    parity_grids: Vec<ParityGrid>,
}

impl Lattice {
    pub fn new(parity_grids: Vec<ParityGrid>) -> Self {
        Self { parity_grids }
    }

    pub fn get_parities(&self) -> &Vec<ParityGrid> {
        &self.parity_grids
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_ag_chunks() -> Vec<Bytes> {
        vec![
            Bytes::from("a"),
            Bytes::from("b"),
            Bytes::from("c"),
            Bytes::from("d"),
            Bytes::from("e"),
            Bytes::from("f"),
            Bytes::from("g"),
        ]
    }

    struct TestCase {
        coord: (u64, u64),
        expected: Option<(&'static str, &'static str)>,
    }

    fn assert_get_pair(test_cases: Vec<TestCase>, parity_grid: ParityGrid) {
        for t in test_cases {
            match t.expected {
                Some((prev, next)) => {
                    assert_eq!(
                        parity_grid.get_pair_at(t.coord.0, t.coord.1),
                        Some((&Bytes::from(prev), &Bytes::from(next))),
                        "expected pair match at ({}, {})",
                        t.coord.0,
                        t.coord.1
                    );
                }
                None => {
                    assert_eq!(parity_grid.get_pair_at(t.coord.0, t.coord.1), None);
                }
            }

            let index = t.coord.0 * parity_grid.grid.get_height() as u64 + t.coord.1;
            match t.expected {
                Some((prev, next)) => {
                    assert_eq!(
                        parity_grid.get_pair_for(index),
                        Some((&Bytes::from(prev), &Bytes::from(next))),
                        "expected pair match for {}",
                        index
                    );
                }
                None => {
                    assert_eq!(parity_grid.get_pair_at(t.coord.0, t.coord.1), None);
                }
            }
        }
    }

    #[test]
    fn test_parity_grid_get_pair_for_h_strand() {
        let parity_grid = ParityGrid {
            grid: Grid::new(create_ag_chunks(), 3).unwrap(),
            strand_type: StrandType::Horizontal,
        };

        // a d g
        // b e .
        // c f .

        let test_cases = vec![
            TestCase {
                coord: (0, 0),
                expected: Some(("g", "a")),
            },
            TestCase {
                coord: (0, 1),
                expected: Some(("e", "b")),
            },
            TestCase {
                coord: (0, 2),
                expected: Some(("f", "c")),
            },
            TestCase {
                coord: (1, 0),
                expected: Some(("a", "d")),
            },
            TestCase {
                coord: (1, 1),
                expected: Some(("b", "e")),
            },
            TestCase {
                coord: (1, 2),
                expected: Some(("c", "f")),
            },
            TestCase {
                coord: (2, 0),
                expected: Some(("d", "g")),
            },
            TestCase {
                coord: (2, 1),
                expected: None,
            },
            TestCase {
                coord: (2, 2),
                expected: None,
            },
        ];

        assert_get_pair(test_cases, parity_grid);
    }

    #[test]
    fn test_parity_grid_get_pair_for_l_strand() {
        let parity_grid = ParityGrid {
            grid: Grid::new(create_ag_chunks(), 4).unwrap(),
            strand_type: StrandType::Left,
        };

        // a e . .
        // b f . .
        // c g . .
        // d . . .

        let test_cases = vec![
            TestCase {
                coord: (0, 0),
                expected: Some(("a", "a")),
            },
            TestCase {
                coord: (0, 1),
                expected: Some(("e", "b")),
            },
            TestCase {
                coord: (0, 2),
                expected: Some(("f", "c")),
            },
            TestCase {
                coord: (0, 3),
                expected: Some(("g", "d")),
            },
            TestCase {
                coord: (1, 0),
                expected: Some(("b", "e")),
            },
            TestCase {
                coord: (1, 1),
                expected: Some(("c", "f")),
            },
            TestCase {
                coord: (1, 2),
                expected: Some(("d", "g")),
            },
            TestCase {
                coord: (1, 3),
                expected: None,
            },
            TestCase {
                coord: (2, 0),
                expected: None,
            },
            TestCase {
                coord: (2, 1),
                expected: None,
            },
            TestCase {
                coord: (2, 2),
                expected: None,
            },
            TestCase {
                coord: (2, 3),
                expected: None,
            },
            TestCase {
                coord: (3, 0),
                expected: None,
            },
            TestCase {
                coord: (3, 1),
                expected: None,
            },
            TestCase {
                coord: (3, 2),
                expected: None,
            },
            TestCase {
                coord: (3, 3),
                expected: None,
            },
        ];

        assert_get_pair(test_cases, parity_grid);
    }

    #[test]
    fn test_parity_grid_get_pair_for_r_strand() {
        let parity_grid = ParityGrid {
            grid: Grid::new(create_ag_chunks(), 3).unwrap(),
            strand_type: StrandType::Right,
        };

        // a d g
        // b e .
        // c f .

        let test_cases = vec![
            TestCase {
                coord: (0, 0),
                expected: Some(("e", "a")),
            },
            TestCase {
                coord: (0, 1),
                expected: Some(("g", "b")),
            },
            TestCase {
                coord: (0, 2),
                expected: Some(("d", "c")),
            },
            TestCase {
                coord: (1, 0),
                expected: Some(("c", "d")),
            },
            TestCase {
                coord: (1, 1),
                expected: Some(("a", "e")),
            },
            TestCase {
                coord: (1, 2),
                expected: Some(("b", "f")),
            },
            TestCase {
                coord: (2, 0),
                expected: Some(("f", "g")),
            },
            TestCase {
                coord: (2, 1),
                expected: None,
            },
            TestCase {
                coord: (2, 2),
                expected: None,
            },
        ];

        assert_get_pair(test_cases, parity_grid);
    }
}
