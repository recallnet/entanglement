// Copyright 2024 Entanglement Contributors
// SPDX-License-Identifier: Apache-2.0, MIT

use crate::grid::{Dir, Grid, Pos};
use serde::{Deserialize, Serialize};
use std;

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

impl Into<Dir> for StrandType {
    fn into(self) -> Dir {
        match self {
            StrandType::Left => Dir::UR,
            StrandType::Horizontal => Dir::R,
            StrandType::Right => Dir::DR,
        }
    }
}

impl std::ops::Add<StrandType> for Pos {
    type Output = Pos;

    fn add(self, rhs: StrandType) -> Self::Output {
        Pos {
            x: self.x + 1,
            y: self.y + rhs.to_i64(),
        }
    }
}

impl std::ops::Sub<StrandType> for Pos {
    type Output = Pos;

    fn sub(self, rhs: StrandType) -> Self::Output {
        Pos {
            x: self.x - 1,
            y: self.y - rhs.to_i64(),
        }
    }
}

impl std::ops::AddAssign<StrandType> for Pos {
    fn add_assign(&mut self, rhs: StrandType) {
        self.x += 1;
        self.y += rhs.to_i64();
    }
}

impl std::ops::SubAssign<StrandType> for Pos {
    fn sub_assign(&mut self, rhs: StrandType) {
        self.x -= 1;
        self.y -= rhs.to_i64();
    }
}

pub struct ParityGrid {
    pub grid: Grid,
    pub strand_type: StrandType,
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
