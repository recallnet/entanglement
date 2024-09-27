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

    pub fn to_opposite_dir(self) -> Dir {
        Dir::from(self).opposite()
    }
}

impl Into<&str> for StrandType {
    fn into(self) -> &'static str {
        match self {
            StrandType::Left => "Left",
            StrandType::Horizontal => "Horizontal",
            StrandType::Right => "Right",
        }
    }
}

impl From<StrandType> for Dir {
    fn from(strand_type: StrandType) -> Self {
        match strand_type {
            StrandType::Left => Dir::UR,
            StrandType::Horizontal => Dir::R,
            StrandType::Right => Dir::DR,
        }
    }
}

impl From<Dir> for StrandType {
    fn from(dir: Dir) -> Self {
        match dir {
            Dir::UR => StrandType::Left,
            Dir::R => StrandType::Horizontal,
            Dir::DR => StrandType::Right,
            Dir::UL => StrandType::Right,
            Dir::L => StrandType::Horizontal,
            Dir::DL => StrandType::Left,
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
