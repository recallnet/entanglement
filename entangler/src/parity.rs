// Copyright 2024 Entanglement Contributors
// SPDX-License-Identifier: Apache-2.0, MIT

use crate::grid::{Dir, Grid, Pos};
use serde::{Deserialize, Serialize};
use std;

/// Enum representing the direction of a strand.
/// Strand is bi-directional. `Dir` should be used if the direction matters.
/// `StrandType` is converted to `Dir` when needed which by default represents the forward direction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum StrandType {
    Left,
    Horizontal,
    Right,
}

impl StrandType {
    /// Turns the strand type into an integer that can be used for navigation in the grid.
    pub fn to_i64(self) -> i64 {
        match self {
            StrandType::Left => -1,
            StrandType::Horizontal => 0,
            StrandType::Right => 1,
        }
    }

    /// Turns the strand type into an integer that can be used indexing into a vector.
    /// This is useful for storing the strand type in a vector.
    /// The order is: Left, Horizontal, Right.
    pub fn to_index(self) -> usize {
        match self {
            StrandType::Left => 0,
            StrandType::Horizontal => 1,
            StrandType::Right => 2,
        }
    }

    /// Converts an index into a strand type.
    /// This is useful for converting a vector index into a strand type.
    /// The order is: Left, Horizontal, Right.
    pub fn try_from_index(index: usize) -> Option<Self> {
        match index {
            0 => Some(StrandType::Left),
            1 => Some(StrandType::Horizontal),
            2 => Some(StrandType::Right),
            _ => None,
        }
    }

    /// Returns the opposite (backward) direction of the current strand.
    pub fn to_opposite_dir(self) -> Dir {
        Dir::from(self).opposite()
    }
}

impl From<StrandType> for &str {
    fn from(val: StrandType) -> &'static str {
        match val {
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

/// A grid with a strand type.
pub struct ParityGrid {
    pub grid: Grid,
    pub strand_type: StrandType,
}
