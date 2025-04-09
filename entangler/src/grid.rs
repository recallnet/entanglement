// Copyright 2024 Entanglement Contributors
// SPDX-License-Identifier: Apache-2.0, MIT

/// Direction enum for moving around the grid.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Dir {
    UL, // Up-left
    UR, // Up-right
    L,  // Left
    R,  // Right
    DL, // Down-left
    DR, // Down-right
}

impl Dir {
    pub fn all() -> [Dir; 6] {
        [Dir::UL, Dir::UR, Dir::L, Dir::R, Dir::DL, Dir::DR]
    }

    pub fn to_i64(self) -> (i64, i64) {
        match self {
            Dir::UL => (-1, -1),
            Dir::UR => (1, -1),
            Dir::L => (-1, 0),
            Dir::R => (1, 0),
            Dir::DL => (-1, 1),
            Dir::DR => (1, 1),
        }
    }

    pub fn opposite(&self) -> Dir {
        match self {
            Dir::UL => Dir::DR,
            Dir::UR => Dir::DL,
            Dir::L => Dir::R,
            Dir::R => Dir::L,
            Dir::DL => Dir::UR,
            Dir::DR => Dir::UL,
        }
    }

    pub fn is_forward(&self) -> bool {
        match self {
            Dir::UL | Dir::L | Dir::DL => false,
            Dir::UR | Dir::R | Dir::DR => true,
        }
    }
}

/// Position struct for representing a position in the grid.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Pos {
    pub x: i64,
    pub y: i64,
}

impl<T, U> From<(T, U)> for Pos
where
    T: TryInto<i64>,
    U: TryInto<i64>,
{
    fn from(coords: (T, U)) -> Self {
        Self {
            x: coords.0.try_into().unwrap_or(i64::MAX),
            y: coords.1.try_into().unwrap_or(i64::MAX),
        }
    }
}

impl Pos {
    /// Returns the position adjacent to the current position in the given direction.
    pub fn adjacent(&self, dir: Dir) -> Pos {
        let (dx, dy) = dir.to_i64();
        Pos {
            x: self.x + dx,
            y: self.y + dy,
        }
    }

    /// Returns the position at a given distance in the given direction.
    pub fn near(&self, dir: Dir, distance: u64) -> Pos {
        let (dx, dy) = dir.to_i64();
        Pos {
            x: self.x + dx * distance as i64,
            y: self.y + dy * distance as i64,
        }
    }

    /// Returns the direction from the current position to the given position.
    /// Returns `None` if the positions are not adjacent along available directions.
    pub fn dir_to(&self, other: Pos) -> Option<Dir> {
        let dx = other.x - self.x;
        let dy = other.y - self.y;
        match (dx, dy) {
            (-1, -1) => Some(Dir::UL),
            (1, -1) => Some(Dir::UR),
            (-1, 0) => Some(Dir::L),
            (1, 0) => Some(Dir::R),
            (-1, 1) => Some(Dir::DL),
            (1, 1) => Some(Dir::DR),
            _ => None,
        }
    }
}

impl std::ops::Add<Dir> for Pos {
    type Output = Pos;

    fn add(self, rhs: Dir) -> Self::Output {
        let dir = rhs.to_i64();
        Pos {
            x: self.x + dir.0,
            y: self.y + dir.1,
        }
    }
}

impl std::ops::Sub<Dir> for Pos {
    type Output = Pos;

    fn sub(self, rhs: Dir) -> Self::Output {
        let dir = rhs.to_i64();
        Pos {
            x: self.x - dir.0,
            y: self.y - dir.1,
        }
    }
}

impl Pos {
    pub fn new<T: TryInto<i64>, U: TryInto<i64>>(x: T, y: U) -> Self {
        (x, y).into()
    }
}

/// Positioner is responsible for providing valid positions in the grid given it's dimensions.
/// It can handle out-of-bound positions and wrap them around to the other side of the grid.
///
/// The position are calculated assuming height of the grid is fixed but width is not and depends
/// on the number of items. The Grid is assumed to be leap-window aligned (LW), meaning that when
/// wrapping along x axis, the grid will ignore the number of actual items (or available columns)
/// and calculate the wrapping index based on the number of items that would be present if the
/// grid was fully populated.
///   Example:
///    - Grid with 7 items and height 3 would have 3 columns and 3 rows. Here the wrapping is
///      straightforward, as number of columns, 3, is equal to LW width
///    - Grid with 7 items and height 4 would have 2 columns and 4 rows. Here we have 2 columns
///      and LW width = 4. So, when wrapping, the grid will consider 4 columns and wrap around to
///      the first column.
#[derive(Debug, Clone, Copy)]
pub struct Positioner {
    height: u64,
    num_items: u64,
    lw_aligned_width: u64,
}

impl Positioner {
    pub fn new(height: u64, num_items: u64) -> Self {
        let lw_aligned_width = Self::calculate_lw_aligned_width(num_items, height);
        Self {
            height,
            num_items,
            lw_aligned_width,
        }
    }

    fn calculate_lw_aligned_width(num_items: u64, height: u64) -> u64 {
        let lw = height * height;
        num_items.div_ceil(lw) * height
    }

    /// Normalizes the given position by making sure out-of-bounds position is wrapped along axises.
    pub fn normalize(&self, pos: Pos) -> Pos {
        Pos::new(self.mod_x(pos.x) as i64, self.mod_y(pos.y) as i64)
    }

    fn mod_x(&self, x: i64) -> u64 {
        mod_int(x, self.lw_aligned_width)
    }

    fn mod_y(&self, y: i64) -> u64 {
        mod_int(y, self.height)
    }

    pub fn pos_to_index(&self, pos: Pos) -> u64 {
        self.mod_x(pos.x) * self.height + self.mod_y(pos.y)
    }

    pub fn index_to_pos(&self, index: u64) -> Pos {
        Pos::new(index / self.height, index % self.height)
    }

    /// Returns true if the given position is available. Even if the position is normalized it might
    /// be missing from the grid because LW alignment might leave empty spots in the end of the
    /// grid.
    pub fn is_pos_available(&self, pos: Pos) -> bool {
        let normalized = self.normalize(pos);
        (normalized.x as u64 * self.height + normalized.y as u64) < self.num_items
    }

    /// Returns a direction from one position to another taking in to account potentially
    /// adjacent position, but on the opposite sides of a grid.
    /// For example if grid has with 9, then pos (0, 0) and (8, 1) are adjacent.
    pub fn determine_dir(&self, from: Pos, mut to: Pos) -> Option<Dir> {
        match from.dir_to(to) {
            Some(dir) => Some(dir),
            None => {
                if from.x.abs_diff(to.x) > 1 {
                    if from.x == 0 {
                        to.x -= self.lw_aligned_width as i64;
                    } else if to.x == 0 {
                        to.x += self.lw_aligned_width as i64;
                    }
                }

                if from.y.abs_diff(to.y) > 1 {
                    if from.y == 0 {
                        to.y -= self.height as i64;
                    } else if to.y == 0 {
                        to.y += self.height as i64;
                    }
                }
                from.dir_to(to)
            }
        }
    }

    pub fn get_num_items(&self) -> u64 {
        self.num_items
    }

    pub fn get_height(&self) -> u64 {
        self.height
    }
}

fn mod_int(int: i64, m: u64) -> u64 {
    let w = m as i64;
    ((int % w + w) % w) as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_positioner_determine_dir() {
        let p = Positioner::new(4, 32);

        let test_cases = vec![
            // Standard cases
            ((0, 0), (1, 0), Some(Dir::R), "Standard right move"),
            ((1, 1), (0, 1), Some(Dir::L), "Standard left move"),
            ((1, 1), (2, 0), Some(Dir::UR), "Standard up-right move"),
            ((1, 1), (0, 2), Some(Dir::DL), "Standard down-left move"),
            // Horizontal wrapping
            ((7, 1), (0, 1), Some(Dir::R), "Wrap right horizontally"),
            ((0, 1), (7, 1), Some(Dir::L), "Wrap left at horizontal edge"),
            // Vertical wrapping
            ((1, 3), (2, 0), Some(Dir::DR), "Wrap down-right vertically"),
            ((2, 0), (1, 3), Some(Dir::UL), "Wrap up-left vertically"),
            ((1, 3), (0, 0), Some(Dir::DL), "Wrap down-left vertically"),
            ((0, 0), (1, 3), Some(Dir::UR), "Wrap up-right vertically"),
            // Corner wrapping
            ((7, 3), (0, 0), Some(Dir::DR), "Wrap down-right at corner"),
            ((0, 0), (7, 3), Some(Dir::UL), "Wrap up-left at corner"),
            ((0, 3), (7, 0), Some(Dir::DL), "Wrap down-left at corner"),
            ((7, 0), (0, 3), Some(Dir::UR), "Wrap up-right at corner"),
            // Edge cases
            ((1, 1), (1, 1), None, "No movement"),
            ((1, 1), (10, 10), None, "Outside valid range"),
            ((2, 2), (2, 4), None, "Not neighboring cells"),
        ];

        for (from, to, expected_dir, message) in test_cases {
            assert_eq!(
                p.determine_dir(Pos::new(from.0, from.1), Pos::new(to.0, to.1)),
                expected_dir,
                "{}",
                message
            );
        }
    }
}
