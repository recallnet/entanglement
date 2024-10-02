// Copyright 2024 Entanglement Contributors
// SPDX-License-Identifier: Apache-2.0, MIT

use anyhow::Result;
use bytes::Bytes;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Invalid height: {0}")]
    InvalidHeight(usize),
    #[error("Can not create grid with height {1} out of {0} items")]
    InvalidNumItems(usize, usize),
}

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
    pub fn near(&self, dir: Dir, distance: usize) -> Pos {
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
    pub(crate) height: usize,
    num_items: usize,
    lw_aligned_width: usize,
}

impl Positioner {
    pub fn new(height: usize, num_items: usize) -> Self {
        let lw_aligned_width = Self::calculate_lw_aligned_width(num_items, height);
        Self {
            height,
            num_items,
            lw_aligned_width,
        }
    }

    fn calculate_lw_aligned_width(num_items: usize, height: usize) -> usize {
        let lw = height * height;
        ((num_items + lw - 1) / lw) * height
    }

    /// Normalizes the given position by making sure out-of-bounds position is wrapped along axises.
    pub fn normalize(&self, pos: Pos) -> Pos {
        Pos::new(self.mod_x(pos.x) as i64, self.mod_y(pos.y) as i64)
    }

    fn mod_x(&self, x: i64) -> usize {
        mod_int(x, self.lw_aligned_width)
    }

    fn mod_y(&self, y: i64) -> usize {
        mod_int(y, self.height)
    }

    /// Returns true if the given position is available. Even is a position is normalized it might
    /// be missing from the grid because LW alignment might leave empty spots in the end of the
    /// grid.
    pub fn is_pos_available(&self, pos: Pos) -> bool {
        let normalized = self.normalize(pos);
        (normalized.x as usize * self.height + normalized.y as usize) < self.num_items
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
}

fn mod_int(int: i64, m: usize) -> usize {
    let w = m as i64;
    ((int % w + w) % w) as usize
}

/// Grid represents a 2D grid of chunks arranged in column-first order.
/// The grid is wrapped in both dimensions, so that negative or out-of-bound indices wrap
/// around to the end or a side of the grid.
///
/// The Grid is leap window (LW) aligned, meaning that when wrapping along x axis, the grid
/// will ignore the number of actual items (or available columns) and calculate the wrapping
/// index based on the number of items that would be present if the grid was fully populated.
///   Example:
///    - Grid with 7 items and height 3 would have 3 columns and 3 rows. Here the wrapping is
///      straightforward, as number of columns, 3, is equal to LW width
///    - Grid with 7 items and height 4 would have 2 columns and 4 rows. Here we have 2 columns
///      and LW width = 4. So, when wrapping, the grid will consider 4 columns and wrap around to
///      the first column.
#[derive(Debug, Clone)]
pub struct Grid {
    data: Vec<Vec<Bytes>>,
    positioner: Positioner,
}

impl Grid {
    pub fn new(data: Vec<Bytes>, height: usize) -> Result<Self, Error> {
        let num_items = data.len();
        Ok(Self {
            data: build_column_first_grid(data, height)?,
            positioner: Positioner::new(height, num_items),
        })
    }

    /// Creates a new empty grid for `num_items` items arranged in `height`-sized columns.
    pub fn new_empty(num_items: usize, height: usize) -> Result<Self, Error> {
        if height == 0 {
            return Err(Error::InvalidHeight(height));
        }
        if num_items < height {
            return Err(Error::InvalidNumItems(num_items, height));
        }

        let num_cols = (num_items + height - 1) / height;
        let mut grid: Vec<Vec<Bytes>> = Vec::with_capacity(num_cols);
        for _ in 0..num_cols {
            grid.push(vec![Bytes::new(); height]);
        }

        Ok(Self {
            data: grid,
            positioner: Positioner::new(height, num_items),
        })
    }

    /// Converts item index into a position
    pub fn index_to_pos(&self, index: usize) -> Pos {
        Pos {
            x: (index / self.get_height()) as i64,
            y: (index % self.get_height()) as i64,
        }
    }

    /// Sets the value of a cell at the given position.
    pub fn set_cell(&mut self, pos: Pos, value: Bytes) {
        let norm_pos = self.positioner.normalize(pos);
        self.data[norm_pos.x as usize][norm_pos.y as usize] = value;
    }

    /// Gets a cell at the given position.
    /// If cell at the given position doesn't exist, it will panic.
    /// If there is uncertainty if the cell at the given position exists, `try_get_cell`
    /// method should be used.
    pub fn get_cell(&self, pos: Pos) -> &Bytes {
        let norm_pos = self.positioner.normalize(pos);
        &self.data[norm_pos.x as usize][norm_pos.y as usize]
    }

    /// Gets a cell at the given position is it exists. Return `None` otherwise.
    pub fn try_get_cell(&self, pos: Pos) -> Option<&Bytes> {
        if self.has_cell(pos) {
            Some(self.get_cell(pos))
        } else {
            None
        }
    }

    /// Returns the width of the grid, i.e., the number of actual columns.
    pub fn get_width(&self) -> usize {
        self.data.len()
    }

    /// Returns the height of the grid, i.e., the number of rows in each column.
    pub fn get_height(&self) -> usize {
        self.data.first().map_or(0, |col| col.len())
    }

    /// Returns true if cell at the given position exists. Can be used before calling
    /// `get_cell` to make sure it won't panic.
    pub fn has_cell(&self, pos: Pos) -> bool {
        self.positioner.is_pos_available(pos)
    }

    /// Assembles the grid data into a single Bytes object.
    pub fn assemble_data(&self) -> Bytes {
        let mut data = Vec::new();
        for col in &self.data {
            for cell in col {
                data.extend_from_slice(cell.as_ref());
            }
        }
        Bytes::from(data)
    }

    /// Returns a number of items the grid deals with.
    pub fn get_num_items(&self) -> usize {
        self.positioner.num_items
    }
}

fn build_column_first_grid(data: Vec<Bytes>, height: usize) -> Result<Vec<Vec<Bytes>>, Error> {
    if height == 0 {
        return Err(Error::InvalidHeight(height));
    }
    if data.len() < height {
        return Err(Error::InvalidNumItems(data.len(), height));
    }

    let num_cols = (data.len() + height - 1) / height;
    let mut grid: Vec<Vec<Bytes>> = Vec::with_capacity(num_cols);
    for _ in 0..num_cols {
        grid.push(Vec::with_capacity(height));
    }
    for (i, datum) in data.into_iter().enumerate() {
        grid[i / height].push(datum);
    }

    Ok(grid)
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

    #[test]
    fn test_grid_new() {
        let data = vec![
            Bytes::from("a"),
            Bytes::from("b"),
            Bytes::from("c"),
            Bytes::from("d"),
        ];
        let grid = Grid::new(data.clone(), 2).unwrap();
        assert_eq!(grid.get_width(), 2, "Grid width should be 2");
        assert_eq!(grid.get_height(), 2, "Grid height should be 2");
        assert_eq!(grid.get_num_items(), 4, "Grid should have 4 items");
        assert_eq!(
            grid.get_cell((0, 0).into()),
            &data[0],
            "Cell (0, 0) should contain 'a'"
        );
        assert_eq!(
            grid.get_cell((0, 1).into()),
            &data[1],
            "Cell (0, 1) should contain 'b'"
        );
        assert_eq!(
            grid.get_cell((1, 0).into()),
            &data[2],
            "Cell (1, 0) should contain 'c'"
        );
        assert_eq!(
            grid.get_cell((1, 1).into()),
            &data[3],
            "Cell (1, 1) should contain 'd'"
        );
    }

    #[test]
    fn if_input_is_empty_error() {
        let result = Grid::new(vec![], 2);
        assert!(result.is_err(), "Expected an error for empty input");
        assert!(matches!(result.unwrap_err(), Error::InvalidNumItems(0, 2)));
    }

    #[test]
    fn if_fewer_chunks_than_height_error() {
        let result = Grid::new(vec![Bytes::from("a"), Bytes::from("b")], 3);
        assert!(result.is_err(), "Expected an error");
        assert!(matches!(result.unwrap_err(), Error::InvalidNumItems(2, 3)));

        let result = Grid::new(vec![Bytes::from("a")], 3);
        assert!(result.is_err(), "Expected an error");
        assert!(matches!(result.unwrap_err(), Error::InvalidNumItems(1, 3)));
    }

    #[test]
    fn if_invalid_height_error() {
        let data = vec![Bytes::from("a"), Bytes::from("b")];
        let result = Grid::new(data, 0);
        assert!(result.is_err(), "Expected an error for invalid height");
        assert!(matches!(result.unwrap_err(), Error::InvalidHeight(0)));
    }

    #[test]
    fn test_grid_get_cell() {
        let grid = Grid::new(create_ag_chunks(), 3).unwrap();

        assert_eq!(grid.get_cell(Pos::new(0, 0)), &Bytes::from("a"));
        assert_eq!(grid.get_cell(Pos::new(0, 1)), &Bytes::from("b"));
        assert_eq!(grid.get_cell(Pos::new(0, 2)), &Bytes::from("c"));
        assert_eq!(grid.get_cell(Pos::new(1, 0)), &Bytes::from("d"));
        assert_eq!(grid.get_cell(Pos::new(1, 1)), &Bytes::from("e"));
        assert_eq!(grid.get_cell(Pos::new(1, 2)), &Bytes::from("f"));
        assert_eq!(grid.get_cell(Pos::new(2, 0)), &Bytes::from("g"));

        // Test wrapping
        assert_eq!(grid.get_cell(Pos::new(3, 0)), &Bytes::from("a"));
        assert_eq!(grid.get_cell(Pos::new(-1, 0)), &Bytes::from("g"));
        assert_eq!(grid.get_cell(Pos::new(0, -1)), &Bytes::from("c"));
    }

    #[test]
    fn test_grid_get_cell_incomplete_lw() {
        let grid = Grid::new(create_ag_chunks(), 4).unwrap();

        assert!(grid.has_cell(Pos::new(0, 0)));
        assert!(grid.has_cell(Pos::new(0, 1)));
        assert!(grid.has_cell(Pos::new(0, 2)));
        assert!(grid.has_cell(Pos::new(0, 3)));
        assert!(grid.has_cell(Pos::new(1, 0)));
        assert!(grid.has_cell(Pos::new(1, 1)));
        assert!(grid.has_cell(Pos::new(1, 2)));

        assert_eq!(grid.get_cell(Pos::new(0, 0)), &Bytes::from("a"));
        assert_eq!(grid.get_cell(Pos::new(0, 1)), &Bytes::from("b"));
        assert_eq!(grid.get_cell(Pos::new(0, 2)), &Bytes::from("c"));
        assert_eq!(grid.get_cell(Pos::new(0, 3)), &Bytes::from("d"));
        assert_eq!(grid.get_cell(Pos::new(1, 0)), &Bytes::from("e"));
        assert_eq!(grid.get_cell(Pos::new(1, 1)), &Bytes::from("f"));
        assert_eq!(grid.get_cell(Pos::new(1, 2)), &Bytes::from("g"));

        assert!(!grid.has_cell(Pos::new(1, 3)));
        assert!(!grid.has_cell(Pos::new(2, 0)));
        assert!(!grid.has_cell(Pos::new(2, 1)));
        assert!(!grid.has_cell(Pos::new(2, 2)));
        assert!(!grid.has_cell(Pos::new(2, 3)));
        assert!(!grid.has_cell(Pos::new(3, 0)));
        assert!(!grid.has_cell(Pos::new(3, 1)));
        assert!(!grid.has_cell(Pos::new(3, 2)));
        assert!(!grid.has_cell(Pos::new(3, 3)));

        // Test wrapping
        assert!(grid.has_cell(Pos::new(4, 0)));
        assert!(grid.has_cell(Pos::new(5, 0)));
        assert!(grid.has_cell(Pos::new(-3, 2)));
        assert!(grid.has_cell(Pos::new(0, -1)));

        assert_eq!(grid.get_cell(Pos::new(4, 0)), &Bytes::from("a"));
        assert_eq!(grid.get_cell(Pos::new(5, 0)), &Bytes::from("e"));
        assert_eq!(grid.get_cell(Pos::new(-3, 2)), &Bytes::from("g"));
        assert_eq!(grid.get_cell(Pos::new(0, -1)), &Bytes::from("d"));

        assert!(!grid.has_cell(Pos::new(6, 0)));
        assert!(!grid.has_cell(Pos::new(6, 1)));
        assert!(!grid.has_cell(Pos::new(6, 2)));
        assert!(!grid.has_cell(Pos::new(5, 3)));
        assert!(!grid.has_cell(Pos::new(-1, 0)));
        assert!(!grid.has_cell(Pos::new(-2, 0)));
        assert!(!grid.has_cell(Pos::new(-1, 1)));
        assert!(!grid.has_cell(Pos::new(-2, 1)));
        assert!(!grid.has_cell(Pos::new(-1, 2)));
        assert!(!grid.has_cell(Pos::new(-2, 2)));
        assert!(!grid.has_cell(Pos::new(-3, 3)));
        assert!(!grid.has_cell(Pos::new(1, -1)));
    }

    #[test]
    fn test_grid_width_height() {
        let test_cases = vec![(1, 7, 1), (2, 4, 2), (3, 3, 3), (4, 2, 4), (7, 1, 7)];

        for (height, expected_width, expected_height) in test_cases {
            let grid = Grid::new(create_ag_chunks(), height).unwrap();
            assert_eq!(grid.get_width(), expected_width, "Height: {}", height);
            assert_eq!(grid.get_height(), expected_height, "Height: {}", height);
        }
    }

    #[test]
    fn test_grid_try_get_cell() {
        let grid = Grid::new(create_ag_chunks(), 3).unwrap();

        assert_eq!(grid.try_get_cell(Pos::new(0, 0)), Some(&Bytes::from("a")));
        assert_eq!(grid.try_get_cell(Pos::new(0, 1)), Some(&Bytes::from("b")));
        assert_eq!(grid.try_get_cell(Pos::new(0, 2)), Some(&Bytes::from("c")));
        assert_eq!(grid.try_get_cell(Pos::new(1, 0)), Some(&Bytes::from("d")));
        assert_eq!(grid.try_get_cell(Pos::new(1, 1)), Some(&Bytes::from("e")));
        assert_eq!(grid.try_get_cell(Pos::new(1, 2)), Some(&Bytes::from("f")));
        assert_eq!(grid.try_get_cell(Pos::new(2, 0)), Some(&Bytes::from("g")));
        assert_eq!(grid.try_get_cell(Pos::new(2, 1)), None);
        assert_eq!(grid.try_get_cell(Pos::new(2, 2)), None);
    }

    #[test]
    fn test_grid_has_cell() {
        let grid = Grid::new(create_ag_chunks(), 3).unwrap();

        assert!(grid.has_cell(Pos::new(0, 0)));
        assert!(grid.has_cell(Pos::new(0, 1)));
        assert!(grid.has_cell(Pos::new(0, 2)));
        assert!(grid.has_cell(Pos::new(1, 0)));
        assert!(grid.has_cell(Pos::new(1, 1)));
        assert!(grid.has_cell(Pos::new(1, 2)));
        assert!(grid.has_cell(Pos::new(2, 0)));
        assert!(!grid.has_cell(Pos::new(2, 1)));
        assert!(!grid.has_cell(Pos::new(2, 2)));
    }

    #[test]
    fn test_grid_assemble_data() {
        let grid = Grid::new(create_ag_chunks(), 3).unwrap();
        let assembled_data = grid.assemble_data();
        let expected_data = Bytes::from("abcdefg");
        assert_eq!(
            assembled_data, expected_data,
            "Assembled data should be 'abcdefg'"
        );
    }

    #[test]
    fn test_grid_assemble_data_with_big_chunks() {
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
        ];
        let grid = Grid::new(chunks.clone(), 3).unwrap();

        let expected_orig_data = Bytes::from(chunks.concat());
        assert_eq!(grid.assemble_data(), expected_orig_data);
    }

    #[test]
    fn test_grid_new_empty() {
        let mut grid = Grid::new_empty(7, 3).unwrap();
        grid.set_cell(Pos::new(0, 0), Bytes::from("a"));
        grid.set_cell(Pos::new(0, 1), Bytes::from("b"));
        grid.set_cell(Pos::new(0, 2), Bytes::from("c"));
        grid.set_cell(Pos::new(1, 0), Bytes::from("d"));
        grid.set_cell(Pos::new(1, 1), Bytes::from("e"));
        grid.set_cell(Pos::new(1, 2), Bytes::from("f"));
        grid.set_cell(Pos::new(2, 0), Bytes::from("g"));

        assert_eq!(grid.get_width(), 3, "Grid width should be 3");
        assert_eq!(grid.get_height(), 3, "Grid height should be 3");
        assert_eq!(grid.get_num_items(), 7, "Grid should have 7 items");
        assert_eq!(grid.get_cell(Pos::new(0, 0)), &Bytes::from("a"));
        assert_eq!(grid.get_cell(Pos::new(0, 1)), &Bytes::from("b"));
        assert_eq!(grid.get_cell(Pos::new(0, 2)), &Bytes::from("c"));
        assert_eq!(grid.get_cell(Pos::new(1, 0)), &Bytes::from("d"));
        assert_eq!(grid.get_cell(Pos::new(1, 1)), &Bytes::from("e"));
        assert_eq!(grid.get_cell(Pos::new(1, 2)), &Bytes::from("f"));
        assert_eq!(grid.get_cell(Pos::new(2, 0)), &Bytes::from("g"));
    }

    #[test]
    fn test_grid_new_empty_invalid_width() {
        let result = Grid::new_empty(5, 0);
        assert!(result.is_err(), "Expected an error for invalid width");
        assert!(matches!(result.unwrap_err(), Error::InvalidHeight(0)));
    }

    #[test]
    fn test_grid_new_empty_invalid_num_items() {
        let test_cases = vec![(0, 3), (1, 3), (2, 3)];

        for (num_items, num_columns) in test_cases {
            let result = Grid::new_empty(num_items, num_columns);
            assert!(
                result.is_err(),
                "Expected an error for invalid number of items: {} items, {} columns",
                num_items,
                num_columns
            );
            assert!(
                matches!(result.unwrap_err(), Error::InvalidNumItems(i, c) if i == num_items && c == num_columns),
                "Expected InvalidNumItems error for {} items and {} columns",
                num_items,
                num_columns
            );
        }
    }

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
