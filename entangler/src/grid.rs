// Copyright 2024 Entanglement Contributors
// SPDX-License-Identifier: Apache-2.0, MIT

use anyhow::Result;
use bytes::Bytes;
use thiserror;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Invalid width: {0}")]
    InvalidWidth(usize),
    #[error("Input vector is empty")]
    EmptyInput,
    #[error("Invalid number of items: {0}")]
    InvalidNumItems(usize),
}

/// Grid represents a 2D grid of chunks built in column-first order.
/// The grid is wrapped in both dimensions, so that negative or out-of-bound indices wrap
/// around to the end or a side of the grid.
///
/// The Grid is leap window (LW) aligned, meaning that when wrapping along x axis, the grid
/// will ignore the number of actual items (or available columns) and calculate the wrapping
/// index based on the number of items that would be present if the grid was fully populated.
///   Example:
///    - Grid with 7 items and height 3 would have 3 columns and 3 rows. Here the wrapping in
/// straightforward, as number of columns, 3, is equal to LW width
///    - Grid with 7 items and height 4 would have 2 columns and 4 rows. Here we have 2 columns
/// and LW width = 4. So, when wrapping, the grid will consider 4 columns and wrap around to
/// the first column.
#[derive(Debug)]
pub struct Grid {
    data: Vec<Vec<Bytes>>,
    num_items: usize,
    lw_aligned_width: usize,
}

fn build_column_first_grid(data: Vec<Bytes>, height: usize) -> Result<Vec<Vec<Bytes>>, Error> {
    if data.is_empty() {
        return Err(Error::EmptyInput);
    }
    if height == 0 {
        return Err(Error::InvalidWidth(height));
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

fn mod_int(int: i64, m: usize) -> usize {
    let w = m as i64;
    ((int % w + w) % w) as usize
}

fn calculate_lw_aligned_width(num_items: usize, height: usize) -> usize {
    let lw = height * height;
    ((num_items + lw - 1) / lw) * height
}

impl Grid {
    pub fn new(data: Vec<Bytes>, height: usize) -> Result<Self, Error> {
        let num_items = data.len();
        Ok(Self {
            data: build_column_first_grid(data, height)?,
            num_items,
            lw_aligned_width: calculate_lw_aligned_width(num_items, height),
        })
    }

    pub fn get_cell(&self, x: i64, y: i64) -> &Bytes {
        &self.data[self.mod_x(x)][self.mod_y(y)]
    }

    pub fn try_get_cell(&self, x: i64, y: i64) -> Option<&Bytes> {
        if self.has_cell(x, y) {
            Some(self.get_cell(x, y))
        } else {
            None
        }
    }

    fn mod_x(&self, x: i64) -> usize {
        mod_int(x, self.lw_aligned_width)
    }

    fn mod_y(&self, y: i64) -> usize {
        mod_int(y, self.get_height())
    }

    /// Returns the width of the grid, i.e., the number of actual columns.
    pub fn get_width(&self) -> usize {
        self.data.len()
    }

    /// Returns the height of the grid, i.e., the number of rows in each column.
    pub fn get_height(&self) -> usize {
        self.data.get(0).map_or(0, |col| col.len())
    }

    pub fn has_cell(&self, x: i64, y: i64) -> bool {
        let x = self.mod_x(x);
        let y = self.mod_y(y);
        x * self.get_height() + y < self.num_items
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

    pub fn get_num_items(&self) -> usize {
        self.num_items
    }
}

/// GridBuilder is a helper struct to build a Grid.
/// It allows setting individual cells in the grid and then building the final Grid.
#[derive(Debug)]
pub struct GridBuilder {
    data: Vec<Vec<Bytes>>,
    num_items: usize,
}

impl GridBuilder {
    /// Creates a new GridBuilder with the given number of items and height.
    pub fn new(num_items: usize, height: usize) -> Result<Self, Error> {
        if num_items == 0 {
            return Err(Error::InvalidNumItems(num_items));
        }
        if height == 0 {
            return Err(Error::InvalidWidth(height));
        }

        let num_cols = (num_items + height - 1) / height;
        let mut grid: Vec<Vec<Bytes>> = Vec::with_capacity(num_cols);
        for _ in 0..num_cols {
            grid.push(vec![Bytes::new(); height]);
        }

        Ok(Self {
            data: grid,
            num_items,
        })
    }

    /// Sets the value of a cell at the given coordinates.
    pub fn set_cell(&mut self, x: usize, y: usize, value: Bytes) {
        self.data[x][y] = value;
    }

    /// Builds the final Grid.
    pub fn build(self) -> Grid {
        let h = self.data[0].len();
        Grid {
            data: self.data,
            num_items: self.num_items,
            lw_aligned_width: calculate_lw_aligned_width(self.num_items, h),
        }
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
            grid.get_cell(0, 0),
            &data[0],
            "Cell (0, 0) should contain 'a'"
        );
        assert_eq!(
            grid.get_cell(0, 1),
            &data[1],
            "Cell (0, 1) should contain 'b'"
        );
        assert_eq!(
            grid.get_cell(1, 0),
            &data[2],
            "Cell (1, 0) should contain 'c'"
        );
        assert_eq!(
            grid.get_cell(1, 1),
            &data[3],
            "Cell (1, 1) should contain 'd'"
        );
    }

    #[test]
    fn test_grid_new_empty_input() {
        let data = vec![];
        let result = Grid::new(data, 2);
        assert!(result.is_err(), "Expected an error for empty input");
        assert_eq!(result.unwrap_err().to_string(), "Input vector is empty");
    }

    #[test]
    fn test_grid_new_invalid_width() {
        let data = vec![Bytes::from("a"), Bytes::from("b")];
        let result = Grid::new(data, 0);
        assert!(result.is_err(), "Expected an error for invalid width");
        assert_eq!(result.unwrap_err().to_string(), "Invalid width: 0");
    }

    #[test]
    fn test_grid_get_cell() {
        let grid = Grid::new(create_ag_chunks(), 3).unwrap();

        assert_eq!(grid.get_cell(0, 0), &Bytes::from("a"));
        assert_eq!(grid.get_cell(0, 1), &Bytes::from("b"));
        assert_eq!(grid.get_cell(0, 2), &Bytes::from("c"));
        assert_eq!(grid.get_cell(1, 0), &Bytes::from("d"));
        assert_eq!(grid.get_cell(1, 1), &Bytes::from("e"));
        assert_eq!(grid.get_cell(1, 2), &Bytes::from("f"));
        assert_eq!(grid.get_cell(2, 0), &Bytes::from("g"));

        // Test wrapping
        assert_eq!(grid.get_cell(3, 0), &Bytes::from("a"));
        assert_eq!(grid.get_cell(-1, 0), &Bytes::from("g"));
        assert_eq!(grid.get_cell(0, -1), &Bytes::from("c"));
    }

    #[test]
    fn test_grid_get_cell_incomplete_lw() {
        let grid = Grid::new(create_ag_chunks(), 4).unwrap();

        assert!(grid.has_cell(0, 0));
        assert!(grid.has_cell(0, 1));
        assert!(grid.has_cell(0, 2));
        assert!(grid.has_cell(0, 3));
        assert!(grid.has_cell(1, 0));
        assert!(grid.has_cell(1, 1));
        assert!(grid.has_cell(1, 2));

        assert_eq!(grid.get_cell(0, 0), &Bytes::from("a"));
        assert_eq!(grid.get_cell(0, 1), &Bytes::from("b"));
        assert_eq!(grid.get_cell(0, 2), &Bytes::from("c"));
        assert_eq!(grid.get_cell(0, 3), &Bytes::from("d"));
        assert_eq!(grid.get_cell(1, 0), &Bytes::from("e"));
        assert_eq!(grid.get_cell(1, 1), &Bytes::from("f"));
        assert_eq!(grid.get_cell(1, 2), &Bytes::from("g"));

        assert!(!grid.has_cell(1, 3));
        assert!(!grid.has_cell(2, 0));
        assert!(!grid.has_cell(2, 1));
        assert!(!grid.has_cell(2, 2));
        assert!(!grid.has_cell(2, 3));
        assert!(!grid.has_cell(3, 0));
        assert!(!grid.has_cell(3, 1));
        assert!(!grid.has_cell(3, 2));
        assert!(!grid.has_cell(3, 3));

        // Test wrapping
        assert!(grid.has_cell(4, 0));
        assert!(grid.has_cell(5, 0));
        assert!(grid.has_cell(-3, 2));
        assert!(grid.has_cell(0, -1));

        assert_eq!(grid.get_cell(4, 0), &Bytes::from("a"));
        assert_eq!(grid.get_cell(5, 0), &Bytes::from("e"));
        assert_eq!(grid.get_cell(-3, 2), &Bytes::from("g"));
        assert_eq!(grid.get_cell(0, -1), &Bytes::from("d"));

        assert!(!grid.has_cell(6, 0));
        assert!(!grid.has_cell(6, 1));
        assert!(!grid.has_cell(6, 2));
        assert!(!grid.has_cell(5, 3));
        assert!(!grid.has_cell(-1, 0));
        assert!(!grid.has_cell(-2, 0));
        assert!(!grid.has_cell(-1, 1));
        assert!(!grid.has_cell(-2, 1));
        assert!(!grid.has_cell(-1, 2));
        assert!(!grid.has_cell(-2, 2));
        assert!(!grid.has_cell(-3, 3));
        assert!(!grid.has_cell(1, -1));
    }

    #[test]
    fn test_grid_width_height() {
        let test_cases = vec![
            (1, 7, 1),
            (2, 4, 2),
            (3, 3, 3),
            (4, 2, 4),
            (7, 1, 7),
            (8, 1, 7),
            (16, 1, 7),
        ];

        for (height, expected_width, expected_height) in test_cases {
            let grid = Grid::new(create_ag_chunks(), height).unwrap();
            assert_eq!(grid.get_width(), expected_width, "Height: {}", height);
            assert_eq!(grid.get_height(), expected_height, "Height: {}", height);
        }
    }

    #[test]
    fn test_grid_try_get_cell() {
        let grid = Grid::new(create_ag_chunks(), 3).unwrap();

        assert_eq!(grid.try_get_cell(0, 0), Some(&Bytes::from("a")));
        assert_eq!(grid.try_get_cell(0, 1), Some(&Bytes::from("b")));
        assert_eq!(grid.try_get_cell(0, 2), Some(&Bytes::from("c")));
        assert_eq!(grid.try_get_cell(1, 0), Some(&Bytes::from("d")));
        assert_eq!(grid.try_get_cell(1, 1), Some(&Bytes::from("e")));
        assert_eq!(grid.try_get_cell(1, 2), Some(&Bytes::from("f")));
        assert_eq!(grid.try_get_cell(2, 0), Some(&Bytes::from("g")));
        assert_eq!(grid.try_get_cell(2, 1), None);
        assert_eq!(grid.try_get_cell(2, 2), None);
    }

    #[test]
    fn test_grid_has_cell() {
        let grid = Grid::new(create_ag_chunks(), 3).unwrap();

        assert!(grid.has_cell(0, 0));
        assert!(grid.has_cell(0, 1));
        assert!(grid.has_cell(0, 2));
        assert!(grid.has_cell(1, 0));
        assert!(grid.has_cell(1, 1));
        assert!(grid.has_cell(1, 2));
        assert!(grid.has_cell(2, 0));
        assert!(!grid.has_cell(2, 1));
        assert!(!grid.has_cell(2, 2));
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
    fn test_grid_builder() {
        let mut builder = GridBuilder::new(7, 3).unwrap();
        builder.set_cell(0, 0, Bytes::from("a"));
        builder.set_cell(0, 1, Bytes::from("b"));
        builder.set_cell(0, 2, Bytes::from("c"));
        builder.set_cell(1, 0, Bytes::from("d"));
        builder.set_cell(1, 1, Bytes::from("e"));
        builder.set_cell(1, 2, Bytes::from("f"));
        builder.set_cell(2, 0, Bytes::from("g"));

        let grid = builder.build();

        assert_eq!(grid.get_width(), 3, "Grid width should be 3");
        assert_eq!(grid.get_height(), 3, "Grid height should be 3");
        assert_eq!(grid.get_num_items(), 7, "Grid should have 7 items");
        assert_eq!(grid.get_cell(0, 0), &Bytes::from("a"));
        assert_eq!(grid.get_cell(0, 1), &Bytes::from("b"));
        assert_eq!(grid.get_cell(0, 2), &Bytes::from("c"));
        assert_eq!(grid.get_cell(1, 0), &Bytes::from("d"));
        assert_eq!(grid.get_cell(1, 1), &Bytes::from("e"));
        assert_eq!(grid.get_cell(1, 2), &Bytes::from("f"));
        assert_eq!(grid.get_cell(2, 0), &Bytes::from("g"));
    }

    #[test]
    fn test_grid_builder_invalid_width() {
        let result = GridBuilder::new(5, 0);
        assert!(result.is_err(), "Expected an error for invalid width");
        assert_eq!(result.unwrap_err().to_string(), "Invalid width: 0");
    }

    #[test]
    fn test_grid_builder_invalid_num_items() {
        let result = GridBuilder::new(0, 3);
        assert!(
            result.is_err(),
            "Expected an error for invalid number of items"
        );
        assert_eq!(
            result.unwrap_err().to_string(),
            "Invalid number of items: 0"
        );
    }
}
