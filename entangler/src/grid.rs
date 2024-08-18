// Copyright 2024 Entanglement Contributors
// SPDX-License-Identifier: Apache-2.0, MIT

use bytes::Bytes;
use thiserror;

use anyhow::Result;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Invalid number of rows: {0}")]
    InvalidRowCount(usize),
    #[error("Input vector is empty")]
    EmptyInput,
}

#[derive(Debug)]
pub struct Grid {
    data: Vec<Vec<Bytes>>,
}

fn build_column_first_grid(
    data: Vec<Bytes>,
    width: usize,
    height: usize,
) -> Result<Vec<Vec<Bytes>>, Error> {
    if data.is_empty() {
        return Err(Error::EmptyInput);
    }
    if width == 0 {
        return Err(Error::InvalidRowCount(width));
    }

    let num_items = data.len();
    let lw = width * height;
    let num_cols = ((data.len() + lw - 1) / lw) * width;
    let mut grid: Vec<Vec<Bytes>> = Vec::with_capacity(num_cols);
    for _ in 0..num_cols {
        grid.push(Vec::with_capacity(width));
    }
    for (i, datum) in data.into_iter().enumerate() {
        grid[i / width].push(datum);
    }

    fill_remaining_from_beginning(num_items, num_cols, width, &mut grid);

    Ok(grid)
}

fn fill_remaining_from_beginning(
    num_items: usize,
    num_cols: usize,
    num_rows: usize,
    grid: &mut Vec<Vec<Bytes>>,
) {
    for i in num_items..(num_cols * num_rows) {
        let ind_from_start = i - num_items;
        let x = ind_from_start / num_rows;
        let y = ind_from_start % num_rows;
        let chunk = grid[x][y].clone();
        grid[i / num_rows].push(chunk);
    }
}

impl Grid {
    pub fn new(data: Vec<Bytes>, width: usize, height: usize) -> Result<Self, Error> {
        Ok(Self {
            data: build_column_first_grid(data, width, height)?,
        })
    }

    pub fn with_size(width: usize, height: usize) -> Result<Self, Error> {
        if height == 0 {
            return Err(Error::InvalidRowCount(height));
        }

        let mut data: Vec<Vec<Bytes>> = Vec::with_capacity(width);
        for _ in 0..width {
            data.push(vec![Bytes::new(); height]);
        }

        Ok(Self { data })
    }

    pub fn set_cell(&mut self, x: i64, y: i64, value: Bytes) {
        let x = self.mod_x(x);
        let y = self.mod_y(y);
        self.data[x][y] = value;
    }

    pub fn get_cell(&self, x: i64, y: i64) -> &Bytes {
        &self.data[self.mod_x(x)][self.mod_y(y)]
    }

    fn mod_x(&self, x: i64) -> usize {
        let w = self.get_width() as i64;
        ((x % w + w) % w) as usize
    }

    fn mod_y(&self, y: i64) -> usize {
        let h = self.get_height() as i64;
        ((y % h + h) % h) as usize
    }

    pub fn get_width(&self) -> usize {
        self.data.len()
    }

    pub fn get_height(&self) -> usize {
        self.data[0].len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new() {
        let data = vec![
            Bytes::from("a"),
            Bytes::from("b"),
            Bytes::from("c"),
            Bytes::from("d"),
        ];
        let grid = Grid::new(data.clone(), 2, 2).unwrap();
        assert_eq!(grid.get_width(), 2, "Grid width should be 2");
        assert_eq!(grid.get_height(), 2, "Grid height should be 2");
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
    fn test_new_empty_input() {
        let data = vec![];
        let result = Grid::new(data, 2, 2);
        assert!(result.is_err(), "Expected an error for empty input");
        assert_eq!(result.unwrap_err().to_string(), "Input vector is empty");
    }

    #[test]
    fn test_new_invalid_row_count() {
        let data = vec![Bytes::from("a"), Bytes::from("b")];
        let result = Grid::new(data, 0, 2);
        assert!(result.is_err(), "Expected an error for invalid row count");
        assert_eq!(result.unwrap_err().to_string(), "Invalid number of rows: 0");
    }

    #[test]
    fn test_with_size() {
        let w = 4;
        let h = 4;
        let grid = Grid::with_size(w, h).unwrap();
        assert_eq!(grid.get_width(), w, "Grid width should be {}", w);
        assert_eq!(grid.get_height(), h, "Grid height should be {}", h);
    }

    #[test]
    fn test_get_cell() {
        let data = vec![
            // first LW
            Bytes::from("a"),
            Bytes::from("b"),
            Bytes::from("c"),
            Bytes::from("d"),
            Bytes::from("e"),
            Bytes::from("f"),
            Bytes::from("g"),
            Bytes::from("h"),
            Bytes::from("i"),
            // second LW
            Bytes::from("j"),
            Bytes::from("k"),
            Bytes::from("l"),
            Bytes::from("m"),
            Bytes::from("n"),
            Bytes::from("o"),
            Bytes::from("p"),
            Bytes::from("q"),
            Bytes::from("r"),
            // third LW, we leave it incomplete to test padding as well
            Bytes::from("s"),
            Bytes::from("t"),
            Bytes::from("u"),
            Bytes::from("v"),
        ];
        let grid = Grid::new(data, 3, 3).unwrap();

        //    0 1 2 3 4 5 6 7 8 x
        //  0 a d g j m p s v c
        //  1 b e h k n q t a d
        //  2 c f i l o r u b e
        //  y
        //
        let test_cases = vec![
            ((0, 0), "a"),
            ((0, 1), "b"),
            ((0, 2), "c"),
            ((1, 0), "d"),
            ((1, 1), "e"),
            ((1, 2), "f"),
            ((2, 0), "g"),
            ((2, 1), "h"),
            ((2, 2), "i"),
            ((3, 0), "j"),
            ((3, 1), "k"),
            ((3, 2), "l"),
            ((4, 0), "m"),
            ((4, 1), "n"),
            ((4, 2), "o"),
            ((5, 0), "p"),
            ((5, 1), "q"),
            ((5, 2), "r"),
            ((6, 0), "s"),
            ((6, 1), "t"),
            ((6, 2), "u"),
            ((7, 0), "v"),
            // padding
            ((7, 1), "a"),
            ((7, 2), "b"),
            ((8, 0), "c"),
            ((8, 1), "d"),
            ((8, 2), "e"),
            // negative and wrapping
            ((-1, 0), "c"),
            ((-2, 0), "v"),
            ((-4, 0), "p"),
            ((2 + 3 * 27, 0), "g"),
            ((0, -1), "c"),
            ((0, -2), "b"),
            ((0, -3), "a"),
            ((-5, -5), "n"),
        ];

        for ((x, y), expected) in test_cases {
            let cell = grid.get_cell(x, y);
            assert_eq!(
                cell,
                &Bytes::from(expected),
                "Cell ({}, {}) should contain '{}'",
                x,
                y,
                expected
            );
        }
    }

    #[test]
    fn test_set_cell() {
        let mut grid = Grid::with_size(9, 3).unwrap();

        //    0 1 2 3 4 5 6 7 8 x
        //  0 a d g j m p s v c
        //  1 b e h k n q t a d
        //  2 c f i l o r u b e
        //  y
        //
        let test_cases = vec![
            ((0, 0), "a"),
            ((0, 1), "b"),
            ((0, 2), "c"),
            ((1, 0), "d"),
            ((1, 1), "e"),
            ((1, 2), "f"),
            ((2, 0), "g"),
            ((2, 1), "h"),
            ((2, 2), "i"),
            ((3, 0), "j"),
            ((3, 1), "k"),
            ((3, 2), "l"),
            ((4, 0), "m"),
            ((4, 1), "n"),
            ((4, 2), "o"),
            ((5, 0), "p"),
            ((5, 1), "q"),
            ((5, 2), "r"),
            ((6, 0), "s"),
            ((6, 1), "t"),
            ((6, 2), "u"),
            ((7, 0), "v"),
            // padding
            ((7, 1), "a"),
            ((7, 2), "b"),
            ((8, 0), "c"),
            ((8, 1), "d"),
            ((8, 2), "e"),
            // negative and wrapping
            ((-1, 0), "c"),
            ((-2, 0), "v"),
            ((-4, 0), "p"),
            ((2 + 3 * 27, 0), "g"),
            ((0, -1), "c"),
            ((0, -2), "b"),
            ((0, -3), "a"),
            ((-5, -5), "n"),
        ];

        for ((x, y), value) in test_cases {
            grid.set_cell(x, y, Bytes::from(value));
            let cell = grid.get_cell(x, y);
            assert_eq!(
                cell,
                &Bytes::from(value),
                "Cell ({}, {}) should contain '{}'",
                x,
                y,
                value
            );
        }
    }
}
