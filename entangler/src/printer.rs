// Copyright 2024 Entanglement Contributors
// SPDX-License-Identifier: Apache-2.0, MIT

use std::fmt::{self, Write};

use crate::grid::{Dir, Pos};
use crate::lattice::Graph;

/// Printer prints a graph state to the console.
/// Here is an example of a printed graph:
/// ```
///      0   1   2   3   
///    \   X           /  
/// 0    O           O  
///    \   \   X       \  
/// 1  -     O -       -
///                       
/// 2            O -    
///    /   /           /  
/// 3    O           O  
///    /   X           \  
/// ```
/// The graph is printed in a grid format, with each node represented by a character.
/// Where:
/// - `O` represents a data node
/// - `/` represents a Left parity node
/// - `\` represents a Right parity node
/// - `-` represents a Horizontal parity
/// - `X` represents both a Left and Right parity node
pub struct Printer<'a> {
    graph: &'a Graph,
    max_x: i64,
    height: i64,
    buffer: String,
}

impl fmt::Display for Graph {
    fn fmt(&self, _: &mut fmt::Formatter) -> fmt::Result {
        Printer::new(self).print();
        Ok(())
    }
}

impl<'a> Printer<'a> {
    pub fn new(graph: &'a Graph) -> Self {
        let max_x = graph.nodes.keys().map(|id| id.pos.x).max().unwrap_or(0);
        let height = graph.positioner.get_height() as i64;

        // Calculate buffer size and preallocate
        let buffer_size = Self::calculate_buffer_size(max_x, height);

        Self {
            graph,
            max_x,
            height,
            buffer: String::with_capacity(buffer_size),
        }
    }

    fn calculate_buffer_size(max_x: i64, height: i64) -> usize {
        let width = (max_x + 2) as usize; // +2 for edge cases
        let header_size = 5 + width * 4 + 1; // "     " + width * 4 (for each column) + newline
        let row_size = 3 + width * 4 + 1; // row number + width * 4 (for data and horizontals) + newline
        let diagonal_size = 2 + width * 4 + 1; // "  " + width * 4 (for diagonals) + newline

        header_size + (height as usize + 1) * (row_size + diagonal_size)
    }

    /// Print the graph to the console.
    pub fn print(&mut self) {
        self.print_header();
        self.buffer.push('\n');
        self.print_diagonal_parity(-1);
        self.buffer.push('\n');

        for y in 0..self.height {
            self.print_data_and_horizontal(y);
            self.buffer.push('\n');
            self.print_diagonal_parity(y);
            self.buffer.push('\n');
        }

        print!("{}", self.buffer);
    }

    fn print_header(&mut self) {
        write!(self.buffer, "     ").unwrap();
        for x in 0..=self.max_x {
            write!(self.buffer, "{:<4}", x).unwrap();
        }
    }

    fn print_diagonal_parity(&mut self, y: i64) {
        write!(self.buffer, "  ").unwrap();
        for x in -1..=self.max_x {
            let has_left = !(x == self.max_x && y == self.height - 1 || x == -1 && y == -1)
                && self
                    .graph
                    .has_parity_node_along_dir(Pos::new(x, y + 1), Dir::UR);
            let has_right = !(x == self.max_x && y == -1 || x == -1 && y == self.height - 1)
                && self
                    .graph
                    .has_parity_node_along_dir(Pos::new(x, y), Dir::DR);

            let symbol = if has_left && has_right {
                " X  "
            } else if has_left {
                " /  "
            } else if has_right {
                " \\  "
            } else {
                "    "
            };
            self.buffer.push_str(symbol);
        }
    }

    fn print_data_and_horizontal(&mut self, y: i64) {
        write!(self.buffer, "{:<3}", y).unwrap();
        let left_edge_pos = Pos::new(self.max_x, y);
        if self.graph.has_parity_node_along_dir(left_edge_pos, Dir::R) {
            self.buffer.push('-');
        } else {
            self.buffer.push(' ');
        }
        self.buffer.push(' ');

        for x in 0..=self.max_x {
            let pos = Pos::new(x, y);
            if self.graph.has_data_node(pos) {
                self.buffer.push('O');
            } else {
                self.buffer.push(' ');
            }
            if x < self.max_x {
                if self.graph.has_parity_node_along_dir(pos, Dir::R) {
                    self.buffer.push_str(" - ");
                } else {
                    self.buffer.push_str("   ");
                }
            }
        }

        let right_edge_pos = Pos::new(0, y);
        if self.graph.has_parity_node_along_dir(right_edge_pos, Dir::L) {
            self.buffer.push_str(" -");
        } else {
            self.buffer.push_str("  ");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parity::StrandType;
    use bytes::Bytes;

    #[test]
    fn test_print_graph() {
        let mut graph = Graph::new(4, 16);

        let b = || -> Bytes { Bytes::new() };

        graph.add_data_node(Pos::new(0, 0), b());
        graph.add_data_node(Pos::new(1, 1), b());
        graph.add_data_node(Pos::new(2, 2), b());
        graph.add_data_node(Pos::new(3, 3), b());
        graph.add_data_node(Pos::new(0, 3), b());
        graph.add_data_node(Pos::new(3, 0), b());

        graph.add_parity_node(Pos::new(0, 0), b(), StrandType::Right);
        graph.add_parity_node(Pos::new(0, 0), b(), StrandType::Left);
        graph.add_parity_node(Pos::new(3, 0), b(), StrandType::Right);
        graph.add_parity_node(Pos::new(3, 0), b(), StrandType::Left);
        graph.add_parity_node(Pos::new(0, 3), b(), StrandType::Right);
        graph.add_parity_node(Pos::new(0, 3), b(), StrandType::Left);
        graph.add_parity_node(Pos::new(3, 3), b(), StrandType::Right);
        graph.add_parity_node(Pos::new(3, 3), b(), StrandType::Left);
        graph.add_parity_node(Pos::new(1, 1), b(), StrandType::Horizontal);
        graph.add_parity_node(Pos::new(1, 1), b(), StrandType::Left);
        graph.add_parity_node(Pos::new(1, 0), b(), StrandType::Right);
        graph.add_parity_node(Pos::new(3, 1), b(), StrandType::Horizontal);
        graph.add_parity_node(Pos::new(2, 2), b(), StrandType::Horizontal);

        let mut printer = Printer::new(&graph);
        printer.print();

        let expected_output = r#"     0   1   2   3   
   \   X           /  
0    O           O  
   \   \   X       \  
1  -     O -       -
                      
2            O -    
   /   /           /  
3    O           O  
   /   X           \  
"#;

        assert_eq!(printer.buffer, expected_output);
    }
}
