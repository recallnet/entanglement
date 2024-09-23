// Copyright 2024 Entanglement Contributors
// SPDX-License-Identifier: Apache-2.0, MIT

use crate::grid::{Dir, Pos, Positioner};
use crate::parity::StrandType;
use bytes::Bytes;
use std::borrow::Borrow;

use std::collections::HashMap;

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub struct NodeId {
    pub grid_type: GridType,
    pub pos: Pos,
}

impl NodeId {
    pub fn new(grid_type: GridType, pos: Pos) -> Self {
        Self { grid_type, pos }
    }

    pub fn new_data_id(pos: Pos) -> Self {
        Self::new(GridType::Data, pos)
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub enum GridType {
    Data,
    ParityLeft,
    ParityHorizontal,
    ParityRight,
}

impl From<Dir> for GridType {
    fn from(dir: Dir) -> Self {
        match dir {
            Dir::DL | Dir::UR => GridType::ParityLeft,
            Dir::L | Dir::R => GridType::ParityHorizontal,
            Dir::UL | Dir::DR => GridType::ParityRight,
        }
    }
}

impl<T: Borrow<StrandType>> From<T> for GridType {
    fn from(st: T) -> Self {
        match st.borrow() {
            StrandType::Left => GridType::ParityLeft,
            StrandType::Horizontal => GridType::ParityHorizontal,
            StrandType::Right => GridType::ParityRight,
        }
    }
}

impl TryInto<StrandType> for GridType {
    type Error = anyhow::Error;
    fn try_into(self) -> Result<StrandType, Self::Error> {
        match self {
            GridType::ParityLeft => Ok(StrandType::Left),
            GridType::ParityHorizontal => Ok(StrandType::Horizontal),
            GridType::ParityRight => Ok(StrandType::Right),
            _ => Err(anyhow::anyhow!("Invalid grid type")),
        }
    }
}

#[derive(Debug, Clone)]
pub struct DataNode {
    pub chunk: Bytes,
    parities: HashMap<Dir, NodeId>,
}

impl DataNode {
    pub fn new(chunk: Bytes) -> Self {
        Self {
            chunk,
            parities: HashMap::new(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ParityNode {
    pub chunk: Bytes,
    curr_data: Option<NodeId>,
    next_data: Option<NodeId>,
}

impl ParityNode {
    pub fn new(chunk: Bytes) -> Self {
        Self {
            chunk,
            curr_data: None,
            next_data: None,
        }
    }
}

#[derive(Debug, Clone)]
pub enum Node {
    Data(DataNode),
    Parity(ParityNode),
}

#[derive(Debug)]
pub struct Graph {
    nodes: HashMap<NodeId, Node>,
    positioner: Positioner,
}

impl Graph {
    pub fn new(grid_height: usize, num_grid_items: usize) -> Self {
        Self {
            nodes: HashMap::new(),
            positioner: Positioner::new(grid_height, num_grid_items),
        }
    }

    pub fn add_data_node(&mut self, pos: Pos, chunk: Bytes) {
        let data_id = NodeId::new_data_id(self.positioner.normalize(pos));

        let mut data_node = DataNode::new(chunk);

        for dir in Dir::all() {
            let parity_pos = if dir.is_forward() { pos } else { pos + dir };
            let parity_id = NodeId::new(dir.into(), parity_pos);
            if let Some(Node::Parity(parity_node)) = self.nodes.get_mut(&parity_id) {
                if dir.is_forward() {
                    parity_node.curr_data = Some(data_id);
                } else {
                    parity_node.next_data = Some(data_id);
                }
                data_node.parities.insert(dir, parity_id);
            }
        }

        self.nodes.insert(data_id, Node::Data(data_node));
    }

    pub fn remove_data_node(&mut self, pos: Pos) {
        let data_id = NodeId::new_data_id(self.positioner.normalize(pos));
        if let Some(Node::Data(data_node)) = self.nodes.remove(&data_id) {
            for parity_id in data_node.parities.values() {
                if let Some(Node::Parity(parity_node)) = self.nodes.get_mut(parity_id) {
                    if parity_node.curr_data == Some(data_id) {
                        parity_node.curr_data = None;
                    } else if parity_node.next_data == Some(data_id) {
                        parity_node.next_data = None;
                    }
                }
            }
        }
    }

    pub fn add_parity_node(&mut self, pos: Pos, chunk: Bytes, strand_type: StrandType) {
        let pos = self.positioner.normalize(pos);
        let parity_id = NodeId::new(strand_type.into(), pos);
        let next_data_pos = self.positioner.normalize(pos.adjacent(strand_type.into()));
        let curr_data_id = NodeId::new_data_id(pos);
        let next_data_id = NodeId::new_data_id(next_data_pos);

        let mut parity_node = ParityNode::new(chunk);
        if let Some(Node::Data(data_node)) = self.nodes.get_mut(&curr_data_id) {
            data_node.parities.insert(strand_type.into(), parity_id);
            parity_node.curr_data = Some(curr_data_id);
        }

        if let Some(Node::Data(next_data_node)) = self.nodes.get_mut(&next_data_id) {
            next_data_node
                .parities
                .insert(Dir::from(strand_type).opposite(), parity_id);
            parity_node.next_data = Some(next_data_id);
        }

        self.nodes.insert(parity_id, Node::Parity(parity_node));
    }

    pub fn get_node(&self, id: &NodeId) -> Option<&Node> {
        self.nodes.get(id)
    }

    pub fn has_data_node(&self, pos: Pos) -> bool {
        self.nodes.contains_key(&NodeId::new_data_id(pos))
    }

    pub fn get_data_node(&self, pos: Pos) -> Option<&DataNode> {
        if let Some(Node::Data(node)) = self.nodes.get(&NodeId::new_data_id(pos)) {
            Some(node)
        } else {
            None
        }
    }

    pub fn get_parity_node(&self, pos: Pos, strand_type: StrandType) -> Option<&ParityNode> {
        if let Some(Node::Parity(node)) = self.nodes.get(&NodeId::new(strand_type.into(), pos)) {
            Some(node)
        } else {
            None
        }
    }

    pub fn get_parity_node_along_dir(&self, pos: Pos, dir: Dir) -> Option<&ParityNode> {
        let pos = if dir.is_forward() {
            pos
        } else {
            self.positioner.normalize(pos + dir)
        };
        if let Some(Node::Parity(node)) = self.nodes.get(&NodeId::new(dir.into(), pos)) {
            Some(node)
        } else {
            None
        }
    }

    pub fn has_parity_node_along_dir(&self, pos: Pos, dir: Dir) -> bool {
        self.get_parity_node_along_dir(pos, dir).is_some()
    }

    pub fn get_neighbor_data_nodes(&self, pos: Pos) -> Vec<&Node> {
        let directions = Dir::all();
        let mut neighbors = Vec::with_capacity(directions.len());

        for &dir in &directions {
            let neighbor_id = NodeId::new_data_id(pos.adjacent(dir));
            if let Some(node) = self.nodes.get(&neighbor_id) {
                neighbors.push(node);
            }
        }

        neighbors
    }

    pub fn get_missing_neighbors_data_nodes(&self, pos: Pos) -> Vec<Pos> {
        let directions = Dir::all();
        let mut neighbors = Vec::with_capacity(directions.len());

        for &dir in &directions {
            let neighbor_id = NodeId::new_data_id(pos.adjacent(dir));
            if !self.nodes.contains_key(&neighbor_id) {
                neighbors.push(pos.adjacent(dir));
            }
        }

        neighbors
    }

    pub fn for_each_neighbor_data_node<F>(&self, pos: Pos, mut f: F)
    where
        F: FnMut(&Node),
    {
        for dir in Dir::all() {
            let neighbor_id = NodeId::new_data_id(pos.adjacent(dir));
            if let Some(node) = self.nodes.get(&neighbor_id) {
                f(node);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bytes(s: &'static str) -> Bytes {
        Bytes::from(s)
    }

    fn bytes_str(s: &str) -> Bytes {
        Bytes::from(s.to_string())
    }

    fn assert_node_connections(graph: &Graph, data_pos: Pos, expected_connections: &[(Dir, Pos)]) {
        let data_node = graph
            .get_data_node(data_pos)
            .expect("Data node should exist");

        for &(dir, parity_pos) in expected_connections {
            assert!(
                data_node.parities.contains_key(&dir),
                "Data node at {:?} should be connected to parity {:?} in direction {:?}",
                data_pos,
                parity_pos,
                dir
            );
            assert_eq!(
                *data_node.parities.get(&dir).unwrap(),
                NodeId::new(GridType::from(dir), parity_pos),
                //NodeId::new(strand_type.into(), parity_pos),
                "Data node at {:?} should be connected to correct parity at {:?}",
                data_pos,
                parity_pos
            );

            let parity_node = graph
                .get_parity_node(parity_pos, dir.into())
                .expect("Parity node should exist");

            if dir.is_forward() {
                assert_eq!(
                    parity_node.curr_data,
                    Some(NodeId::new_data_id(data_pos)),
                    "Parity node at {:?} should have correct curr_data {:?}",
                    parity_pos,
                    data_pos
                );
            } else {
                assert_eq!(
                    parity_node.next_data,
                    Some(NodeId::new_data_id(data_pos)),
                    "Parity node at {:?} should have correct next_data {:?}",
                    parity_pos,
                    data_pos
                );
            }
        }
    }

    fn assert_parity_connections(
        graph: &Graph,
        parity_pos: Pos,
        strand_type: StrandType,
        expected_curr: Option<Pos>,
        expected_next: Option<Pos>,
    ) {
        let parity_node = graph
            .get_parity_node(parity_pos, strand_type)
            .expect("Parity node should exist");

        assert_eq!(
            parity_node.curr_data,
            expected_curr.map(NodeId::new_data_id),
            "Parity node at {:?} should have correct curr_data",
            parity_pos
        );
        assert_eq!(
            parity_node.next_data,
            expected_next.map(NodeId::new_data_id),
            "Parity node at {:?} should have correct next_data",
            parity_pos
        );
    }

    #[test]
    fn added_data_node_can_be_retrieved() {
        let mut graph = Graph::new(4, 16);
        let pos = Pos::new(2, 1);
        graph.add_data_node(pos, bytes("a"));

        let data_node = graph.get_data_node(pos).unwrap();
        assert_eq!(data_node.chunk, bytes("a"));
        assert_eq!(data_node.parities.len(), 0);

        let data_node = graph.get_data_node(Pos::new(0, 1));
        assert!(data_node.is_none());

        assert!(graph.get_parity_node(pos, StrandType::Left).is_none());
        assert!(graph.get_parity_node(pos, StrandType::Right).is_none());
        assert!(graph.get_parity_node(pos, StrandType::Horizontal).is_none());
    }

    #[test]
    fn added_parity_node_can_be_retrieved() {
        let mut graph = Graph::new(4, 16);
        let pos = Pos::new(2, 1);
        graph.add_parity_node(pos, bytes("la"), StrandType::Left);

        let parity_node = graph.get_parity_node(pos, StrandType::Left).unwrap();
        assert_eq!(parity_node.chunk, bytes("la"));
        assert_parity_connections(&graph, pos, StrandType::Left, None, None);

        assert!(graph.get_data_node(pos).is_none());
        assert!(graph.get_parity_node(pos, StrandType::Right).is_none());
        assert!(graph.get_parity_node(pos, StrandType::Horizontal).is_none());
    }

    #[test]
    fn get_node_retrieves_correct_node() {
        let mut graph = Graph::new(4, 16);
        let data_pos = Pos::new(1, 1);
        let parity_pos = Pos::new(2, 1);

        graph.add_data_node(data_pos, bytes("data_chunk"));
        graph.add_parity_node(parity_pos, bytes("parity_chunk"), StrandType::Right);

        let data_id = NodeId::new_data_id(data_pos);
        let parity_id = NodeId::new(GridType::ParityRight, parity_pos);

        if let Some(Node::Data(data_node)) = graph.get_node(&data_id) {
            assert_eq!(data_node.chunk, bytes("data_chunk"));
        } else {
            panic!("Expected data node at {:?}", data_pos);
        }

        if let Some(Node::Parity(parity_node)) = graph.get_node(&parity_id) {
            assert_eq!(parity_node.chunk, bytes("parity_chunk"));
        } else {
            panic!("Expected parity node at {:?}", parity_pos);
        }

        let non_existent_id = NodeId::new_data_id(Pos::new(3, 3));
        assert!(graph.get_node(&non_existent_id).is_none());
    }

    #[test]
    fn if_data_exists_and_matching_parity_is_added_should_connect_both() {
        let mut graph = Graph::new(4, 16);
        let pos = Pos::new(1, 1);
        graph.add_data_node(pos, bytes("a"));
        graph.add_parity_node(pos, bytes("ra"), StrandType::Right);

        assert_node_connections(&graph, pos, &[(Dir::DR, pos)]);
        assert_parity_connections(&graph, pos, StrandType::Right, Some(pos), None);
    }

    #[test]
    fn if_data_exists_and_matching_from_back_parity_is_added_should_connect_both() {
        let mut graph = Graph::new(4, 16);
        let data_pos = Pos::new(1, 1);
        let parity_pos = data_pos.adjacent(Dir::DL);
        graph.add_data_node(data_pos, bytes("f"));
        graph.add_parity_node(parity_pos, bytes("lc"), StrandType::Left);

        assert_node_connections(&graph, data_pos, &[(Dir::DL, parity_pos)]);
        assert_parity_connections(&graph, parity_pos, StrandType::Left, None, Some(data_pos));
    }

    #[test]
    fn if_parity_exists_and_matching_data_is_added_should_connect_both() {
        let mut graph = Graph::new(4, 16);
        let pos = Pos::new(1, 1);
        graph.add_parity_node(pos, bytes("la"), StrandType::Left);
        graph.add_data_node(pos, bytes("a"));

        assert_node_connections(&graph, pos, &[(Dir::UR, pos)]);
        assert_parity_connections(&graph, pos, StrandType::Left, Some(pos), None);
    }

    #[test]
    fn if_parity_exists_and_matching_data_is_added_in_front_should_connect_both() {
        let mut graph = Graph::new(4, 16);
        let parity_pos = Pos::new(0, 1);
        let data_pos = parity_pos.adjacent(Dir::R);
        graph.add_parity_node(parity_pos, bytes("hb"), StrandType::Horizontal);
        graph.add_data_node(data_pos, bytes("f"));

        assert_node_connections(&graph, data_pos, &[(Dir::L, parity_pos)]);
        assert_parity_connections(
            &graph,
            parity_pos,
            StrandType::Horizontal,
            None,
            Some(data_pos),
        );
    }

    #[test]
    fn edge_connections_wrap_around_correctly() {
        let mut graph = Graph::new(3, 9);

        let data = &["a", "b", "c", "d", "e", "f", "g", "h", "i"];
        let l_parity = &["la", "lb", "lc", "ld", "le", "lf", "lg", "lh", "li"];
        let h_parity = &["ha", "hb", "hc", "hd", "he", "hf", "hg", "hh", "hi"];
        let r_parity = &["ra", "rb", "rc", "rd", "re", "rf", "rg", "rh", "ri"];

        for x in 0..3 {
            for y in 0..3 {
                graph.add_data_node(Pos::new(x, y), bytes(data[x * 3 + y]));
            }
        }

        for x in 0..3 {
            for y in 0..3 {
                let pos = Pos::new(x, y);
                graph.add_parity_node(pos, bytes(l_parity[x * 3 + y]), StrandType::Left);
                graph.add_parity_node(pos, bytes(h_parity[x * 3 + y]), StrandType::Horizontal);
                graph.add_parity_node(pos, bytes(r_parity[x * 3 + y]), StrandType::Right);
            }
        }

        assert_node_connections(
            &graph,
            Pos::new(2, 1),
            &[
                (Dir::DL, Pos::new(1, 2)),
                (Dir::UR, Pos::new(2, 1)),
                (Dir::L, Pos::new(1, 1)),
                (Dir::R, Pos::new(2, 1)),
                (Dir::UL, Pos::new(1, 0)),
                (Dir::DR, Pos::new(2, 1)),
            ],
        );

        assert_node_connections(
            &graph,
            Pos::new(1, 2),
            &[
                (Dir::DL, Pos::new(0, 0)),
                (Dir::UR, Pos::new(1, 2)),
                (Dir::L, Pos::new(0, 2)),
                (Dir::R, Pos::new(1, 2)),
                (Dir::UL, Pos::new(0, 1)),
                (Dir::DR, Pos::new(1, 2)),
            ],
        );

        assert_node_connections(
            &graph,
            Pos::new(2, 0),
            &[
                (Dir::DL, Pos::new(1, 1)),
                (Dir::UR, Pos::new(2, 0)),
                (Dir::L, Pos::new(1, 0)),
                (Dir::R, Pos::new(2, 0)),
                (Dir::UL, Pos::new(1, 2)),
                (Dir::DR, Pos::new(2, 0)),
            ],
        );

        assert_node_connections(
            &graph,
            Pos::new(0, 2),
            &[
                (Dir::DL, Pos::new(2, 0)),
                (Dir::UR, Pos::new(0, 2)),
                (Dir::L, Pos::new(2, 2)),
                (Dir::R, Pos::new(0, 2)),
                (Dir::UL, Pos::new(2, 1)),
                (Dir::DR, Pos::new(0, 2)),
            ],
        );
    }

    #[test]
    fn sparse_grid_scenario() {
        let mut graph = Graph::new(3, 9);

        let data_positions = vec![
            Pos::new(0, 0),
            Pos::new(1, 0),
            Pos::new(2, 0),
            Pos::new(1, 1),
            Pos::new(2, 1),
            Pos::new(0, 2),
            Pos::new(2, 2),
        ];
        for pos in data_positions.iter() {
            graph.add_data_node(*pos, bytes_str(&format!("d{}{}", pos.x, pos.y)));
        }

        let parity_nodes = vec![
            (Pos::new(0, 0), StrandType::Horizontal),
            (Pos::new(1, 0), StrandType::Left),
            (Pos::new(2, 0), StrandType::Right),
            (Pos::new(0, 1), StrandType::Left),
            (Pos::new(1, 1), StrandType::Horizontal),
            (Pos::new(1, 1), StrandType::Right),
            (Pos::new(2, 1), StrandType::Right),
            (Pos::new(0, 2), StrandType::Horizontal),
            (Pos::new(1, 2), StrandType::Right),
            (Pos::new(2, 2), StrandType::Left),
        ];
        for (pos, strand_type) in parity_nodes.iter() {
            graph.add_parity_node(
                *pos,
                bytes_str(&format!("{:?}{}{}", strand_type, pos.x, pos.y)),
                *strand_type,
            );
        }

        assert_node_connections(&graph, Pos::new(0, 0), &[(Dir::R, Pos::new(0, 0))]);
        assert_parity_connections(
            &graph,
            Pos::new(0, 0),
            StrandType::Horizontal,
            Some(Pos::new(0, 0)),
            Some(Pos::new(1, 0)),
        );

        assert_node_connections(
            &graph,
            Pos::new(1, 0),
            &[(Dir::UR, Pos::new(1, 0)), (Dir::L, Pos::new(0, 0))],
        );
        assert_parity_connections(
            &graph,
            Pos::new(1, 0),
            StrandType::Left,
            Some(Pos::new(1, 0)),
            Some(Pos::new(2, 2)),
        );

        assert_node_connections(&graph, Pos::new(2, 0), &[(Dir::DR, Pos::new(2, 0))]);
        assert_parity_connections(
            &graph,
            Pos::new(2, 0),
            StrandType::Right,
            Some(Pos::new(2, 0)),
            None,
        );

        assert_node_connections(
            &graph,
            Pos::new(1, 1),
            &[(Dir::DR, Pos::new(1, 1)), (Dir::R, Pos::new(1, 1))],
        );
        assert_parity_connections(
            &graph,
            Pos::new(1, 1),
            StrandType::Horizontal,
            Some(Pos::new(1, 1)),
            Some(Pos::new(2, 1)),
        );
        assert_parity_connections(
            &graph,
            Pos::new(1, 1),
            StrandType::Right,
            Some(Pos::new(1, 1)),
            Some(Pos::new(2, 2)),
        );

        assert_node_connections(
            &graph,
            Pos::new(2, 1),
            &[(Dir::L, Pos::new(1, 1)), (Dir::DR, Pos::new(2, 1))],
        );
        assert_parity_connections(
            &graph,
            Pos::new(2, 1),
            StrandType::Right,
            Some(Pos::new(2, 1)),
            Some(Pos::new(0, 2)),
        );

        assert_node_connections(
            &graph,
            Pos::new(0, 2),
            &[(Dir::R, Pos::new(0, 2)), (Dir::UL, Pos::new(2, 1))],
        );
        assert_parity_connections(
            &graph,
            Pos::new(0, 2),
            StrandType::Horizontal,
            Some(Pos::new(0, 2)),
            None,
        );

        assert_parity_connections(
            &graph,
            Pos::new(1, 2),
            StrandType::Right,
            None,
            Some(Pos::new(2, 0)),
        );

        assert_node_connections(
            &graph,
            Pos::new(2, 2),
            &[
                (Dir::UR, Pos::new(2, 2)),
                (Dir::UL, Pos::new(1, 1)),
                (Dir::DL, Pos::new(1, 0)),
            ],
        );
        assert_parity_connections(
            &graph,
            Pos::new(2, 2),
            StrandType::Left,
            Some(Pos::new(2, 2)),
            None,
        );
    }

    #[test]
    fn complex_scenario_with_multiple_nodes() {
        let mut graph = Graph::new(3, 9);

        let data = &["a", "b", "c", "d", "e", "f", "g", "h", "i"];
        let l_parity = &["la", "lb", "lc", "ld", "le", "lf", "lg", "lh", "li"];
        let h_parity = &["ha", "hb", "hc", "hd", "he", "hf", "hg", "hh", "hi"];
        let r_parity = &["ra", "rb", "rc", "rd", "re", "rf", "rg", "rh", "ri"];

        for x in 0..3 {
            for y in 0..3 {
                graph.add_data_node(Pos::new(x, y), bytes(data[x * 3 + y]));
            }
        }

        for x in 0..3 {
            for y in 0..3 {
                let pos = Pos::new(x, y);
                graph.add_parity_node(pos, bytes(l_parity[x * 3 + y]), StrandType::Left);
                graph.add_parity_node(pos, bytes(h_parity[x * 3 + y]), StrandType::Horizontal);
                graph.add_parity_node(pos, bytes(r_parity[x * 3 + y]), StrandType::Right);
            }
        }

        assert_node_connections(
            &graph,
            Pos::new(1, 1),
            &[
                (Dir::UR, Pos::new(1, 1)),
                (Dir::R, Pos::new(1, 1)),
                (Dir::DR, Pos::new(1, 1)),
                (Dir::DL, Pos::new(0, 2)),
                (Dir::L, Pos::new(0, 1)),
                (Dir::UL, Pos::new(0, 0)),
            ],
        );

        assert_parity_connections(
            &graph,
            Pos::new(1, 1),
            StrandType::Left,
            Some(Pos::new(1, 1)),
            Some(Pos::new(2, 0)),
        );
        assert_parity_connections(
            &graph,
            Pos::new(1, 1),
            StrandType::Horizontal,
            Some(Pos::new(1, 1)),
            Some(Pos::new(2, 1)),
        );
        assert_parity_connections(
            &graph,
            Pos::new(1, 1),
            StrandType::Right,
            Some(Pos::new(1, 1)),
            Some(Pos::new(2, 2)),
        );

        assert_node_connections(
            &graph,
            Pos::new(0, 0),
            &[
                (Dir::UR, Pos::new(0, 0)),
                (Dir::R, Pos::new(0, 0)),
                (Dir::DR, Pos::new(0, 0)),
            ],
        );

        assert_parity_connections(
            &graph,
            Pos::new(0, 0),
            StrandType::Left,
            Some(Pos::new(0, 0)),
            Some(Pos::new(1, 2)),
        );
        assert_parity_connections(
            &graph,
            Pos::new(0, 0),
            StrandType::Horizontal,
            Some(Pos::new(0, 0)),
            Some(Pos::new(1, 0)),
        );
        assert_parity_connections(
            &graph,
            Pos::new(0, 0),
            StrandType::Right,
            Some(Pos::new(0, 0)),
            Some(Pos::new(1, 1)),
        );
    }
}
