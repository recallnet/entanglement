// Copyright 2024 Entanglement Contributors
// SPDX-License-Identifier: Apache-2.0, MIT

use crate::grid::{Dir, Pos, Positioner};
use crate::lattice::{Graph, NodeId, NodeType};
use crate::parity::StrandType;
use anyhow::Result;
use recall_entanglement_storage::{ChunkIdMapper, Error as StorageError, Storage};

use crate::Metadata;
use bytes::Bytes;
use std::collections::{HashMap, HashSet};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Failed to repair chunks")]
    FailedToRepairChunks,

    #[error("Storage error: {0}")]
    Storage(#[from] StorageError),

    #[error("Error occurred: {0}")]
    Other(#[source] anyhow::Error),
}

/// Repairer repairs missing (or sick) grid chunks using the available healthy chunks.
/// It build a so called sick graph out of known sick nodes and traverses all the nodes in
/// the sick graph looking for healthy neighbors to repair the sick nodes.
/// All found healthy nodes are stored in the healthy graph.
pub struct Repairer<'a, S: Storage> {
    metadata: Metadata,
    storage: &'a S,
    positioner: Positioner,
    mapper: S::ChunkIdMapper,
}

fn xor_chunks(chunk1: &Bytes, chunk2: &Bytes) -> Bytes {
    let mut chunk = Vec::with_capacity(chunk1.len());
    for i in 0..chunk1.len() {
        chunk.push(chunk1[i] ^ chunk2[i]);
    }
    Bytes::from(chunk)
}

impl<'a, S: Storage> Repairer<'a, S> {
    pub fn new(
        storage: &'a S,
        positioner: Positioner,
        metadata: Metadata,
        mapper: S::ChunkIdMapper,
    ) -> Self {
        Self {
            metadata,
            storage,
            positioner,
            mapper,
        }
    }

    pub async fn repair_chunks(
        &mut self,
        chunks: Vec<S::ChunkId>,
    ) -> Result<HashMap<S::ChunkId, Bytes>, Error> {
        let sick_nodes: Result<Vec<_>, _> = chunks
            .into_iter()
            .map(|c| self.mapper.id_to_index(&c))
            .collect();
        let sick_nodes = sick_nodes?;
        let sick_nodes = sick_nodes
            .into_iter()
            .map(|index| self.positioner.index_to_pos(index))
            .collect();

        let mut healer = Healer::new(
            self.storage.clone(),
            self.metadata.clone(),
            sick_nodes,
            self.positioner.height,
            self.positioner.num_items,
            self.mapper.clone(),
        );

        let healthy_chunks = healer.heal().await?;

        Ok(healthy_chunks
            .into_iter()
            .map(|(pos, chunk)| {
                (
                    self.mapper
                        .index_to_id(self.positioner.pos_to_index(pos))
                        .unwrap(),
                    chunk,
                )
            })
            .collect())
    }
}

/// Executes actual graph traversal and healing of the sick nodes.
struct Healer<T: Storage> {
    storage: T,
    sick_graph: Graph,
    healthy_graph: Graph,
    sick_nodes: Vec<Pos>,
    visited: HashSet<Pos>,
    positioner: Positioner,
    metadata: Metadata,
    mapper: T::ChunkIdMapper,
    /// Stack of nodes to visit. Each element is a tuple of a node and a list of directions to process.
    /// It is used to perform depth-first traversal of the graph while keeping the traversal state
    /// on the heap. It also helps to avoid recursion.
    stack: Vec<(Pos, Vec<Dir>)>,
    result: HashMap<Pos, Bytes>,
}

impl<T: Storage> Healer<T> {
    fn new(
        storage: T,
        metadata: Metadata,
        sick_nodes: Vec<Pos>,
        grid_height: u64,
        num_grid_items: u64,
        mapper: T::ChunkIdMapper,
    ) -> Self {
        let mut sick_graph = Graph::new(grid_height, num_grid_items);
        let healthy_graph = Graph::new(grid_height, num_grid_items);

        for &chunk in &sick_nodes {
            sick_graph.add_data_node(chunk, Bytes::new());
        }

        Self {
            storage,
            sick_graph,
            healthy_graph,
            sick_nodes,
            visited: HashSet::new(),
            positioner: Positioner::new(grid_height, num_grid_items),
            metadata,
            mapper,
            stack: Vec::new(),
            result: HashMap::new(),
        }
    }

    /// Keeps executing healing until all sick nodes are healed or no progress is made.
    async fn heal(&mut self) -> Result<HashMap<Pos, Bytes>, Error> {
        let mut num_sick_nodes = self.sick_nodes.len();
        while num_sick_nodes > 0 {
            self.execute_healing().await;
            if num_sick_nodes == self.sick_nodes.len() {
                return Err(Error::FailedToRepairChunks);
            }
            num_sick_nodes = self.sick_nodes.len();
        }

        Ok(self.result.clone())
    }

    async fn execute_healing(&mut self) {
        self.visited.clear();
        self.stack.clear();

        for pos in self.sick_nodes.clone() {
            if self.visited.contains(&pos) {
                continue;
            }

            self.visited.insert(pos);
            self.stack.push((pos, Dir::all().to_vec()));
            while let Some((pos, dirs)) = self.stack.pop() {
                if self.heal_data_node(pos, dirs).await {
                    self.result.insert(
                        pos,
                        self.healthy_graph.get_data_node(pos).unwrap().chunk.clone(),
                    );
                }
            }
        }

        self.sick_nodes
            .retain(|&pos| !self.result.contains_key(&pos));
    }

    /// Tries to heal a sick data node by traversing the graph and looking for healthy neighbors.
    /// If a healthy neighbor is found, the sick node is healed.
    /// Returns true if the node was healed, false otherwise.
    async fn heal_data_node(&mut self, pos: Pos, dirs: Vec<Dir>) -> bool {
        for i in 0..dirs.len() {
            let neighbor_pos = self.positioner.normalize(pos.adjacent(dirs[i]));
            if !self.positioner.is_pos_available(neighbor_pos) {
                continue;
            }
            // Check if the neighbor is a healthy data node
            if let Some(healthy_neighbor) = self.healthy_graph.get_data_node(neighbor_pos).cloned()
            {
                // If the neighbor is healthy, try to heal the sick node
                if self
                    .try_heal_with_neighbor_data_node(pos, neighbor_pos, &healthy_neighbor.chunk)
                    .await
                {
                    return true;
                    // Otherwise, continue to the next neighbor
                }
            } else if self.sick_graph.get_data_node(neighbor_pos).is_some() {
                if self.schedule_visit(pos, neighbor_pos, &dirs[i..]) {
                    return false;
                }
            } else {
                // if the data node is unknown, try to load it from the storage
                match self
                    .load_node(NodeId::new(NodeType::Data, neighbor_pos))
                    .await
                {
                    Ok(chunk) => {
                        self.healthy_graph
                            .add_data_node(neighbor_pos, chunk.clone());

                        if self
                            .try_heal_with_neighbor_data_node(pos, neighbor_pos, &chunk)
                            .await
                        {
                            return true;
                        }
                    }
                    Err(_) => {
                        self.sick_graph.add_data_node(neighbor_pos, Bytes::new());
                        if self.schedule_visit(pos, neighbor_pos, &dirs[i..]) {
                            return false;
                        }
                    }
                }
            }
        }
        false
    }

    /// Schedules a visit to a neighbor node if it hasn't been visited yet.
    fn schedule_visit(&mut self, pos: Pos, neighbor_pos: Pos, dirs: &[Dir]) -> bool {
        if !self.visited.contains(&neighbor_pos) {
            self.visited.insert(neighbor_pos);
            self.stack.push((pos, dirs.to_vec()));
            self.stack.push((neighbor_pos, Dir::all().to_vec()));
            return true;
        }
        false
    }

    /// Tries to heal a sick data node using a healthy neighbor data node and a parity node
    /// between them.
    async fn try_heal_with_neighbor_data_node(
        &mut self,
        pos: Pos,
        neighbor_pos: Pos,
        neighbor_chunk: &Bytes,
    ) -> bool {
        let parity_dir = self.positioner.determine_dir(pos, neighbor_pos).unwrap();
        // if the parity node is known to be sick, we can't heal the data node
        if self.sick_graph.has_parity_node_along_dir(pos, parity_dir) {
            return false;
        }
        // if the parity node is known to be healthy, we can heal the data node
        if let Some(healthy_parity) = self
            .healthy_graph
            .get_parity_node_along_dir(pos, parity_dir)
        {
            self.heal_with_neighbor_data_node(neighbor_chunk, &healthy_parity.chunk.clone(), pos);
            return true;
        } else {
            let parity_pos = if parity_dir.is_forward() {
                pos
            } else {
                neighbor_pos
            };
            let parity_strand: StrandType = parity_dir.into();
            // if the parity node is unknown, try to load it from the storage
            match self
                .load_node(NodeId::new(NodeType::from(parity_strand), parity_pos))
                .await
            {
                Ok(chunk) => {
                    self.healthy_graph
                        .add_parity_node(parity_pos, chunk.clone(), parity_strand);
                    self.heal_with_neighbor_data_node(neighbor_chunk, &chunk, pos);
                    return true;
                }
                Err(_) => {
                    if !self
                        .sick_graph
                        .has_parity_node_along_dir(neighbor_pos, parity_strand.into())
                    {
                        self.sick_graph
                            .add_parity_node(parity_pos, Bytes::new(), parity_strand);
                    }
                }
            }
        }
        false
    }

    /// Heals a sick data node using a healthy neighbor data node and a parity node between them.
    fn heal_with_neighbor_data_node(
        &mut self,
        neighbor_chunk: &Bytes,
        parity_chunk: &Bytes,
        pos: Pos,
    ) {
        let healed_data = xor_chunks(neighbor_chunk, parity_chunk);
        self.healthy_graph.add_data_node(pos, healed_data);
        self.sick_graph.remove_data_node(pos);
    }

    async fn load_node(&mut self, node_id: NodeId) -> Result<Bytes, StorageError> {
        let hash = self.get_blob_hash_for_type(node_id.node_type);
        self.storage
            .download_chunk(hash, self.chunk_id_from_pos(node_id.pos))
            .await
    }

    fn chunk_id_from_pos(&self, pos: Pos) -> T::ChunkId {
        let index = self.positioner.pos_to_index(pos);
        self.mapper.index_to_id(index).unwrap()
    }

    fn get_blob_hash_for_type(&self, grid_type: NodeType) -> &str {
        match grid_type {
            NodeType::Data => &self.metadata.orig_hash,
            NodeType::ParityLeft => self.metadata.parity_hashes.get(&StrandType::Left).unwrap(),
            NodeType::ParityHorizontal => self
                .metadata
                .parity_hashes
                .get(&StrandType::Horizontal)
                .unwrap(),
            NodeType::ParityRight => self.metadata.parity_hashes.get(&StrandType::Right).unwrap(),
        }
    }
}
