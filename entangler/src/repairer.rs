// Copyright 2024 Entanglement Contributors
// SPDX-License-Identifier: Apache-2.0, MIT

use crate::grid::{Dir, Grid, Pos, Positioner};
use crate::lattice::{Graph, GridType, NodeId};
use crate::parity::StrandType;
use anyhow::Result;
use storage::{Error as StorageError, Storage};

use crate::Metadata;
use bytes::Bytes;
use std::collections::{HashMap, HashSet};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Failed to repair chunks")]
    FailedToRepairChunks,

    #[error("Storage error: {0}")]
    StorageError(#[from] StorageError),

    #[error("Error occurred: {0}")]
    Other(#[source] anyhow::Error),
}

pub struct Repairer<'a, 'b, T: Storage> {
    metadata: Metadata,
    storage: &'a T,
    grid: &'b mut Grid,
    pos_to_id_map: HashMap<Pos, T::ChunkId>,
    id_to_pos_map: HashMap<T::ChunkId, Pos>,
}

fn xor_chunks(chunk1: &Bytes, chunk2: &Bytes) -> Bytes {
    let mut chunk = Vec::with_capacity(chunk1.len());
    for i in 0..chunk1.len() {
        chunk.push(chunk1[i] ^ chunk2[i]);
    }
    Bytes::from(chunk)
}

impl<'a, 'b, T: Storage> Repairer<'a, 'b, T> {
    pub fn new(
        storage: &'a T,
        grid: &'b mut Grid,
        metadata: Metadata,
        pos_to_id_map: HashMap<Pos, T::ChunkId>,
        id_to_pos_map: HashMap<T::ChunkId, Pos>,
    ) -> Self {
        return Self {
            metadata,
            storage,
            grid,
            pos_to_id_map,
            id_to_pos_map,
        };
    }

    pub async fn repair_chunks(&mut self, chunks: Vec<T::ChunkId>) -> Result<(), Error> {
        let mut healer = Healer::new(
            self.storage.clone(),
            self.metadata.clone(),
            chunks.into_iter().map(|c| self.id_to_pos_map[&c]).collect(),
            self.grid.get_height(),
            self.grid.get_num_items(),
            self.pos_to_id_map.to_owned(),
        );

        let healthy_chunks = healer.heal().await?;

        for (pos, chunk) in healthy_chunks {
            self.grid.set_cell(pos, chunk);
        }

        Ok(())
    }
}

struct Healer<T: Storage> {
    storage: T,
    sick_graph: Graph,
    healthy_graph: Graph,
    sick_nodes: Vec<Pos>,
    visited: HashSet<Pos>,
    positioner: Positioner,
    metadata: Metadata,
    pos_to_id_map: HashMap<Pos, T::ChunkId>,
    stack: Vec<(Pos, Vec<Dir>)>,
    result: HashMap<Pos, Bytes>,
}

impl<T: Storage> Healer<T> {
    fn new(
        storage: T,
        metadata: Metadata,
        sick_nodes: Vec<Pos>,
        grid_height: usize,
        num_grid_items: usize,
        pos_to_id_map: HashMap<Pos, T::ChunkId>,
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
            pos_to_id_map,
            stack: Vec::new(),
            result: HashMap::new(),
        }
    }

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

        for pos in self.sick_nodes.to_owned() {
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

    async fn heal_data_node(&mut self, pos: Pos, dirs: Vec<Dir>) -> bool {
        for i in 0..dirs.len() {
            let neighbor_pos = self.positioner.normalize(pos.adjacent(dirs[i]));
            if let Some(healthy_neighbor) = self.healthy_graph.get_data_node(neighbor_pos).cloned()
            {
                if self
                    .try_heal_with_neighbor_data_node(pos, neighbor_pos, &healthy_neighbor.chunk)
                    .await
                {
                    return true;
                }
            } else if let Some(_) = self.sick_graph.get_data_node(neighbor_pos) {
                if !self.visited.contains(&neighbor_pos) {
                    self.visited.insert(neighbor_pos);
                    self.stack.push((pos, dirs[i..].to_vec()));
                    self.stack.push((neighbor_pos, Dir::all().to_vec()));
                    return false;
                }
            } else {
                match self
                    .load_node(NodeId::new(GridType::Data, neighbor_pos))
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
                        if !self.visited.contains(&neighbor_pos) {
                            self.visited.insert(neighbor_pos);
                            self.stack.push((pos, dirs[i..].to_vec()));
                            self.stack.push((neighbor_pos, Dir::all().to_vec()));
                            return false;
                        }
                    }
                }
            }
        }
        false
    }

    async fn try_heal_with_neighbor_data_node(
        &mut self,
        pos: Pos,
        neighbor_pos: Pos,
        neighbor_chunk: &Bytes,
    ) -> bool {
        let parity_dir = self.positioner.determine_dir(pos, neighbor_pos).unwrap();
        if self.sick_graph.has_parity_node_along_dir(pos, parity_dir) {
            return false;
        }
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
            match self
                .load_node(NodeId::new(GridType::from(parity_strand), parity_pos))
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
        return false;
    }

    fn heal_with_neighbor_data_node(
        &mut self,
        neighbor_chunk: &Bytes,
        parity_chunk: &Bytes,
        pos: Pos,
    ) {
        let healed_data = xor_chunks(&neighbor_chunk, parity_chunk);
        self.healthy_graph.add_data_node(pos, healed_data);
        self.sick_graph.remove_data_node(pos);
    }

    async fn load_node(&mut self, node_id: NodeId) -> Result<Bytes, StorageError> {
        let hash = self.get_blob_hash_for_type(node_id.grid_type);
        self.storage
            .download_chunk(hash, self.chunk_id_from_pos(node_id.pos))
            .await
    }

    fn chunk_id_from_pos(&self, pos: Pos) -> T::ChunkId {
        self.pos_to_id_map[&pos].clone()
    }

    fn get_blob_hash_for_type(&self, grid_type: GridType) -> &str {
        match grid_type {
            GridType::Data => &self.metadata.orig_hash,
            GridType::ParityLeft => self.metadata.parity_hashes.get(&StrandType::Left).unwrap(),
            GridType::ParityHorizontal => self
                .metadata
                .parity_hashes
                .get(&StrandType::Horizontal)
                .unwrap(),
            GridType::ParityRight => self.metadata.parity_hashes.get(&StrandType::Right).unwrap(),
        }
    }
}
