// Copyright 2024 Entanglement Contributors
// SPDX-License-Identifier: Apache-2.0, MIT

use crate::grid::{Dir, Grid, Pos, Positioner};
use crate::lattice::{DataNode, Graph, GridType, NodeId, ParityNode};
use crate::parity::{ParityGrid, StrandType};
use anyhow::Result;
use chunker::tree::Node;
use storage::{Error as StorageError, Storage};

use crate::Metadata;
use bytes::Bytes;
use std::boxed::Box;
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::marker::PhantomPinned;
use std::pin::Pin;
use std::task::{Context, Poll};

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

    pub async fn repair_chunks(&mut self, chunks: Vec<T::ChunkId>) -> Result<(), StorageError> {
        let healer = Healer::new(
            self.storage.clone(),
            self.metadata.clone(),
            chunks
                .into_iter()
                .map(|c| ChunkInfo::new(c.clone(), self.id_to_pos_map[&c]))
                .collect(),
            self.grid.get_height(),
            self.grid.get_num_items(),
            self.pos_to_id_map.to_owned(),
        );

        tokio::pin!(healer);
        let healthy_chunks = healer.heal().await?;

        for (pos, chunk) in healthy_chunks {
            self.grid.set_cell(pos, chunk);
        }

        Ok(())
    }
}

#[derive(Clone)]
pub struct ChunkInfo<T: Clone> {
    chunk_id: T,
    pos: Pos,
}

impl<T: Clone> ChunkInfo<T> {
    pub fn new(chunk_id: T, pos: Pos) -> Self {
        Self { chunk_id, pos }
    }
}

struct Healer<T: Storage> {
    storage: T,
    sick_graph: Graph,
    healthy_graph: Graph,
    sick_nodes: Vec<Pos>,
    visited: HashSet<NodeId>,
    positioner: Positioner,
    metadata: Metadata,
    pos_to_id_map: HashMap<Pos, T::ChunkId>,
}

impl<T: Storage> Healer<T> {
    fn new(
        storage: T,
        metadata: Metadata,
        sick_chunks: Vec<ChunkInfo<T::ChunkId>>,
        grid_height: usize,
        num_grid_items: usize,
        pos_to_id_map: HashMap<Pos, T::ChunkId>,
    ) -> Self {
        let mut sick_graph = Graph::new(grid_height, num_grid_items);
        let healthy_graph = Graph::new(grid_height, num_grid_items);

        let mut sick_nodes = Vec::with_capacity(sick_chunks.len());

        for chunk in sick_chunks {
            sick_graph.add_data_node(chunk.pos, Bytes::new());
            sick_nodes.push(chunk.pos);
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
        }
    }

    async fn heal(self: Pin<&mut Self>) -> Result<HashMap<Pos, Bytes>, StorageError> {
        let this = self.get_mut();

        let mut res = HashMap::new();
        let mut i = 0;
        while i < this.sick_nodes.len() {
            let pos = this.sick_nodes[i];
            let pinned = Pin::new(&mut *this);
            if !pinned.sick_graph.has_data_node(pos) || pinned.heal_data_node(pos).await {
                this.sick_nodes.swap_remove(i);
                res.insert(
                    pos,
                    this.healthy_graph.get_data_node(pos).unwrap().chunk.clone(),
                );
            } else {
                i += 1;
            }
        }
        Ok(res)
    }

    fn heal_data_node<'a>(
        self: Pin<&'a mut Self>,
        pos: Pos,
    ) -> Pin<Box<dyn Future<Output = bool> + 'a>> {
        Box::pin(async move {
            let this = self.get_mut();
            if this.visited.contains(&NodeId::new(GridType::Data, pos)) {
                return false;
            }
            this.visited.insert(NodeId::new(GridType::Data, pos));

            let missing_neighbors = this.sick_graph.get_missing_neighbors_data_nodes(pos);
            let mut i = 0;
            while i < missing_neighbors.len() {
                let neighbor_pos = this.positioner.normalize(missing_neighbors[i]);
                if let Some(healthy_neighbor) =
                    this.healthy_graph.get_data_node(neighbor_pos).cloned()
                {
                    if this
                        .try_heal_with_neighbor_data_node(
                            pos,
                            neighbor_pos,
                            &healthy_neighbor.chunk,
                        )
                        .await
                    {
                        return true;
                    }
                    i += 1;
                } else {
                    match this
                        .load_node(NodeId::new(GridType::Data, neighbor_pos))
                        .await
                    {
                        Ok(chunk) => this
                            .healthy_graph
                            .add_data_node(neighbor_pos, chunk.clone()),
                        Err(_) => {
                            this.sick_graph.add_data_node(neighbor_pos, Bytes::new());
                            let pin = Pin::new(&mut *this);
                            if !pin.heal_data_node(neighbor_pos).await {
                                i += 1;
                            }
                        }
                    }
                }
            }
            false
        })
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
                    self.sick_graph
                        .add_parity_node(parity_pos, Bytes::new(), parity_strand);
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
