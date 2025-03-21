// Copyright 2024 Entanglement Contributors
// SPDX-License-Identifier: Apache-2.0, MIT

use crate::grid::{self, Grid, Pos};
use crate::parity::{ParityGrid, StrandType};
use anyhow::Result;
use bytes::Bytes;
use futures::{Stream, StreamExt};
use std::error::Error as StdError;
use std::fmt;
use std::pin::Pin;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

/// Stream type for handling bytes with possible errors
pub type ByteStream<E> = Pin<Box<dyn Stream<Item = Result<Bytes, E>> + Send + Unpin + 'static>>;

/// The executer executes the entanglement process with streaming data.
/// It creates `alpha` parity streams in parallel.
pub struct Executer {
    alpha: u8,
    leap_window: u64,
}

/// Result of the entanglement process
pub struct EntanglementStreamResult {
    /// Array of parity streams, one for each strand type
    pub parity_streams: Vec<ByteStream<EntanglementError>>,
    /// The strand types corresponding to the streams
    pub strand_types: Vec<StrandType>,
}

/// A custom error type for entanglement operations
#[derive(thiserror::Error, Debug, Clone)]
pub enum EntanglementError {
    #[error("Failed to send chunk to channel")]
    SendError,
    
    #[error("Input stream error: {0}")]
    InputError(String),
    
    #[error("Task join error")]
    TaskJoinError,
    
    #[error("Storage error: {0}")]
    StorageError(String),
}

impl Executer {
    /// Create a new executer with the given alpha and leap window.
    /// - alpha: The number of parity streams to create
    /// - leap_window: The size of the leap window, which determines the wrapping behavior
    pub fn new(alpha: u8) -> Self {
        Self { 
            alpha,
            leap_window: 1, // Default leap window size, will be updated during entanglement
        }
    }
    
    /// Sets the leap window size
    pub fn with_leap_window(mut self, leap_window: u64) -> Self {
        self.leap_window = leap_window;
        self
    }
    
    /// Executes the entanglement process on the given grid.
    /// It produces `alpha` parity grids one by one.
    /// 
    /// Note: This method is preserved for compatibility with the existing code.
    pub fn iter_parities(&self, grid: crate::grid::Grid) -> impl Iterator<Item = crate::parity::ParityGrid> {
        let strand_types = vec![StrandType::Left, StrandType::Horizontal, StrandType::Right];
        strand_types
            .into_iter()
            .take(self.alpha as usize)
            .map(move |strand_type| create_parity_grid(&grid, strand_type).unwrap())
    }
    /// Entangles the given input stream, producing `alpha` parity streams in parallel.
    /// The parity streams are created according to the strand types.
    pub async fn entangle<S, E>(&self, input_stream: S) -> Result<EntanglementStreamResult, EntanglementError>
    where
        S: Stream<Item = Result<Bytes, E>> + Send + Unpin + 'static,
        E: StdError + Send + Sync + 'static,
    {
        let strand_types = vec![StrandType::Left, StrandType::Horizontal, StrandType::Right];
        let selected_strands = strand_types.iter()
            .take(self.alpha as usize)
            .cloned()
            .collect::<Vec<_>>();

        // Create channels for each parity stream
        let mut receivers = Vec::with_capacity(self.alpha as usize);
        let mut senders = Vec::with_capacity(self.alpha as usize);

        for _ in 0..self.alpha {
            let (tx, rx) = mpsc::channel::<Result<Bytes, EntanglementError>>(100);
            senders.push(tx);
            receivers.push(rx);
        }

        // Create output streams
        let parity_streams = receivers
            .into_iter()
            .map(|rx| {
                let stream = ReceiverStream::new(rx);
                Box::pin(stream) as ByteStream<EntanglementError>
            })
            .collect::<Vec<_>>();

        // Process the input stream in a separate task to avoid blocking
        let leap_window = self.leap_window;
        let strands_for_processing = selected_strands.clone();
        tokio::spawn(async move {
            if let Err(e) = process_input_stream(input_stream, senders, strands_for_processing, leap_window).await {
                eprintln!("Error processing input stream: {}", e);
            }
        });

        Ok(EntanglementStreamResult {
            parity_streams,
            strand_types: selected_strands,
        })
    }
}

/// Processes the input stream and generates parity chunks for each strand type
async fn process_input_stream<S, E>(
    mut input: S,
    senders: Vec<mpsc::Sender<Result<Bytes, EntanglementError>>>,
    strand_types: Vec<StrandType>,
    leap_window: u64,
) -> Result<(), EntanglementError>
where
    S: Stream<Item = Result<Bytes, E>> + Unpin,
    E: StdError + Send + Sync + 'static,
{
    // We need to maintain a buffer of chunks for each strand
    // The buffer size depends on the leap window
    let buffer_size = leap_window as usize;
    let mut chunk_buffer = Vec::with_capacity(buffer_size);
    
    // Read chunks from the input stream
    while let Some(chunk_result) = input.next().await {
        match chunk_result {
            Ok(current_chunk) => {
                // Add the current chunk to the buffer
                chunk_buffer.push(current_chunk);
                
                // Once we have enough chunks in the buffer, we can start producing parity chunks
                if chunk_buffer.len() >= 2 {
                    let mut parity_chunks = Vec::with_capacity(strand_types.len());
                    
                    // Generate parity chunks for each strand type
                    for strand_type in &strand_types {
                        let last_idx = chunk_buffer.len() - 1;
                        let prev_idx = match strand_type {
                            StrandType::Left => {
                                // For Left strand (diagonal up), we need to wrap around
                                // if at the top of the column
                                if last_idx % buffer_size == 0 {
                                    if last_idx >= buffer_size {
                                        // We have a previous column, wrap to the bottom
                                        last_idx - 1
                                    } else {
                                        // Safeguard for the first column
                                        last_idx
                                    }
                                } else {
                                    // Normal case - move up one row
                                    last_idx - 1
                                }
                            },
                            StrandType::Horizontal => {
                                // For Horizontal strand, we use the chunk at the same height
                                // in the previous column
                                if last_idx >= buffer_size {
                                    // We have a previous column
                                    last_idx - buffer_size
                                } else {
                                    // Safeguard for the first column
                                    last_idx
                                }
                            },
                            StrandType::Right => {
                                // For Right strand (diagonal down), we need to wrap around
                                // if at the bottom of the column
                                if last_idx % buffer_size == buffer_size - 1 {
                                    if last_idx >= buffer_size {
                                        // We have a previous column, wrap to the top
                                        last_idx - (buffer_size - 1)
                                    } else {
                                        // Safeguard for the first column
                                        last_idx
                                    }
                                } else {
                                    // Normal case - move down one row
                                    last_idx + 1
                                }
                            }
                        };
                        
                        // Make sure the previous index is within bounds
                        let prev_idx = prev_idx % chunk_buffer.len();
                        
                        // Create the parity chunk by entangling the current and previous chunks
                        let parity = entangle_chunks(&chunk_buffer[prev_idx], &chunk_buffer[last_idx]);
                        parity_chunks.push(parity);
                    }
                    
                    // Send parity chunks to their respective channels
                    for (i, chunk) in parity_chunks.into_iter().enumerate() {
                        if i < senders.len() {
                            senders[i].send(Ok(chunk))
                                .await
                                .map_err(|_| EntanglementError::SendError)?;
                        }
                    }
                    
                    // Remove older chunks to keep the buffer size manageable
                    // We need to keep at least buffer_size chunks in the buffer
                    if chunk_buffer.len() > 2 * buffer_size {
                        chunk_buffer.drain(0..buffer_size);
                    }
                }
            },
            Err(e) => {
                // Convert external error to our error type
                let error = EntanglementError::InputError(format!("{}", e));
                
                // Forward the error to all senders
                for sender in &senders {
                    sender.send(Err(error.clone()))
                        .await
                        .map_err(|_| EntanglementError::SendError)?;
                }
                
                return Err(error);
            }
        }
    }
    
    // Handle wrapping around - entangle the last chunks with the first chunks
    if chunk_buffer.len() >= 2 {
        for i in 0..buffer_size {
            if i + buffer_size >= chunk_buffer.len() {
                break;
            }
            
            let mut parity_chunks = Vec::with_capacity(strand_types.len());
            
            for strand_type in &strand_types {
                let last_idx = i + buffer_size;
                let first_idx = match strand_type {
                    StrandType::Left => {
                        if i % buffer_size == 0 {
                            // We're at the top of the column, wrap to the bottom
                            if i + buffer_size - 1 < chunk_buffer.len() {
                                i + buffer_size - 1
                            } else {
                                i  // Failsafe, just use self
                            }
                        } else {
                            // Normal case, move up one row
                            i - 1
                        }
                    },
                    StrandType::Horizontal => i,  // Same row in previous column
                    StrandType::Right => {
                        if i % buffer_size == buffer_size - 1 {
                            // We're at the bottom of the column, wrap to the top
                            if i >= buffer_size {
                                i - buffer_size + 1
                            } else {
                                i  // Failsafe, just use self
                            }
                        } else {
                            // Normal case, move down one row
                            i + 1
                        }
                    }
                };
                
                // Make sure the index is within bounds
                let first_idx = first_idx % chunk_buffer.len();
                
                // Create the parity chunk
                let parity = entangle_chunks(&chunk_buffer[first_idx], &chunk_buffer[last_idx]);
                parity_chunks.push(parity);
            }
            
            // Send parity chunks to their respective channels
            for (j, chunk) in parity_chunks.into_iter().enumerate() {
                if j < senders.len() {
                    senders[j].send(Ok(chunk))
                        .await
                        .map_err(|_| EntanglementError::SendError)?;
                }
            }
        }
    }
    
    // Close the channels by dropping the senders
    drop(senders);
    
    Ok(())
}

/// Entangles two chunks using XOR operation for stream processing
fn entangle_chunks(chunk1: &Bytes, chunk2: &Bytes) -> Bytes {
    let mut result = Vec::with_capacity(chunk1.len());
    for i in 0..chunk1.len() {
        result.push(chunk1[i] ^ chunk2[i % chunk2.len()]);
    }
    Bytes::from(result)
}

/// For backwards compatibility: entangles two chunks using XOR operation
fn entangle_grid_chunks(chunk1: &Bytes, chunk2: &Bytes) -> Bytes {
    let mut chunk = Vec::with_capacity(chunk1.len());
    for i in 0..chunk1.len() {
        chunk.push(chunk1[i] ^ chunk2[i]);
    }
    Bytes::from(chunk)
}

/// Create a parity grid from the given grid and strand type.
/// The parity grid is created by entangling each cell with the next cell along the strand.
/// The last cells along x axis are entangled with the first cells forming toroidal lattice.
fn create_parity_grid(grid: &Grid, strand_type: StrandType) -> Result<ParityGrid> {
    let mut parity_grid = Grid::new_empty(grid.get_num_items(), grid.get_height())?;
    for x in 0..grid.get_width() {
        for y in 0..grid.get_height() {
            let pos = Pos::new(x, y);
            if let Some(cell) = grid.try_get_cell(pos) {
                let next_pos = pos + strand_type;
                if let Some(pair) = grid.try_get_cell(next_pos) {
                    parity_grid.set_cell(pos, entangle_grid_chunks(cell, pair));
                } else {
                    // we need LW size. At the moment we assume it's square with side equal to grid's height
                    let lw_size = grid.get_height();
                    // calculate the number of steps to go along the strand
                    let steps = lw_size - (next_pos.x as u64 % lw_size);
                    let pair = grid.get_cell(next_pos.near(strand_type.into(), steps));
                    parity_grid.set_cell(pos, entangle_grid_chunks(cell, pair));
                }
            }
        }
    }
    Ok(ParityGrid {
        grid: parity_grid,
        strand_type,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::stream;
    use tokio_stream::StreamExt;

    // Helper function to create a stream of bytes
    fn create_test_stream(data: Vec<Vec<u8>>) -> impl Stream<Item = Result<Bytes, std::io::Error>> {
        stream::iter(data.into_iter().map(|chunk| Ok(Bytes::from(chunk))))
    }

    #[tokio::test]
    async fn test_stream_entangle_simple() {
        // Create a simple stream with two chunks
        let chunk1 = vec![1, 2, 3, 4];
        let chunk2 = vec![5, 6, 7, 8];
        let input_stream = create_test_stream(vec![chunk1.clone(), chunk2.clone()]);
        
        // Create an executer with alpha=3 (all strand types)
        let executer = Executer::new(3).with_leap_window(1);
        
        // Entangle the stream
        let result = executer.entangle(input_stream).await.unwrap();
        
        // Verify we got 3 parity streams (Left, Horizontal, Right)
        assert_eq!(result.parity_streams.len(), 3);
        assert_eq!(result.strand_types.len(), 3);
        assert_eq!(result.strand_types[0], StrandType::Left);
        assert_eq!(result.strand_types[1], StrandType::Horizontal);
        assert_eq!(result.strand_types[2], StrandType::Right);
        
        // Collect from each parity stream to verify contents
        let mut parity_results = Vec::new();
        for (i, mut stream) in result.parity_streams.into_iter().enumerate() {
            let mut chunks = Vec::new();
            while let Some(chunk_result) = stream.next().await {
                chunks.push(chunk_result.unwrap().to_vec());
            }
            // Each strand has at least one chunk for this simple case
            assert!(!chunks.is_empty(), "Parity stream {} should have chunks", i);
            parity_results.push(chunks);
        }
        
        // Each parity stream has its own generation function, so we just verify they exist
        // and have valid data (non-empty and right size)
        for (i, chunks) in parity_results.iter().enumerate() {
            for chunk in chunks {
                assert_eq!(chunk.len(), 4, "Parity chunk {} should be 4 bytes long", i);
            }
        }
    }

    #[tokio::test]
    async fn test_stream_entangle_with_error() {
        // Create a stream that yields an error
        let error_stream = stream::iter(vec![
            Ok(Bytes::from(vec![1, 2, 3, 4])),
            Ok(Bytes::from(vec![5, 6, 7, 8])),
            Err(std::io::Error::new(std::io::ErrorKind::Other, "test error"))
        ]);
        
        // Create an executer
        let executer = Executer::new(1).with_leap_window(1);
        
        // Entangle the stream
        let result = executer.entangle(error_stream).await.unwrap();
        
        // Verify we got 1 parity stream
        assert_eq!(result.parity_streams.len(), 1);
        
        // Collect from the parity stream to verify we get the error
        let mut stream = result.parity_streams.into_iter().next().unwrap();
        let mut error_received = false;
        
        while let Some(result) = stream.next().await {
            if result.is_err() {
                error_received = true;
                break;
            }
        }
        
        assert!(error_received, "Should have received error from parity stream");
    }

    #[tokio::test]
    async fn test_stream_entangle_multiple_chunks() {
        // Create a stream with multiple chunks
        let chunks = vec![
            vec![1, 2, 3, 4],
            vec![5, 6, 7, 8],
            vec![9, 10, 11, 12],
            vec![13, 14, 15, 16],
            vec![17, 18, 19, 20],
        ];
        let input_stream = create_test_stream(chunks.clone());
        
        // Create an executer with just one strand type for simplicity
        let executer = Executer::new(1).with_leap_window(2);
        
        // Entangle the stream
        let result = executer.entangle(input_stream).await.unwrap();
        
        // Verify we got 1 parity stream
        assert_eq!(result.parity_streams.len(), 1);
        
        // Collect from the parity stream
        let mut stream = result.parity_streams.into_iter().next().unwrap();
        let mut parity_chunks = Vec::new();
        
        while let Some(result) = stream.next().await {
            parity_chunks.push(result.unwrap().to_vec());
        }
        
        // We should get at least chunks.len() - 1 parity chunks
        assert!(parity_chunks.len() >= chunks.len() - 1, 
            "Expected at least {} parity chunks but got {}", 
            chunks.len() - 1, parity_chunks.len());
    }
    
    use crate::grid::Grid;
    use bytes::Bytes;
    use std::str;

    const WIDTH: u64 = 3;
    const HEIGHT: u64 = 3;

    fn create_chunks() -> Vec<Bytes> {
        vec![
            Bytes::from("a"),
            Bytes::from("b"),
            Bytes::from("c"),
            Bytes::from("d"),
            Bytes::from("e"),
            Bytes::from("f"),
            Bytes::from("g"),
            Bytes::from("h"),
            Bytes::from("i"),
        ]
    }

    fn create_num_chunks(num_chunks: usize) -> Vec<Bytes> {
        let mut chunks = Vec::with_capacity(num_chunks);
        for i in 0..num_chunks {
            let ch = vec![i as u8; 1];
            chunks.push(Bytes::from(ch));
        }
        chunks
    }

    fn entangle(s1: &str, s2: &str) -> Bytes {
        entangle_chunks(&Bytes::from(s1.to_string()), &Bytes::from(s2.to_string()))
    }

    fn entangle_by_pos(x: u64, y: u64, dy: i64) -> Bytes {
        const CHARS: &str = "abcdefghi";
        let pos1 = x * HEIGHT + y;
        // calculate y of the second cell relative to y of the first cell
        // we add HEIGHT in case y is negative
        let y2 = ((y + HEIGHT) as i64 + dy) as u64 % HEIGHT;
        let pos2 = ((x + 1) % WIDTH) * HEIGHT + y2;
        let ch1 = &CHARS[pos1 as usize..pos1 as usize + 1];
        let ch2 = &CHARS[pos2 as usize..pos2 as usize + 1];
        entangle(ch1, ch2)
    }

    fn assert_parity_grid(parity_grid: &ParityGrid) {
        let dy = parity_grid.strand_type.to_i64();

        let g = &parity_grid.grid;
        assert_eq!(g.get_cell(Pos::new(0, 0)), &entangle_by_pos(0, 0, dy));
        assert_eq!(g.get_cell(Pos::new(0, 1)), &entangle_by_pos(0, 1, dy));
        assert_eq!(g.get_cell(Pos::new(0, 2)), &entangle_by_pos(0, 2, dy));
        assert_eq!(g.get_cell(Pos::new(1, 0)), &entangle_by_pos(1, 0, dy));
        assert_eq!(g.get_cell(Pos::new(1, 1)), &entangle_by_pos(1, 1, dy));
        assert_eq!(g.get_cell(Pos::new(1, 2)), &entangle_by_pos(1, 2, dy));
        assert_eq!(g.get_cell(Pos::new(2, 0)), &entangle_by_pos(2, 0, dy));
        assert_eq!(g.get_cell(Pos::new(2, 1)), &entangle_by_pos(2, 1, dy));
        assert_eq!(g.get_cell(Pos::new(2, 2)), &entangle_by_pos(2, 2, dy));
    }

    fn find_next_cell_along_strand(grid: &Grid, pos: Pos, strand_type: StrandType) -> &Bytes {
        let mut next_pos = pos + strand_type;
        while grid.try_get_cell(next_pos).is_none() {
            next_pos += strand_type;
        }
        grid.get_cell(next_pos)
    }

    #[test]
    fn test_entangle_chunks() {
        let chunk1 = Bytes::from(vec![0b00000000, 0b11110000]);
        let chunk2 = Bytes::from(vec![0b10101010, 0b00001111]);
        let expected = Bytes::from(vec![0b10101010, 0b11111111]);
        let result = entangle_chunks(&chunk1, &chunk2);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_create_parity_grid() {
        let grid = Grid::new(create_chunks(), 3).unwrap();

        let parity_grid = create_parity_grid(&grid, StrandType::Left).unwrap();
        assert_parity_grid(&parity_grid);

        let parity_grid = create_parity_grid(&grid, StrandType::Horizontal).unwrap();
        assert_parity_grid(&parity_grid);

        let parity_grid = create_parity_grid(&grid, StrandType::Right).unwrap();
        assert_parity_grid(&parity_grid);
    }

    #[test]
    fn test_if_lw_is_not_complete_it_wraps_and_entangles_correctly() {
        const DIM: u64 = 5;

        // LW is 25-chunks-aligned (5 * 5). We want to have first LW complete and second LW incomplete.
        // We test different variations of incomplete LWs.
        // For the last case (50 chunks) we have the second LW complete as well.
        for num_chunks in 26..50 {
            let grid = Grid::new(create_num_chunks(num_chunks), DIM)
                .unwrap_or_else(|_| panic!("failed to create grid for num chunks: {}", num_chunks));

            let parity_grid = create_parity_grid(&grid, StrandType::Left).unwrap_or_else(|_| {
                panic!(
                    "failed to create parity grid for num chunks: {}",
                    num_chunks
                )
            });

            let st = parity_grid.strand_type;

            // we don't need to start from 0 as we know the first LW is complete and first 20 chunks
            // (a.k.a. first 4 columns) have adjacent pair cell.
            for chunk_index in 20..num_chunks as u64 {
                let x = chunk_index / DIM;
                let y = chunk_index % DIM;
                let pos = Pos::new(x, y);
                let cell = grid.get_cell(pos);
                let pair_cell = find_next_cell_along_strand(&grid, pos, st);

                let entangled_cell = entangle_chunks(cell, pair_cell);
                assert_eq!(
                    entangled_cell,
                    parity_grid.grid.get_cell(pos),
                    "num_chunks: {}, chunk index: {}",
                    num_chunks,
                    chunk_index
                );
            }
        }
    }

    #[test]
    fn test_execute() {
        let grid = Grid::new(create_chunks(), 3).unwrap();

        let executer = Executer::new(3);
        let parities: Vec<_> = executer.iter_parities(grid.clone()).collect();

        assert_eq!(grid.get_cell(Pos::new(0, 0)), &Bytes::from("a"));
        assert_eq!(grid.get_cell(Pos::new(0, 1)), &Bytes::from("b"));
        assert_eq!(grid.get_cell(Pos::new(0, 2)), &Bytes::from("c"));
        assert_eq!(grid.get_cell(Pos::new(1, 0)), &Bytes::from("d"));
        assert_eq!(grid.get_cell(Pos::new(1, 1)), &Bytes::from("e"));
        assert_eq!(grid.get_cell(Pos::new(1, 2)), &Bytes::from("f"));
        assert_eq!(grid.get_cell(Pos::new(2, 0)), &Bytes::from("g"));
        assert_eq!(grid.get_cell(Pos::new(2, 1)), &Bytes::from("h"));
        assert_eq!(grid.get_cell(Pos::new(2, 2)), &Bytes::from("i"));

        assert_eq!(parities.len(), 3);

        let lh_strand = parities.first().unwrap();
        assert_parity_grid(lh_strand);

        let h_strand = parities.get(1).unwrap();
        assert_parity_grid(h_strand);

        let rh_strand = parities.get(2).unwrap();
        assert_parity_grid(rh_strand);
    }

    #[test]
    fn test_execute_with_unaligned_leap_window() {
        let grid = Grid::new(create_chunks(), 4).unwrap();

        let executer = Executer::new(3);
        let parities: Vec<_> = executer.iter_parities(grid.clone()).collect();

        assert_eq!(grid.get_cell(Pos::new(0, 0)), &Bytes::from("a"));
        assert_eq!(grid.get_cell(Pos::new(0, 1)), &Bytes::from("b"));
        assert_eq!(grid.get_cell(Pos::new(0, 2)), &Bytes::from("c"));
        assert_eq!(grid.get_cell(Pos::new(0, 3)), &Bytes::from("d"));

        assert_eq!(grid.get_cell(Pos::new(1, 0)), &Bytes::from("e"));
        assert_eq!(grid.get_cell(Pos::new(1, 1)), &Bytes::from("f"));
        assert_eq!(grid.get_cell(Pos::new(1, 2)), &Bytes::from("g"));
        assert_eq!(grid.get_cell(Pos::new(1, 3)), &Bytes::from("h"));

        assert_eq!(grid.get_cell(Pos::new(2, 0)), &Bytes::from("i"));

        assert_eq!(parities.len(), 3);

        let lh_strand = parities.first().unwrap();
        assert_eq!(lh_strand.strand_type, StrandType::Left);
        assert_eq!(lh_strand.grid.get_cell(Pos::new(0, 0)), &entangle("a", "h"));
        assert_eq!(lh_strand.grid.get_cell(Pos::new(0, 1)), &entangle("b", "e"));
        assert_eq!(lh_strand.grid.get_cell(Pos::new(0, 2)), &entangle("c", "f"));
        assert_eq!(lh_strand.grid.get_cell(Pos::new(0, 3)), &entangle("d", "g"));
        assert_eq!(lh_strand.grid.get_cell(Pos::new(1, 0)), &entangle("e", "b"));
        assert_eq!(lh_strand.grid.get_cell(Pos::new(1, 1)), &entangle("f", "i"));
        assert_eq!(lh_strand.grid.get_cell(Pos::new(1, 2)), &entangle("g", "d"));
        assert_eq!(lh_strand.grid.get_cell(Pos::new(1, 3)), &entangle("h", "a"));
        assert_eq!(lh_strand.grid.get_cell(Pos::new(2, 0)), &entangle("i", "c"));

        let h_strand = parities.get(1).unwrap();
        assert_eq!(h_strand.strand_type, StrandType::Horizontal);
        assert_eq!(h_strand.grid.get_cell(Pos::new(0, 0)), &entangle("a", "e"));
        assert_eq!(h_strand.grid.get_cell(Pos::new(0, 1)), &entangle("b", "f"));
        assert_eq!(h_strand.grid.get_cell(Pos::new(0, 2)), &entangle("c", "g"));
        assert_eq!(h_strand.grid.get_cell(Pos::new(0, 3)), &entangle("d", "h"));
        assert_eq!(h_strand.grid.get_cell(Pos::new(1, 0)), &entangle("e", "i"));
        assert_eq!(h_strand.grid.get_cell(Pos::new(1, 1)), &entangle("f", "b"));
        assert_eq!(h_strand.grid.get_cell(Pos::new(1, 2)), &entangle("g", "c"));
        assert_eq!(h_strand.grid.get_cell(Pos::new(1, 3)), &entangle("h", "d"));
        assert_eq!(h_strand.grid.get_cell(Pos::new(2, 0)), &entangle("i", "a"));

        let rh_strand = parities.get(2).unwrap();
        assert_eq!(rh_strand.strand_type, StrandType::Right);
        assert_eq!(rh_strand.grid.get_cell(Pos::new(0, 0)), &entangle("a", "f"));
        assert_eq!(rh_strand.grid.get_cell(Pos::new(0, 1)), &entangle("b", "g"));
        assert_eq!(rh_strand.grid.get_cell(Pos::new(0, 2)), &entangle("c", "h"));
        assert_eq!(rh_strand.grid.get_cell(Pos::new(0, 3)), &entangle("d", "e"));
        assert_eq!(rh_strand.grid.get_cell(Pos::new(1, 0)), &entangle("e", "d"));
        assert_eq!(rh_strand.grid.get_cell(Pos::new(1, 1)), &entangle("f", "a"));
        assert_eq!(rh_strand.grid.get_cell(Pos::new(1, 2)), &entangle("g", "b"));
        assert_eq!(rh_strand.grid.get_cell(Pos::new(1, 3)), &entangle("h", "i"));
        assert_eq!(rh_strand.grid.get_cell(Pos::new(2, 0)), &entangle("i", "c"));
    }

    #[test]
    fn test_execute_with_large_data() {
        // we choose 18 chunks of 8 bytes each and create a grid with 3x6 cells
        // so that there are no holes in the grid and we can safely call get_cell
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
            Bytes::from(vec![0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88]),
            Bytes::from(vec![0x99, 0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF, 0x00]),
            Bytes::from(vec![0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC, 0xDE, 0xF0]),
            Bytes::from(vec![0x0F, 0x1E, 0x2D, 0x3C, 0x4B, 0x5A, 0x69, 0x78]),
            Bytes::from(vec![0x87, 0x96, 0xA5, 0xB4, 0xC3, 0xD2, 0xE1, 0xF0]),
            Bytes::from(vec![0x1A, 0x2B, 0x3C, 0x4D, 0x5E, 0x6F, 0x70, 0x81]),
        ];
        const HEIGHT: u64 = 3;
        const WIDTH: u64 = 6;
        let grid = Grid::new(chunks.clone(), HEIGHT).unwrap();

        let executer = Executer::new(3);
        let parities: Vec<_> = executer.iter_parities(grid.clone()).collect();

        assert_eq!(parities.len() as u64, HEIGHT);

        for parity_grid in parities {
            let assembled_data = parity_grid.grid.assemble_data();
            assert_eq!(assembled_data.len() as u64, WIDTH * HEIGHT * 8);

            for x in 0..WIDTH as i64 {
                for y in 0..HEIGHT as i64 {
                    let pos = Pos::new(x, y);
                    let orig_cell1 = grid.get_cell(pos);
                    let orig_cell2 = grid.get_cell(pos + parity_grid.strand_type);
                    let expected = entangle_chunks(orig_cell1, orig_cell2);
                    let cell = parity_grid.grid.get_cell(pos);
                    assert_eq!(
                        cell,
                        &expected,
                        "Parity grid mismatch at coordinate ({}, {})\n\
                         Parity type: {:?}\n\
                         Actual parity cell value: {:?}\n\
                         Expected parity cell value: {:?}\n\
                         Original grid cell 1 at {:?}: {:?}\n\
                         Original grid cell 2 at {:?}: {:?}",
                        x,
                        y,
                        parity_grid.strand_type,
                        cell,
                        expected,
                        pos,
                        orig_cell1,
                        pos + parity_grid.strand_type,
                        orig_cell2
                    );
                }
            }
        }
    }
}
