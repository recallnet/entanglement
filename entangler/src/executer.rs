// Copyright 2024 Entanglement Contributors
// SPDX-License-Identifier: Apache-2.0, MIT

use crate::grid::{Grid, Pos, Positioner};
use crate::parity::{ParityGrid, StrandType};
use anyhow::Result;
use bytes::Bytes;
use futures::{Stream, StreamExt};
use std::error::Error as StdError;
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
    chunk_size: usize,
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
            leap_window: 0,
            chunk_size: 4, // Default to 4-byte chunks
        }
    }

    /// Sets the leap window size
    pub fn with_leap_window(mut self, leap_window: u64) -> Self {
        self.leap_window = leap_window;
        self
    }

    /// Sets the chunk size
    pub fn with_chunk_size(mut self, chunk_size: usize) -> Self {
        self.chunk_size = chunk_size;
        self
    }

    /// Executes the entanglement process on the given grid.
    /// It produces `alpha` parity grids one by one.
    ///
    /// Note: This method is preserved for compatibility with the existing code.
    pub fn iter_parities(
        &self,
        grid: crate::grid::Grid,
    ) -> impl Iterator<Item = crate::parity::ParityGrid> {
        let strand_types = vec![StrandType::Left, StrandType::Horizontal, StrandType::Right];
        strand_types
            .into_iter()
            .take(self.alpha as usize)
            .map(move |strand_type| create_parity_grid(&grid, strand_type).unwrap())
    }

    /// Entangles the given input stream, producing `alpha` parity streams in parallel.
    /// The parity streams are created according to the strand types.
    pub async fn entangle<S, E>(
        &self,
        input_stream: S,
    ) -> Result<EntanglementStreamResult, EntanglementError>
    where
        S: Stream<Item = Result<Bytes, E>> + Send + Unpin + 'static,
        E: StdError + Send + Sync + 'static,
    {
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
        let strands_for_processing = StrandType::list(self.alpha as usize).unwrap();
        let column_height = (self.leap_window as f64).sqrt() as u64;
        let chunk_size = self.chunk_size;
        tokio::spawn(async move {
            if let Err(e) = process_input_stream(
                input_stream,
                senders,
                strands_for_processing,
                column_height,
                chunk_size,
            )
            .await
            {
                eprintln!("Error processing input stream: {}", e);
            }
        });

        Ok(EntanglementStreamResult {
            parity_streams,
            strand_types: StrandType::list(self.alpha as usize).unwrap(),
        })
    }
}

/// Processes the input stream and generates parity chunks for each strand type
async fn process_input_stream<S, E>(
    mut input: S,
    senders: Vec<mpsc::Sender<Result<Bytes, EntanglementError>>>,
    strand_types: Vec<StrandType>,
    column_height: u64,
    chunk_size: usize,
) -> Result<(), EntanglementError>
where
    S: Stream<Item = Result<Bytes, E>> + Unpin,
    E: StdError + Send + Sync + 'static,
{
    // Buffer to store chunks for processing
    let mut chunk_buffer = Vec::with_capacity((2 * column_height) as usize);
    let mut current_chunk = Vec::with_capacity(chunk_size);
    let mut first_column: Option<Vec<Bytes>> = None;
    let mut total_columns_processed = 0;

    // Read chunks from the input stream
    while let Some(chunk_result) = input.next().await {
        match chunk_result {
            Ok(data) => {
                // Process incoming data byte by byte
                for byte in data.iter() {
                    current_chunk.push(*byte);

                    // When we have a complete chunk
                    if current_chunk.len() == chunk_size {
                        chunk_buffer.push(Bytes::from(current_chunk.clone()));
                        current_chunk.clear();

                        // Store first column when we have enough chunks
                        if first_column.is_none() && chunk_buffer.len() >= column_height as usize {
                            first_column = Some(chunk_buffer[..column_height as usize].to_vec());
                        }

                        // Process chunks when we have enough for two complete columns
                        if chunk_buffer.len() >= 2 * column_height as usize {
                            // Process the first complete column
                            process_columns(
                                &chunk_buffer[..2 * column_height as usize],
                                &senders,
                                &strand_types,
                                column_height,
                            )
                            .await?;
                            // Keep only the last column for next iteration
                            chunk_buffer.drain(0..column_height as usize);
                            total_columns_processed += 1;
                        }
                    }
                }
            }
            Err(e) => {
                let error = EntanglementError::InputError(format!("{}", e));
                for sender in &senders {
                    sender
                        .send(Err(error.clone()))
                        .await
                        .map_err(|_| EntanglementError::SendError)?;
                }
                return Err(error);
            }
        }
    }

    // Handle any remaining data in current_chunk
    if !current_chunk.is_empty() {
        // Pad the last chunk with zeros to reach chunk_size
        while current_chunk.len() < chunk_size {
            current_chunk.push(0);
        }
        chunk_buffer.push(Bytes::from(current_chunk));
    }

    // Process any remaining complete columns
    if chunk_buffer.len() >= 2 * column_height as usize {
        process_columns(
            &chunk_buffer[..2 * column_height as usize],
            &senders,
            &strand_types,
            column_height,
        )
        .await?;
        chunk_buffer.drain(0..column_height as usize);
        total_columns_processed += 1;
    }

    // we might start iterating over the remaining chunks from the middle of the leap window
    // so we need to offset the index
    let index_offset = (total_columns_processed % column_height) * column_height;

    let positioner = Positioner::new(column_height, index_offset + chunk_buffer.len() as u64);

    for (i, chunk) in chunk_buffer.iter().enumerate() {
        let pos = positioner.index_to_pos(i as u64 + index_offset);

        for (j, &strand_type) in strand_types.iter().enumerate() {
            let next_pos = positioner.normalize(pos + strand_type);
            let next_chunk = if next_pos.x == 0 {
                // if x is 0, we should entangle with the first column
                if let Some(first_column) = &first_column {
                    &first_column[next_pos.y as usize]
                } else {
                    // if there is no first column, we deal with very short input data that doesn't fit
                    // into a single column, so it's not possible to entangle it and we return the
                    // chunk as is, so it will entangle with itself and produce bytes of zeros
                    &chunk
                }
            } else {
                let next_index = positioner.pos_to_index(next_pos);
                let buffer_index = next_index - index_offset;
                // if the pair we want to entangle with is within the buffer, we use it
                if buffer_index < chunk_buffer.len() as u64 {
                    &chunk_buffer[(buffer_index) as usize]
                } else if let Some(first_column) = &first_column {
                    // if the pair we want to entangle with is not within the buffer, we need to wrap
                    // around the leap window and use the first column
                    let steps = column_height - (next_pos.x as u64 % column_height);
                    let next_pos = positioner.normalize(next_pos.near(strand_type.into(), steps));
                    &first_column[next_pos.y as usize]
                } else {
                    &chunk
                }
            };

            let parity = entangle_chunks(chunk, next_chunk);
            senders[j]
                .send(Ok(parity))
                .await
                .map_err(|_| EntanglementError::SendError)?;
        }

        if i == column_height as usize - 1 {
            total_columns_processed += 1;
        }
    }

    // Close the channels by dropping the senders
    drop(senders);

    Ok(())
}

async fn process_columns(
    chunk_buffer: &[Bytes],
    senders: &[mpsc::Sender<Result<Bytes, EntanglementError>>],
    strand_types: &[StrandType],
    column_height: u64,
) -> Result<(), EntanglementError> {
    let column_start = chunk_buffer.len() - 2 * column_height as usize;

    // For each position in the first column
    for y in 0..column_height as usize {
        let mut parity_chunks = Vec::with_capacity(strand_types.len());
        let current_idx = column_start + y;

        // Calculate parity for each strand type
        for strand_type in strand_types {
            let next_y = match strand_type {
                StrandType::Left => {
                    if y == 0 {
                        column_height as usize - 1
                    } else {
                        y - 1
                    }
                }
                StrandType::Horizontal => y,
                StrandType::Right => (y + 1) % column_height as usize,
            };

            let next_idx = column_start + column_height as usize + next_y;
            let parity = entangle_chunks(&chunk_buffer[current_idx], &chunk_buffer[next_idx]);
            parity_chunks.push(parity);
        }

        // Send parity chunks to their respective channels
        for (j, chunk) in parity_chunks.into_iter().enumerate() {
            if j < senders.len() {
                senders[j]
                    .send(Ok(chunk))
                    .await
                    .map_err(|_| EntanglementError::SendError)?;
            }
        }
    }

    Ok(())
}

/// For backwards compatibility: entangles two chunks using XOR operation
fn entangle_chunks<T: AsRef<[u8]>>(chunk1: &T, chunk2: &T) -> Bytes {
    let chunk1 = chunk1.as_ref();
    let chunk2 = chunk2.as_ref();
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

                if grid.get_positioner().is_pos_available(next_pos) {
                    let norm_next_pos = grid.get_positioner().normalize(next_pos);
                    parity_grid.set_cell(pos, entangle_chunks(cell, grid.get_cell(norm_next_pos)));
                } else {
                    // we need LW size. At the moment we assume it's square with side equal to grid's height
                    let lw_size = grid.get_height();
                    // calculate the number of steps to go along the strand
                    let steps = lw_size - (next_pos.x as u64 % lw_size);
                    let pair = grid.get_cell(next_pos.near(strand_type.into(), steps));
                    parity_grid.set_cell(pos, entangle_chunks(cell, pair));
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
            Err(std::io::Error::new(std::io::ErrorKind::Other, "test error")),
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

        assert!(
            error_received,
            "Should have received error from parity stream"
        );
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
        assert!(
            parity_chunks.len() >= chunks.len() - 1,
            "Expected at least {} parity chunks but got {}",
            chunks.len() - 1,
            parity_chunks.len()
        );
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

    #[tokio::test]
    async fn test_entangle_fixed_size_chunks() {
        // Create input stream with 8-byte chunks
        // We'll create a 3x6 grid (column_height = 3) to test donut-shaped wrapping
        let input_data = vec![
            // First column (x=0)
            vec![1, 1, 1, 1, 1, 1, 1, 1], // y=0
            vec![2, 2, 2, 2, 2, 2, 2, 2], // y=1
            vec![3, 3, 3, 3, 3, 3, 3, 3], // y=2
            // Second column (x=1)
            vec![4, 4, 4, 4, 4, 4, 4, 4], // y=0
            vec![5, 5, 5, 5, 5, 5, 5, 5], // y=1
            vec![6, 6, 6, 6, 6, 6, 6, 6], // y=2
            // Third column (x=2)
            vec![7, 7, 7, 7, 7, 7, 7, 7], // y=0
            vec![8, 8, 8, 8, 8, 8, 8, 8], // y=1
            vec![9, 9, 9, 9, 9, 9, 9, 9], // y=2
            // Fourth column (x=3)
            vec![10, 10, 10, 10, 10, 10, 10, 10], // y=0
            vec![11, 11, 11, 11, 11, 11, 11, 11], // y=1
            vec![12, 12, 12, 12, 12, 12, 12, 12], // y=2
            // Fifth column (x=4)
            vec![13, 13, 13, 13, 13, 13, 13, 13], // y=0
            vec![14, 14, 14, 14, 14, 14, 14, 14], // y=1
            vec![15, 15, 15, 15, 15, 15, 15, 15], // y=2
            // Sixth column (x=5)
            vec![16, 16, 16, 16, 16, 16, 16, 16], // y=0
            vec![17, 17, 17, 17, 17, 17, 17, 17], // y=1
            vec![18, 18, 18, 18, 18, 18, 18, 18], // y=2
        ];
        let input_stream = stream::iter(
            input_data
                .into_iter()
                .map(|chunk| Ok::<_, std::io::Error>(Bytes::from(chunk))),
        );

        // Create executer with alpha=3, leap_window=9 (3x3 grid), and 8-byte chunks
        let executer = Executer::new(3).with_leap_window(9).with_chunk_size(8);

        // Entangle the stream
        let result = executer.entangle(input_stream).await.unwrap();

        // Verify we got 3 parity streams
        assert_eq!(result.parity_streams.len(), 3);
        assert_eq!(result.strand_types.len(), 3);
        assert_eq!(result.strand_types[0], StrandType::Left);
        assert_eq!(result.strand_types[1], StrandType::Horizontal);
        assert_eq!(result.strand_types[2], StrandType::Right);

        // Collect results from each parity stream
        let mut parity_results = Vec::new();
        for mut stream in result.parity_streams {
            let mut chunks = Vec::new();
            while let Some(chunk_result) = stream.next().await {
                chunks.push(chunk_result.unwrap().to_vec());
            }
            parity_results.push(chunks);
        }

        // Helper function to create expected chunk
        let create_expected = |v1: u8, v2: u8| -> Vec<u8> {
            let chunk1 = vec![v1; 8];
            let chunk2 = vec![v2; 8];
            entangle_chunks(&Bytes::from(chunk1), &Bytes::from(chunk2)).to_vec()
        };

        //  0  1  2  3  4
        // 01 04 07 10 13 ..
        // 02 05 08 11 .. ..
        // 03 06 09 12 .. ..

        // Verify Left strand (moves right and up, wraps at top)
        let left_chunks = &parity_results[0];
        // First column entangles with second column
        assert_eq!(
            left_chunks[0],
            create_expected(1, 6),
            "Left strand (0,0) -> (1,2)"
        );
        assert_eq!(
            left_chunks[1],
            create_expected(2, 4),
            "Left strand (0,1) -> (1,0)"
        );
        assert_eq!(
            left_chunks[2],
            create_expected(3, 5),
            "Left strand (0,2) -> (1,1)"
        );
        // Last column entangles with first column
        assert_eq!(
            left_chunks[15],
            create_expected(16, 3),
            "Left strand (5,0) -> (0,2)"
        );
        assert_eq!(
            left_chunks[16],
            create_expected(17, 1),
            "Left strand (5,1) -> (0,0)"
        );
        assert_eq!(
            left_chunks[17],
            create_expected(18, 2),
            "Left strand (5,2) -> (0,1)"
        );

        // Verify Horizontal strand (moves right only)
        let horizontal_chunks = &parity_results[1];
        // First column entangles with second column
        assert_eq!(
            horizontal_chunks[0],
            create_expected(1, 4),
            "Horizontal strand (0,0) -> (1,0)"
        );
        assert_eq!(
            horizontal_chunks[1],
            create_expected(2, 5),
            "Horizontal strand (0,1) -> (1,1)"
        );
        assert_eq!(
            horizontal_chunks[2],
            create_expected(3, 6),
            "Horizontal strand (0,2) -> (1,2)"
        );
        // Last column entangles with first column
        assert_eq!(
            horizontal_chunks[15],
            create_expected(16, 1),
            "Horizontal strand (5,0) -> (0,0)"
        );
        assert_eq!(
            horizontal_chunks[16],
            create_expected(17, 2),
            "Horizontal strand (5,1) -> (0,1)"
        );
        assert_eq!(
            horizontal_chunks[17],
            create_expected(18, 3),
            "Horizontal strand (5,2) -> (0,2)"
        );

        // Verify Right strand (moves right and down, wraps at bottom)
        let right_chunks = &parity_results[2];
        // First column entangles with second column
        assert_eq!(
            right_chunks[0],
            create_expected(1, 5),
            "Right strand (0,0) -> (1,1)"
        );
        assert_eq!(
            right_chunks[1],
            create_expected(2, 6),
            "Right strand (0,1) -> (1,2)"
        );
        assert_eq!(
            right_chunks[2],
            create_expected(3, 4),
            "Right strand (0,2) -> (1,0)"
        );
        // Last column entangles with first column
        assert_eq!(
            right_chunks[15],
            create_expected(16, 2),
            "Right strand (5,0) -> (0,1)"
        );
        assert_eq!(
            right_chunks[16],
            create_expected(17, 3),
            "Right strand (5,1) -> (0,2)"
        );
        assert_eq!(
            right_chunks[17],
            create_expected(18, 1),
            "Right strand (5,2) -> (0,0)"
        );
    }

    #[tokio::test]
    async fn test_entangle_with_partial_chunks() {
        // Create input data that doesn't align with chunk size
        let input_data = vec![
            vec![1, 2, 3],        // 3 bytes
            vec![4, 5, 6, 7],     // 4 bytes
            vec![8, 9],           // 2 bytes
            vec![10, 11, 12, 13], // 4 bytes
            vec![14, 15, 16], // 3 bytes - this ensures we have enough data for a complete column
        ];
        let input_stream = stream::iter(
            input_data
                .into_iter()
                .map(|chunk| Ok::<_, std::io::Error>(Bytes::from(chunk))),
        );

        // Create executer with alpha=2, leap_window=4 (2x2 grid), and 8-byte chunks
        let executer = Executer::new(2).with_leap_window(4).with_chunk_size(8);

        // Entangle the stream
        let result = executer.entangle(input_stream).await.unwrap();

        // Verify we got 2 parity streams
        assert_eq!(result.parity_streams.len(), 2);
        assert_eq!(result.strand_types.len(), 2);
        assert_eq!(result.strand_types[0], StrandType::Left);
        assert_eq!(result.strand_types[1], StrandType::Horizontal);

        // Collect results from each parity stream
        let mut parity_results = Vec::new();
        for mut stream in result.parity_streams {
            let mut chunks = Vec::new();
            while let Some(chunk_result) = stream.next().await {
                chunks.push(chunk_result.unwrap().to_vec());
            }
            parity_results.push(chunks);
        }

        // Verify each parity stream has chunks of the correct size
        for (i, chunks) in parity_results.iter().enumerate() {
            assert!(!chunks.is_empty(), "Parity stream {} should have chunks", i);
            for (j, chunk) in chunks.iter().enumerate() {
                assert_eq!(
                    chunk.len(),
                    8,
                    "Chunk {} in stream {} should be 8 bytes",
                    j,
                    i
                );
            }
        }
    }

    #[tokio::test]
    async fn test_entangle_error_propagation() {
        // Create a stream that will produce an error after some valid data
        let mut input_data = Vec::new();
        input_data.push(Ok(Bytes::from(vec![1, 2, 3, 4, 5, 6, 7, 8])));
        input_data.push(Ok(Bytes::from(vec![9, 10, 11, 12, 13, 14, 15, 16])));
        input_data.push(Err(std::io::Error::new(
            std::io::ErrorKind::Other,
            "test error",
        )));

        let input_stream = stream::iter(input_data);

        // Create executer with alpha=1 and leap_window=4
        let executer = Executer::new(1).with_leap_window(4);

        // Entangle the stream
        let result = executer.entangle(input_stream).await.unwrap();

        // Get the first (and only) parity stream
        let mut parity_stream = result.parity_streams.into_iter().next().unwrap();

        // Should receive some valid chunks followed by an error
        let mut received_valid_chunks = false;
        let mut received_error = false;
        let mut error_message = String::new();

        while let Some(result) = parity_stream.next().await {
            match result {
                Ok(_) => received_valid_chunks = true,
                Err(e) => {
                    received_error = true;
                    error_message = e.to_string();
                    break;
                }
            }
        }

        assert!(received_valid_chunks, "Should have received valid chunks");
        assert!(received_error, "Should have received error");
        assert!(
            error_message.contains("test error"),
            "Error message should contain 'test error'"
        );
    }

    // Get a chunk to entangle with for a given position and strand type
    fn get_pair_chunk(
        data: &[Vec<u8>],
        positioner: &Positioner,
        pos: Pos,
        strand_type: StrandType,
    ) -> (Vec<u8>, Pos) {
        let mut next_pos = positioner.normalize(pos + strand_type);
        while !positioner.is_pos_available(next_pos) {
            next_pos = positioner.normalize(next_pos + strand_type);
        }
        let next_index = positioner.pos_to_index(next_pos);
        (data[next_index as usize].clone(), next_pos)
    }

    #[tokio::test]
    async fn test_stream_entangle_with_incomplete_leap_window() {
        // Create a stream with 13 chunks (not aligned with leap window of 9)
        let input_data = vec![
            // First column (x=0)
            vec![1, 1, 1, 1, 1, 1, 1, 1], // y=0
            vec![2, 2, 2, 2, 2, 2, 2, 2], // y=1
            vec![3, 3, 3, 3, 3, 3, 3, 3], // y=2
            // Second column (x=1)
            vec![4, 4, 4, 4, 4, 4, 4, 4], // y=0
            vec![5, 5, 5, 5, 5, 5, 5, 5], // y=1
            vec![6, 6, 6, 6, 6, 6, 6, 6], // y=2
            // Third column (x=2)
            vec![7, 7, 7, 7, 7, 7, 7, 7], // y=0
            vec![8, 8, 8, 8, 8, 8, 8, 8], // y=1
            vec![9, 9, 9, 9, 9, 9, 9, 9], // y=2
            // Fourth column (x=3)
            vec![10, 10, 10, 10, 10, 10, 10, 10], // y=0
            vec![11, 11, 11, 11, 11, 11, 11, 11], // y=1
            vec![12, 12, 12, 12, 12, 12, 12, 12], // y=2
            // Fifth column (x=4)
            vec![13, 13, 13, 13, 13, 13, 13, 13], // y=0
            vec![14, 14, 14, 14, 14, 14, 14, 14], // y=1
            vec![15, 15, 15, 15, 15, 15, 15, 15], // y=2
            // Sixth column (x=5)
            vec![16, 16, 16, 16, 16, 16, 16, 16], // y=0
            vec![17, 17, 17, 17, 17, 17, 17, 17], // y=1
            vec![18, 18, 18, 18, 18, 18, 18, 18], // y=2
        ];

        // test with different data lengths
        for data_len in 1..input_data.len() {
            let input_data = input_data[..data_len].to_vec();

            let input_stream = stream::iter(
                input_data
                    .clone()
                    .into_iter()
                    .map(|chunk| Ok::<_, std::io::Error>(Bytes::from(chunk))),
            );

            // Create executer with alpha=3, leap_window=9 (3x3 grid), and 8-byte chunks
            let executer = Executer::new(3).with_leap_window(9).with_chunk_size(8);

            // Entangle the stream
            let result = executer.entangle(input_stream).await.unwrap();

            // Verify we got 3 parity streams
            assert_eq!(result.parity_streams.len(), 3);
            assert_eq!(result.strand_types.len(), 3);
            assert_eq!(result.strand_types[0], StrandType::Left);
            assert_eq!(result.strand_types[1], StrandType::Horizontal);
            assert_eq!(result.strand_types[2], StrandType::Right);

            // Collect results from each parity stream
            let mut parity_results = Vec::new();
            for mut stream in result.parity_streams {
                let mut chunks = Vec::new();
                while let Some(chunk_result) = stream.next().await {
                    chunks.push(chunk_result.unwrap().to_vec());
                }
                parity_results.push(chunks);
            }

            let positioner = Positioner::new(3, data_len as u64);
            for (i, parity_result) in parity_results.iter().enumerate() {
                let strand_type = StrandType::try_from_index(i).unwrap();
                for (j, chunk) in input_data.iter().enumerate() {
                    let pos = positioner.index_to_pos(j as u64);
                    let (pair_chunk, pair_pos) =
                        get_pair_chunk(&input_data, &positioner, pos, strand_type);
                    let expected_chunk = entangle_chunks(chunk, &pair_chunk);
                    assert_eq!(
                        &parity_result[j],
                        &expected_chunk.to_vec(),
                        "Unexpected {} parity chunk at pos {:?}\n\tOriginal chunk: {:?}\n\tPair chunk: {:?} at pos {:?}\n\tExpected chunk: {:?}",
                        strand_type,
                        pos,
                        chunk,
                        pair_chunk,
                        pair_pos,
                        expected_chunk.to_vec()
                    );
                }
            }
        }
    }
}
