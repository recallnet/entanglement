// Copyright 2024 Entanglement Contributors
// SPDX-License-Identifier: Apache-2.0, MIT

use crate::grid::Positioner;
use crate::parity::StrandType;
use crate::Config;
use anyhow::Result;
use bytes::Bytes;
use futures::{Stream, StreamExt};
use std::pin::Pin;
use tokio::sync::mpsc::{self, error::SendError};
use tokio_stream::wrappers::ReceiverStream;

/// Stream type for handling bytes with possible errors
pub type ByteStream<E> = Pin<Box<dyn Stream<Item = Result<Bytes, E>> + Send + Unpin + 'static>>;

/// The executer executes the entanglement process with streaming data.
/// It creates `alpha` parity streams in parallel.
pub struct Executer {
    alpha: u8,
    height: u64,
    chunk_size: usize,
    channel_buffer_size: usize,
}

/// A custom error type for entanglement operations
#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Failed to send chunk to channel: {source}")]
    SendError {
        #[source]
        source: anyhow::Error,
    },

    #[error("Input stream error: {source}")]
    InputError {
        #[source]
        source: anyhow::Error,
    },

    #[error("Failed to process chunks: {source}")]
    ProcessingError {
        #[source]
        source: anyhow::Error,
    },
}

impl<T> From<SendError<T>> for Error {
    fn from(err: SendError<T>) -> Self {
        Error::SendError {
            source: anyhow::anyhow!("Failed to send chunk: {}", err),
        }
    }
}

impl Clone for Error {
    fn clone(&self) -> Self {
        match self {
            Error::SendError { source } => Error::SendError {
                source: anyhow::anyhow!(source.to_string()),
            },
            Error::InputError { source } => Error::InputError {
                source: anyhow::anyhow!(source.to_string()),
            },
            Error::ProcessingError { source } => Error::ProcessingError {
                source: anyhow::anyhow!(source.to_string()),
            },
        }
    }
}

impl Executer {
    /// Sets the chunk size
    pub fn with_chunk_size(mut self, chunk_size: usize) -> Self {
        self.chunk_size = chunk_size;
        self
    }

    /// Creates a new executer from a Config
    pub fn from_config(config: &Config) -> Self {
        Self {
            alpha: config.alpha,
            height: config.s as u64,
            chunk_size: 1024, // Keep default chunk size
            channel_buffer_size: config.channel_buffer_size,
        }
    }

    /// Entangles the given input stream, producing `alpha` parity streams in parallel.
    /// The parity streams are created according to the strand types.
    pub async fn entangle<S, E>(&self, input_stream: S) -> Result<Vec<ByteStream<Error>>, Error>
    where
        S: Stream<Item = Result<Bytes, E>> + Send + Unpin + 'static,
        E: std::error::Error + Send + Sync + 'static,
    {
        let mut receivers = Vec::with_capacity(self.alpha as usize);
        let mut senders = Vec::with_capacity(self.alpha as usize);

        for _ in 0..self.alpha {
            let (tx, rx) = mpsc::channel::<Result<Bytes, Error>>(self.channel_buffer_size);
            senders.push(tx);
            receivers.push(rx);
        }

        let parity_streams = receivers
            .into_iter()
            .map(|rx| {
                let stream = ReceiverStream::new(rx);
                Box::pin(stream) as ByteStream<Error>
            })
            .collect::<Vec<_>>();

        // Process the input stream in a separate task to avoid blocking
        let strands_for_processing = StrandType::list(self.alpha as usize).unwrap();
        let chunk_size = self.chunk_size;
        let column_height = self.height;
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

        Ok(parity_streams)
    }
}

/// Processes the input stream and generates parity chunks for each strand type
async fn process_input_stream<S, E>(
    mut input_stream: S,
    senders: Vec<mpsc::Sender<Result<Bytes, Error>>>,
    strand_types: Vec<StrandType>,
    column_height: u64,
    chunk_size: usize,
) -> Result<(), Error>
where
    S: Stream<Item = Result<Bytes, E>> + Unpin,
    E: std::error::Error + Send + Sync + 'static,
{
    let mut chunk_buffer = Vec::with_capacity((2 * column_height) as usize);
    let mut current_chunk = Vec::with_capacity(chunk_size);
    let mut first_column: Option<Vec<Bytes>> = None;
    let mut total_columns_processed = 0;

    while let Some(chunk_result) = input_stream.next().await {
        match chunk_result {
            Ok(data) => {
                let mut data_slice = &data[..];
                while !data_slice.is_empty() {
                    let remaining_space = chunk_size - current_chunk.len();
                    let bytes_to_take = remaining_space.min(data_slice.len());
                    current_chunk.extend_from_slice(&data_slice[..bytes_to_take]);
                    data_slice = &data_slice[bytes_to_take..];

                    if current_chunk.len() == chunk_size {
                        chunk_buffer.push(Bytes::from(current_chunk));
                        current_chunk = Vec::with_capacity(chunk_size);

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
                            .await
                            .map_err(|e| Error::ProcessingError {
                                source: anyhow::Error::from(e),
                            })?;
                            // Keep only the last column for next iteration
                            chunk_buffer.drain(0..column_height as usize);
                            total_columns_processed += 1;
                        }
                    }
                }
            }
            Err(e) => {
                let error = Error::InputError {
                    source: anyhow::Error::from(e),
                };
                // The error to be sent through all parity streams because the parity streams
                // are being consumed asynchronously by other parts of the system, and they
                // need to know why the stream ended prematurely.
                for sender in &senders {
                    sender.send(Err(error.clone())).await?;
                }
                return Err(error);
            }
        }
    }

    process_remaining_chunks(
        chunk_buffer,
        first_column,
        total_columns_processed,
        column_height,
        &senders,
        &strand_types,
    )
    .await
    .map_err(|e| Error::ProcessingError {
        source: anyhow::Error::from(e),
    })?;

    drop(senders);

    Ok(())
}

/// Process any remaining chunks that don't form complete columns
/// This function handles entangling of the end of the stream with the first column.
async fn process_remaining_chunks(
    chunk_buffer: Vec<Bytes>,
    first_column: Option<Vec<Bytes>>,
    total_columns_processed: u64,
    column_height: u64,
    senders: &[mpsc::Sender<Result<Bytes, Error>>],
    strand_types: &[StrandType],
) -> Result<(), Error> {
    // we might start iterating over the remaining chunks from the middle of the leap window
    // (i.e. from the second or the third column), so we need to offset the index
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
            senders[j].send(Ok(parity)).await?;
        }
    }

    Ok(())
}

/// Entangles the first column of the grid with the second column.
async fn process_columns(
    chunk_buffer: &[Bytes],
    senders: &[mpsc::Sender<Result<Bytes, Error>>],
    strand_types: &[StrandType],
    column_height: u64,
) -> Result<(), Error> {
    let column_start = chunk_buffer.len() - 2 * column_height as usize;
    let positioner = Positioner::new(column_height, 2 * column_height);

    // Process each chunk in the first column
    for i in 0..column_height as usize {
        let pos = positioner.index_to_pos(i as u64);
        let chunk = &chunk_buffer[column_start + i];

        for (j, &strand_type) in strand_types.iter().enumerate() {
            let next_pos = positioner.normalize(pos + strand_type);
            let next_chunk =
                &chunk_buffer[column_start + column_height as usize + next_pos.y as usize];

            let parity = entangle_chunks(chunk, next_chunk);
            senders[j].send(Ok(parity)).await?;
        }
    }

    Ok(())
}

fn entangle_chunks<T: AsRef<[u8]>>(chunk1: &T, chunk2: &T) -> Bytes {
    let chunk1 = chunk1.as_ref();
    let chunk2 = chunk2.as_ref();
    let mut chunk = Vec::with_capacity(chunk1.len());
    for i in 0..chunk1.len() {
        chunk.push(chunk1[i] ^ chunk2[i]);
    }
    Bytes::from(chunk)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::grid::Pos;
    use futures::stream;

    // Helper function to create a stream of bytes
    fn create_stream(data: Vec<Vec<u8>>) -> impl Stream<Item = Result<Bytes, std::io::Error>> {
        stream::iter(data.into_iter().map(|chunk| Ok(Bytes::from(chunk))))
    }

    #[tokio::test]
    async fn test_stream_entangle_simple() {
        // Create a simple stream with two chunks
        let chunk1 = vec![1, 2, 3, 4];
        let chunk2 = vec![5, 6, 7, 8];
        let input_stream = create_stream(vec![chunk1.clone(), chunk2.clone()]);

        let executer = Executer::from_config(&Config::new(3, 1, 1)).with_chunk_size(4);

        let result = executer.entangle(input_stream).await.unwrap();

        assert_eq!(result.len(), 3, "Expected 3 parity streams");

        // Collect from each parity stream to verify contents
        let mut parity_results = Vec::new();
        for (i, mut stream) in result.into_iter().enumerate() {
            let mut chunks = Vec::new();
            while let Some(chunk_result) = stream.next().await {
                chunks.push(chunk_result.unwrap().to_vec());
            }
            assert_eq!(chunks.len(), 2, "Parity stream {} should have 2 chunks", i);
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

        let executer = Executer::from_config(&Config::new(1, 1, 1)).with_chunk_size(4);

        let result = executer.entangle(error_stream).await.unwrap();

        assert_eq!(result.len(), 1);

        let mut stream = result.into_iter().next().unwrap();
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
        let chunks = vec![
            vec![1, 2, 3, 4],
            vec![5, 6, 7, 8],
            vec![9, 10, 11, 12],
            vec![13, 14, 15, 16],
            vec![17, 18, 19, 20],
        ];
        let input_stream = create_stream(chunks.clone());

        let executer = Executer::from_config(&Config::new(1, 2, 2)).with_chunk_size(4);

        let parity_streams = executer.entangle(input_stream).await.unwrap();

        assert_eq!(parity_streams.len(), 1);

        for mut stream in parity_streams.into_iter() {
            let mut parity_chunks = Vec::new();

            while let Some(result) = stream.next().await {
                parity_chunks.push(result.unwrap().to_vec());
            }

            assert!(
                parity_chunks.len() == chunks.len(),
                "Expected {} parity chunks but got {}",
                chunks.len(),
                parity_chunks.len()
            );
        }
    }

    #[test]
    fn test_entangle_chunks() {
        let chunk1 = Bytes::from(vec![0b00000000, 0b11110000]);
        let chunk2 = Bytes::from(vec![0b10101010, 0b00001111]);
        let expected = Bytes::from(vec![0b10101010, 0b11111111]);
        let result = entangle_chunks(&chunk1, &chunk2);
        assert_eq!(result, expected);
    }

    #[tokio::test]
    async fn test_entangle_fixed_size_chunks() {
        // Create input stream with 8-byte chunks
        // We'll create a 3x6 grid (column_height = 3) to test toroidal wrapping
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

        let executer = Executer::from_config(&Config::new(3, 3, 3)).with_chunk_size(8);

        let result = executer.entangle(input_stream).await.unwrap();

        assert_eq!(result.len(), 3);

        let mut parity_results = Vec::new();
        for mut stream in result {
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
            vec![1, 2, 3],            // 3 bytes
            vec![4, 5, 6, 7],         // 4 bytes
            vec![8, 9],               // 2 bytes
            vec![10, 11, 12, 13],     // 4 bytes
            vec![14, 15, 16, 17, 18], // 5 bytes
        ];
        let input_stream = stream::iter(
            input_data
                .into_iter()
                .map(|chunk| Ok::<_, std::io::Error>(Bytes::from(chunk))),
        );

        let executer = Executer::from_config(&Config::new(2, 4, 4)).with_chunk_size(8);

        let result = executer.entangle(input_stream).await.unwrap();

        assert_eq!(result.len(), 2);

        let mut parity_results = Vec::new();
        for mut stream in result {
            let mut chunks = Vec::new();
            while let Some(chunk_result) = stream.next().await {
                chunks.push(chunk_result.unwrap().to_vec());
            }
            parity_results.push(chunks);
        }

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

        let executer = Executer::from_config(&Config::new(1, 2, 2)).with_chunk_size(4);

        let result = executer.entangle(input_stream).await.unwrap();

        let mut parity_stream = result.into_iter().next().unwrap();

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

            let executer = Executer::from_config(&Config::new(3, 3, 3)).with_chunk_size(8);

            let result = executer.entangle(input_stream).await.unwrap();

            assert_eq!(result.len(), 3);

            let mut parity_results = Vec::new();
            for mut stream in result {
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
