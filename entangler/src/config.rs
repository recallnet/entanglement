/// Configuration for the entangler.
#[derive(Debug, Clone)]
pub struct Config {
    /// The number of parity chunks to generate for each data chunk. It should be from 1 to 3.
    pub alpha: u8,
    /// The number of horizontal strands in the grid. It should be larger than 0.
    pub s: u8,
    /// Specifies whether to always try to heal blobs.
    /// If set to `false`, the entangler will try to repair and consequently upload the entire blob
    /// only when the whole blob was requested.
    /// To make it try to heal and upload the blob also when only a part of it was requested
    /// (e.g. with range request), set this to `true`.
    /// Default is `true`.
    pub always_repair: bool,
    /// Channel buffer size for parity streams (default: 10 * 1024)
    /// This is the number of 1024-bytes chunks that fit into a single parity buffer
    /// Entangle might produce parity data faster than the storage can upload it,
    /// but in case the storage is too slow, this buffer will fill up and entangle will block
    /// until the storage has uploaded some data.
    pub channel_buffer_size: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            alpha: 3,
            s: 5,
            always_repair: true,
            channel_buffer_size: 10 * 1024,
        }
    }
}

impl Config {
    /// Creates a new `Config` with the given parameters. The reset of the parameters are set to
    /// their default values.
    ///
    /// # Arguments
    ///
    /// * `alpha` - The number of parity chunks to generate for each data chunk. It should be from 1 to 3.
    /// * `s` - The number of horizontal strands in the grid. It should be larger than 0.
    ///
    /// # Returns
    ///
    /// A new `Config` with the given parameters.
    pub fn new(alpha: u8, s: u8) -> Self {
        Config {
            alpha,
            s,
            ..Default::default()
        }
    }

    /// Sets the channel buffer size for parity streams
    /// This is the number of 1024-bytes chunks that fit into a single parity buffer
    /// Entangle might produce parity data faster than the storage can upload it,
    /// but in case the storage is too slow, this buffer will fill up and entangle will block
    /// until the storage has uploaded some data.
    pub fn with_channel_buffer_size(mut self, size: usize) -> Self {
        self.channel_buffer_size = size;
        self
    }
}
