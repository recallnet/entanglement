/// Configuration for the entangler.
pub struct Config {
    /// The number of parity chunks to generate for each data chunk.
    pub alpha: u8,
    /// The number of horizontal strands in the grid.
    pub s: u8,
    /// The number of helical strands in the grid.
    pub p: u8,
    /// Specifies whether to always try to heal blobs.
    /// If set to `false`, the entangler will try to repair and consequently upload the entire blob
    /// only when the whole blob was requested.
    /// To make it try to heal and upload the blob also when only a part of it was requested
    /// (e.g. with range request), set this to `true`.
    /// Default is `true`.
    pub always_repair: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            alpha: 3,
            s: 5,
            p: 5,
            always_repair: true,
        }
    }
}

impl Config {
    /// Creates a new `Config` with the given parameters. The reset of the parameters are set to
    /// their default values.
    ///
    /// # Arguments
    ///
    /// * `alpha` - The number of parity chunks to generate for each data chunk.
    /// * `s` - The number of horizontal strands in the grid.
    /// * `p` - The number of helical strands in the grid.
    ///
    /// # Returns
    ///
    /// A new `Config` with the given parameters.
    pub fn new(alpha: u8, s: u8, p: u8) -> Self {
        let mut c = Self::default();
        c.alpha = alpha;
        c.s = s;
        c.p = p;
        c
    }
}
