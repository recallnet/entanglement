// Copyright 2024 Entanglement Contributors
// SPDX-License-Identifier: Apache-2.0, MIT

/// This is the main entry point for the Entanglement CLI application.
///
/// The application supports uploading and downloading files using the Entangler library.
/// It uses the `clap` crate for command-line argument parsing and `stderrlog` for logging.
use std::net::SocketAddr;

use clap::{Args, Parser, Subcommand};
use futures::StreamExt;
use std::str::FromStr;
use stderrlog::Timestamp;

use entangler::{ByteStream, Config, Entangler};
use storage::iroh::IrohStorage;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
pub struct Cli {
    #[command(subcommand)]
    command: Commands,

    #[arg(long, env)]
    iroh_path: Option<String>,

    #[arg(long, env)]
    iroh_addr: Option<String>,
}

#[derive(Subcommand)]
enum Commands {
    Upload(UploadArgs),
    Download(DownloadArgs),
}

#[derive(Debug, Clone, PartialEq, Eq, clap::ValueEnum)]
enum StorageType {
    Iroh,
}

impl Cli {
    fn get_storage_config(&self) -> Result<StorageConfig, ConfigError> {
        match (self.iroh_path.as_ref(), self.iroh_addr.as_ref()) {
            (Some(path), None) => Ok(StorageConfig::Iroh(IrohConfig::Path(path.clone()))),
            (None, Some(addr)) => Ok(StorageConfig::Iroh(IrohConfig::Addr(addr.clone()))),
            (None, None) => Err(ConfigError::MissingConfig),
            _ => Err(ConfigError::ConflictingConfig),
        }
    }
}

#[derive(Args)]
struct UploadArgs {
    #[arg(short, long)]
    file: String,
}

#[derive(Args)]
struct DownloadArgs {
    #[arg(long)]
    hash: String,
    #[arg(long)]
    metadata_hash: Option<String>,
    #[arg(short, long)]
    output: String,
}

#[derive(Debug, Clone)]
enum StorageConfig {
    Iroh(IrohConfig),
}

#[derive(Debug, Clone)]
enum IrohConfig {
    Path(String),
    Addr(String),
}

#[derive(Debug, thiserror::Error)]
enum ConfigError {
    #[error("Missing storage configuration")]
    MissingConfig,
    #[error("Conflicting storage configuration options provided")]
    ConflictingConfig,
}

async fn write_stream_to_file(mut stream: ByteStream, path: &str) -> anyhow::Result<()> {
    use tokio::fs::File;
    use tokio::io::AsyncWriteExt;

    let mut file = File::create(path).await?;
    while let Some(chunk) = stream.next().await {
        file.write_all(&chunk?).await?;
    }
    file.flush().await?;
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    stderrlog::new()
        .module(module_path!())
        .timestamp(Timestamp::Millisecond)
        .init()
        .unwrap();

    let storage_config = cli.get_storage_config()?;

    let StorageConfig::Iroh(iroh_config) = storage_config;

    let storage = match iroh_config {
        IrohConfig::Path(path) => IrohStorage::from_path(path).await?,
        IrohConfig::Addr(addr) => {
            let socket_addr = SocketAddr::from_str(&addr)?;
            IrohStorage::from_addr(socket_addr).await?
        }
    };

    let entangler = Entangler::new(storage, Config::new(3, 3, 3))?;

    match cli.command {
        Commands::Upload(args) => {
            let bytes = tokio::fs::read(args.file.clone()).await?;
            let (file_hash, meta_hash) = entangler.upload(bytes).await?;
            println!("uploaded file. Hash: {}, Meta: {}", file_hash, meta_hash);
        }
        Commands::Download(args) => {
            let stream = entangler
                .download(&args.hash, args.metadata_hash.as_deref())
                .await?;
            write_stream_to_file(stream, &args.output).await?;
            println!("downloaded file");
        }
    }

    Ok(())
}
