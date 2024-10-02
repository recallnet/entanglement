// Copyright 2024 Entanglement Contributors
// SPDX-License-Identifier: Apache-2.0, MIT

/// This is the main entry point for the Entanglement CLI application.
///
/// The application supports uploading and downloading files using the Entangler library.
/// It uses the `clap` crate for command-line argument parsing and `stderrlog` for logging.
use std::net::SocketAddr;

use clap::{Args, Parser, Subcommand};
use std::str::FromStr;
use stderrlog::Timestamp;

use entangler::Entangler;
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

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    print!("main\n");
    let cli = Cli::parse();

    print!("after Cli::parse\n");
    stderrlog::new()
        .module(module_path!())
        .timestamp(Timestamp::Millisecond)
        .init()
        .unwrap();

    let storage_config = cli.get_storage_config()?;

    let iroh_config = match storage_config {
        StorageConfig::Iroh(config) => config,
    };

    let storage = match iroh_config {
        IrohConfig::Path(path) => IrohStorage::from_path(path).await?,
        IrohConfig::Addr(addr) => {
            let socket_addr = SocketAddr::from_str(&addr)?;
            IrohStorage::from_addr(socket_addr).await?
        }
    };

    let entangler = Entangler::new(storage, 3, 3, 3)?;

    match cli.command {
        Commands::Upload(args) => {
            let bytes = tokio::fs::read(args.file.clone()).await?;
            print!(
                "uploading file {} of size {} bytes...\n",
                args.file,
                bytes.len()
            );
            let (file_hash, meta_hash) = entangler.upload(bytes).await?;
            print!("uploaded file. Hash: {}, Meta: {}\n", file_hash, meta_hash);
        }
        Commands::Download(args) => {
            let bytes = entangler
                .download(&args.hash, args.metadata_hash.as_deref())
                .await?;
            tokio::fs::write(args.output, bytes).await?;
            print!("downloaded file\n");
        }
    }

    Ok(())
}
