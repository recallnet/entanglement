// Copyright 2024 Entanglement Contributors
// SPDX-License-Identifier: Apache-2.0, MIT

/// This is the main entry point for the Entanglement CLI application.
///
/// The application supports uploading and downloading files using the Entangler library.
/// It uses the `clap` crate for command-line argument parsing and `stderrlog` for logging.
use std::path::PathBuf;

use clap::{Args, Parser, Subcommand};
use futures::StreamExt;
use stderrlog::Timestamp;

use entangler::{ByteStream, Config, Entangler};
use storage::iroh::IrohStorage;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
pub struct Cli {
    #[command(subcommand)]
    command: Commands,

    #[arg(long, env)]
    iroh_path: PathBuf,
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

    let storage = IrohStorage::new_permanent(cli.iroh_path).await?;

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
