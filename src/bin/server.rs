use clap::{Parser, Subcommand};
use std::{fs, path::PathBuf};
use tokio::{net::TcpListener, runtime::Runtime, signal};
use tracing::{error, info};

use relay::{config::Config, server};

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
#[command(propagate_version = true)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}
#[derive(Subcommand)]
enum Commands {
    /// run with config file: --config config.toml
    Run {
        #[arg(short, long, value_name = "FILE")]
        config: PathBuf,
    },
    Debug,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    let rt = Runtime::new()?;

    match cli.command {
        Commands::Run { config } => {
            let toml_string = fs::read_to_string(&config)?;
            let config: Config = toml::from_str(&toml_string)?;
            tracing_subscriber::fmt().init();
            rt.block_on(async move {
                info!("Starting relay server...");
                match TcpListener::bind(&format!("127.0.0.1:{}", &config.port)).await {
                    Ok(listener) => {
                        server::run(config, listener, signal::ctrl_c()).await;
                    }
                    Err(error) => {
                        error!("Bind tcp with error: {:?}", error);
                    }
                }
            });
        }
        Commands::Debug => {}
    };

    Ok(())
}
