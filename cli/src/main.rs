use clap::Parser;
use lyra_stream_cli::unit::UnitAction;

#[derive(Parser)]
#[command(name = "lyra-stream", about = "Lyra streaming storage")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(clap::Subcommand)]
enum Commands {
    /// Manage the local stream unit.
    Unit {
        #[command(subcommand)]
        action: UnitAction,
    },
}

#[tokio::main(worker_threads = 4)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Unit { action } => lyra_stream_cli::unit::run(action).await?,
    }

    Ok(())
}
