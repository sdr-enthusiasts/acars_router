// Copyright (c) Mike Nye, Fred Clausen
//
// Licensed under the MIT license: https://opensource.org/licenses/MIT
// Permission is granted to use, copy, modify, and redistribute the work.
// Full license information available in the project LICENSE file.
//

use acars_config::Input;
use acars_config::clap::Parser;
use acars_connection_manager::service_init::start_processes;
use std::error::Error;
use std::process;
use tokio::signal;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, trace};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args: Input = Input::parse();
    init_tracing(&args.verbose);
    match args.check_config_option_sanity() {
        Ok(()) => {
            trace!("Config options are sane");
        }
        Err(e) => {
            error!("{e}");
            process::exit(1);
        }
    }

    let shutdown = CancellationToken::new();
    let shutdown_signal = shutdown.clone();
    tokio::spawn(async move {
        wait_for_shutdown_signal().await;
        info!("Shutdown signal received, asking tasks to stop");
        shutdown_signal.cancel();
    });

    start_processes(args, shutdown).await;
    info!("Shutdown complete");
    Ok(())
}

/// Initialise the global `tracing` subscriber.
///
/// Honours `RUST_LOG` when set (standard `EnvFilter` syntax, e.g.
/// `RUST_LOG=acars_router=debug,hickory_resolver=warn`); otherwise
/// falls back to the `--verbose` / `AR_VERBOSITY` value (one of
/// `trace`/`debug`/`info`/`warn`/`error`), which preserves the
/// pre-existing CLI surface. If the configured value is unparsable
/// we degrade to `info` rather than panic — the router should always
/// boot.
fn init_tracing(verbosity: &str) {
    let filter = std::env::var("RUST_LOG").map_or_else(
        |_| EnvFilter::try_new(verbosity).unwrap_or_else(|_| EnvFilter::new("info")),
        |s| EnvFilter::try_new(s).unwrap_or_else(|_| EnvFilter::new("info")),
    );
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(true)
        .init();
}

/// Resolves on the first of: Ctrl-C, SIGTERM (Unix). On non-Unix only
/// Ctrl-C is observed; SIGTERM has no portable equivalent in `tokio::signal`.
async fn wait_for_shutdown_signal() {
    #[cfg(unix)]
    {
        let mut sigterm = match signal::unix::signal(signal::unix::SignalKind::terminate()) {
            Ok(s) => s,
            Err(e) => {
                error!("Failed to install SIGTERM handler: {e}; falling back to Ctrl-C only");
                let _ = signal::ctrl_c().await;
                return;
            }
        };
        tokio::select! {
            _ = signal::ctrl_c() => {}
            _ = sigterm.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = signal::ctrl_c().await;
    }
}
