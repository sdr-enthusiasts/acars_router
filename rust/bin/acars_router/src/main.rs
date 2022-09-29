// Copyright (c) Mike Nye, Fred Clausen
//
// Licensed under the MIT license: https://opensource.org/licenses/MIT
// Permission is granted to use, copy, modify, and redistribute the work.
// Full license information available in the project LICENSE file.
//

#[macro_use]
extern crate log;
extern crate acars_config;
extern crate acars_connection_manager;
extern crate acars_logging;
extern crate chrono;
extern crate failure;
extern crate serde;
extern crate serde_json;

use acars_config::clap::Parser;
use acars_config::Input;
use acars_connection_manager::service_init::start_processes;
use acars_logging::SetupLogging;
use std::error::Error;
use std::process;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args: Input = Input::parse();
    args.verbose.enable_logging();
    match args.check_config_option_sanity() {
        Ok(_) => {
            trace!("Config options are sane");
        }
        Err(e) => {
            error!("{}", e);
            process::exit(1);
        }
    }
    start_processes(args).await;
    Ok(())
}
