extern crate chrono;
extern crate env_logger;
extern crate log;

use chrono::Local;
use env_logger::Builder;
use log::LevelFilter;
use std::io::Write;

pub trait SetupLogging {
    fn set_logging_level(self) -> LevelFilter;
    fn enable_logging(&self);
}

impl SetupLogging for u8 {
    fn set_logging_level(self) -> LevelFilter {
        match self {
            0 => LevelFilter::Info,
            1 => LevelFilter::Debug,
            2..=u8::MAX => LevelFilter::Trace,
        }
    }

    fn enable_logging(&self) {
        Builder::new()
            .format(|buf, record| {
                writeln!(
                    buf,
                    "{} [{}] - {}",
                    Local::now().format("%Y-%m-%dT%H:%M:%S"),
                    record.level(),
                    record.args()
                )
            })
            .filter(None, self.set_logging_level())
            .init();
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_set_logging_level() {
        let info_level: u8 = 0;
        let debug_level: u8 = 1;
        let trace_level: u8 = 2;
        let stupid_levels: u8 = 255;
        let info_level_logging: LevelFilter = info_level.set_logging_level();
        let debug_level_logging: LevelFilter = debug_level.set_logging_level();
        let trace_level_logging: LevelFilter = trace_level.set_logging_level();
        let stupid_levels_logging: LevelFilter = stupid_levels.set_logging_level();
        assert_eq!(info_level_logging, LevelFilter::Info);
        assert_eq!(debug_level_logging, LevelFilter::Debug);
        assert_eq!(trace_level_logging, LevelFilter::Trace);
        assert_eq!(stupid_levels_logging, LevelFilter::Trace);
    }
}
