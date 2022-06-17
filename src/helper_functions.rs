pub fn should_start_service(config: &Vec<String>) -> bool {
    config.len() > 0 && config[0].len() > 0
}

pub fn exit_process(code: i32) {
    std::process::exit(code);
}
