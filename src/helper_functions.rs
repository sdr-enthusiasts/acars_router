// Copyright (c) Mike Nye, Fred Clausen
//
// Licensed under the MIT license: https://opensource.org/licenses/MIT
// Permission is granted to use, copy, modify, and redistribute the work.
// Full license information available in the project LICENSE file.
//

pub fn should_start_service(config: &Vec<String>) -> bool {
    !config.is_empty() && !config[0].is_empty()
}

pub fn exit_process(code: i32) {
    std::process::exit(code);
}

pub fn strip_line_endings(line: &String) -> String {
    return line
        .strip_suffix("\r\n")
        .or(line.strip_suffix('\n'))
        .unwrap_or(line)
        .to_string();
}
