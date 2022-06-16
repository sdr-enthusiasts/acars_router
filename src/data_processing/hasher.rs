// Copyright (c) Mike Nye, Fred Clausen
//
// Licensed under the MIT license: https://opensource.org/licenses/MIT
// Permission is granted to use, copy, modify, and redistribute the work.
// Full license information available in the project LICENSE file.
//

use log::{debug, trace};
use serde_json::Value;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

pub fn hash_message(mut message: Value) -> (u64, Value) {
    let mut hasher = DefaultHasher::new();
    message = match message.get("vdl2") {
        Some(_) => generate_dumpvdl2_hashable_data(message),
        None => generate_acarsdec_or_vdlm2dec_hashable_data(message),
    };
    let msg = message.to_string();
    trace!("[Hasher] Message to be hashed: {}", msg);
    msg.hash(&mut hasher);
    let hashed_value = hasher.finish();
    return (hashed_value, message);
}

fn generate_acarsdec_or_vdlm2dec_hashable_data(mut message: Value) -> Value {
    // if app is present, remove it
    match message.get("app").is_some() {
        true => {
            message.as_object_mut().unwrap().remove("app");
        }
        false => debug!("[Hasher] No app found in message"),
    }
    // if error is present, remove it
    match message.get("error").is_some() {
        true => {
            message.as_object_mut().unwrap().remove("error");
        }
        false => debug!("[Hasher] No error found in message"),
    }

    // if level is present, remove it

    match message.get("level").is_some() {
        true => {
            message.as_object_mut().unwrap().remove("level");
        }
        false => debug!("[Hasher] No level found in message"),
    }

    // if station_id is present, remove it

    match message.get("station_id").is_some() {
        true => {
            message.as_object_mut().unwrap().remove("station_id");
        }
        false => debug!("[Hasher] No station_id found in message"),
    }

    // if timestamp is present, remove it

    match message.get("timestamp").is_some() {
        true => {
            message.as_object_mut().unwrap().remove("timestamp");
        }
        false => debug!("[Hasher] No timestamp found in message"),
    }

    // if channel is present, remove it

    match message.get("channel").is_some() {
        true => {
            message.as_object_mut().unwrap().remove("channel");
        }
        false => debug!("[Hasher] No channel found in message"),
    }

    trace!("[Hasher] Hashable data: {}", message);

    return message;
}

fn generate_dumpvdl2_hashable_data(mut message: Value) -> Value {
    // if app is present in vdl2, remove it
    match message["vdl2"].get("app").is_some() {
        true => match message["vdl2"].get("app").is_some() {
            true => {
                message["vdl2"].as_object_mut().unwrap().remove("app");
            }
            false => debug!("[Hasher] No app found in message"),
        },
        false => debug!("[Hasher] No app found in message"),
    }

    // if freq_skew is present in vdl2, remove it
    match message["vdl2"].get("freq_skew").is_some() {
        true => match message["vdl2"].get("freq_skew").is_some() {
            true => {
                message["vdl2"].as_object_mut().unwrap().remove("freq_skew");
            }
            false => debug!("[Hasher] No freq_skew found in message"),
        },
        false => debug!("[Hasher] No freq_skew found in message"),
    }

    // if hdr_bits_fixed in vdl2, remove it

    match message["vdl2"].get("hdr_bits_fixed").is_some() {
        true => match message["vdl2"].get("hdr_bits_fixed").is_some() {
            true => {
                message["vdl2"]
                    .as_object_mut()
                    .unwrap()
                    .remove("hdr_bits_fixed");
            }
            false => debug!("[Hasher] No hdr_bits_fixed found in message"),
        },
        false => debug!("[Hasher] No hdr_bits_fixed found in message"),
    }

    // if noise_level is present in vdl2, remove it

    match message["vdl2"].get("noise_level").is_some() {
        true => match message["vdl2"].get("noise_level").is_some() {
            true => {
                message["vdl2"]
                    .as_object_mut()
                    .unwrap()
                    .remove("noise_level");
            }
            false => debug!("[Hasher] No noise_level found in message"),
        },
        false => debug!("[Hasher] No noise_level found in message"),
    }

    // if octets_corrected_by_fec is present in vdl2, remove it

    match message["vdl2"].get("octets_corrected_by_fec").is_some() {
        true => match message["vdl2"].get("octets_corrected_by_fec").is_some() {
            true => {
                message["vdl2"]
                    .as_object_mut()
                    .unwrap()
                    .remove("octets_corrected_by_fec");
            }
            false => debug!("[Hasher] No octets_corrected_by_fec found in message"),
        },
        false => debug!("[Hasher] No octets_corrected_by_fec found in message"),
    }

    // if sig_level is present in vdl2, remove it

    match message["vdl2"].get("sig_level").is_some() {
        true => match message["vdl2"].get("sig_level").is_some() {
            true => {
                message["vdl2"].as_object_mut().unwrap().remove("sig_level");
            }
            false => debug!("[Hasher] No sig_level found in message"),
        },
        false => debug!("[Hasher] No sig_level found in message"),
    }

    // if station is present in vdl2, remove it

    match message["vdl2"].get("station").is_some() {
        true => match message["vdl2"].get("station").is_some() {
            true => {
                message["vdl2"].as_object_mut().unwrap().remove("station");
            }
            false => debug!("[Hasher] No station found in message"),
        },
        false => debug!("[Hasher] No station found in message"),
    }

    // if t is present in vdl2, remove it

    match message["vdl2"].get("t").is_some() {
        true => match message["vdl2"].get("t").is_some() {
            true => {
                message["vdl2"].as_object_mut().unwrap().remove("t");
            }
            false => debug!("[Hasher] No t found in message"),
        },
        false => debug!("[Hasher] No t found in message"),
    }

    return message;
}
