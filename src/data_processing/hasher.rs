use log::{debug, trace};
use serde_json::Value;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

pub fn hash_message(mut message: Value) -> u64 {
    let mut hasher = DefaultHasher::new();
    message = match message.get("vdl2") {
        Some(_) => generate_dumpvdl2_hashable_data(message),
        None => generate_acarsdec_or_vdlm2dec_hashable_data(message),
    };
    let msg = message.to_string();
    msg.hash(&mut hasher);
    hasher.finish()
}

fn generate_acarsdec_or_vdlm2dec_hashable_data(mut message: Value) -> Value {
    // if app is present, remove it
    match message.get("app").is_some() {
        true => {
            message.as_object_mut().unwrap().remove("app");
        }
        false => debug!("[Message Handler] No app found in message"),
    }
    // if error is present, remove it
    match message.get("error").is_some() {
        true => {
            message.as_object_mut().unwrap().remove("error");
        }
        false => debug!("[Message Handler] No error found in message"),
    }

    // if level is present, remove it

    match message.get("level").is_some() {
        true => {
            message.as_object_mut().unwrap().remove("level");
        }
        false => debug!("[Message Handler] No level found in message"),
    }

    // if station_id is present, remove it

    match message.get("station_id").is_some() {
        true => {
            message.as_object_mut().unwrap().remove("station_id");
        }
        false => debug!("[Message Handler] No station_id found in message"),
    }

    // if timestamp is present, remove it

    match message.get("timestamp").is_some() {
        true => {
            message.as_object_mut().unwrap().remove("timestamp");
        }
        false => debug!("[Message Handler] No timestamp found in message"),
    }

    // if channel is present, remove it

    match message.get("channel").is_some() {
        true => {
            message.as_object_mut().unwrap().remove("channel");
        }
        false => debug!("[Message Handler] No channel found in message"),
    }

    trace!("[Message Handler] Hashable data: {}", message);

    return message;
}

fn generate_dumpvdl2_hashable_data(message: Value) -> Value {
    return message;
}
