// Copyright (c) Mike Nye, Fred Clausen
//
// Licensed under the MIT license: https://opensource.org/licenses/MIT
// Permission is granted to use, copy, modify, and redistribute the work.
// Full license information available in the project LICENSE file.
//

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use acars_vdlm2_parser::{AcarsVdlm2Message, MessageResult};
use acars_vdlm2_parser::acars::AcarsMessage;
use acars_vdlm2_parser::vdlm2::Vdlm2Message;

pub fn hash_message(mut message: AcarsVdlm2Message) -> MessageResult<u64> {
    let mut hasher = DefaultHasher::new();
    // message = match message.get("vdl2") {
    //     Some(_) => generate_dumpvdl2_hashable_data(message),
    //     None => generate_acarsdec_or_vdlm2dec_hashable_data(message),
    // };
    message = match message {
        AcarsVdlm2Message::Vdlm2Message(vdml2) => generate_dumpvdl2_hashable_data(vdml2),
        AcarsVdlm2Message::AcarsMessage(acars) => generate_acarsdec_or_vdlm2dec_hashable_data(acars)
    };
    let parse_msg = message.to_string();
    match parse_msg {
        Err(parse_error) => Err(parse_error),
        Ok(msg) => {
            trace!("[Hasher] Message to be hashed: {}", msg);
            msg.hash(&mut hasher);
            let hashed_value = hasher.finish();
            Ok(hashed_value)
        }
    }
}

fn generate_acarsdec_or_vdlm2dec_hashable_data(mut message: AcarsMessage) -> AcarsVdlm2Message {
    // if app is present, remove it
    // match message.get("app").is_some() {
    //     true => match message.as_object_mut() {
    //         Some(obj) => {
    //             obj.remove("app");
    //         }
    //         None => (),
    //     },
    //     false => (),
    // }
    // if error is present, remove it
    // match message.get("error").is_some() {
    //     true => match message.as_object_mut() {
    //         Some(obj) => {
    //             obj.remove("error");
    //         }
    //         None => (),
    //     },
    //     false => (),
    // }

    // if level is present, remove it

    // match message.get("level").is_some() {
    //     true => match message.as_object_mut() {
    //         Some(obj) => {
    //             obj.remove("level");
    //         }
    //         None => (),
    //     },
    //     false => (),
    // }

    // if station_id is present, remove it

    message.clear_station_name();

    // if timestamp is present, remove it

    message.clear_time();

    // if channel is present, remove it

    // match message.get("channel").is_some() {
    //     true => match message.as_object_mut() {
    //         Some(obj) => {
    //             obj.remove("channel");
    //         }
    //         None => (),
    //     },
    //     false => (),
    // }

    trace!("[Hasher] Hashable data: {:?}", message);

    AcarsVdlm2Message::AcarsMessage(message)
}

fn generate_dumpvdl2_hashable_data(mut message: Vdlm2Message) -> AcarsVdlm2Message {
    // if app is present in vdl2, remove it
    // match message["vdl2"].get("app").is_some() {
    //     true => match message["vdl2"].as_object_mut() {
    //         Some(vdl2) => {
    //             vdl2.remove("app");
    //         }
    //         None => (),
    //     },
    //     false => (),
    // }

    // if freq_skew is present in vdl2, remove it
    // match message["vdl2"].get("freq_skew").is_some() {
    //     true => {
    //         match message["vdl2"].as_object_mut() {
    //             Some(vdl2) => {
    //                 vdl2.remove("freq_skew");
    //             }
    //             None => (),
    //         };
    //     }
    //     false => (),
    // }

    // if hdr_bits_fixed in vdl2, remove it

    // match message["vdl2"].get("hdr_bits_fixed").is_some() {
    //     true => match message["vdl2"].as_object_mut() {
    //         Some(vdl2) => {
    //             vdl2.remove("hdr_bits_fixed");
    //         }
    //         None => (),
    //     },
    //     false => (),
    // }

    // if noise_level is present in vdl2, remove it

    // match message["vdl2"].get("noise_level").is_some() {
    //     true => {
    //         match message["vdl2"].as_object_mut() {
    //             Some(vdl2) => {
    //                 vdl2.remove("noise_level");
    //             }
    //             None => (),
    //         };
    //     }
    //     false => (),
    // }

    // if octets_corrected_by_fec is present in vdl2, remove it

    // match message["vdl2"].get("octets_corrected_by_fec").is_some() {
    //     true => match message["vdl2"].as_object_mut() {
    //         Some(obj) => {
    //             obj.remove("octets_corrected_by_fec");
    //         }
    //         None => (),
    //     },
    //     false => (),
    // }

    // if sig_level is present in vdl2, remove it

    // match message["vdl2"].get("sig_level").is_some() {
    //     true => {
    //         match message["vdl2"].as_object_mut() {
    //             Some(obj) => {
    //                 obj.remove("sig_level");
    //             }
    //             None => (),
    //         };
    //     }
    //     false => (),
    // }

    // if station is present in vdl2, remove it

    message.clear_station_name();

    // if t is present in vdl2, remove it

    message.clear_time();

    AcarsVdlm2Message::Vdlm2Message(message)
}
