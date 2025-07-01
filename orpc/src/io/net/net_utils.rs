// Copyright 2025 OPPO.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::io::IOResult;
use crate::{try_err, try_option};
use std::net::ToSocketAddrs;
use sysinfo::System;

pub struct NetUtils;

impl NetUtils {
    // Get a system-available port.
    pub fn get_available_port() -> u16 {
        std::net::TcpListener::bind("0.0.0.0:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port()
    }

    pub fn local_hostname() -> String {
        Self::get_hostname().unwrap_or("localhost".to_string())
    }

    pub fn local_ip<T: AsRef<str>>(hostname: T) -> String {
        Self::get_ip_v4(hostname.as_ref()).unwrap_or("127.0.0.1".to_owned())
    }

    pub fn get_hostname() -> Option<String> {
        System::host_name()
    }

    pub fn get_ip_v4<T: AsRef<str>>(host: T) -> IOResult<String> {
        let hostname = format!("{}:{}", host.as_ref(), 0);
        // Try to get an ipv4 address.
        let ip = try_err!(hostname.to_socket_addrs())
            .filter(|x| x.is_ipv4())
            .next();
        let ip = try_option!(ip).ip().to_string();
        Ok(ip)
    }
}
