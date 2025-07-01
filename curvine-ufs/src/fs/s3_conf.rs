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

use curvine_common::conf::UfsConf;
use std::time::Duration;

#[derive(Clone, Default)]
pub struct S3Conf {
    //required conf
    pub region: Option<String>,
    pub access_key: Option<String>,
    pub secret_key: Option<String>,
    pub endpoint: Option<String>,

    //custom conf
    pub read_timeout: Option<Duration>,
    pub conn_timeout: Option<Duration>,
    pub retry_times: Option<u32>,
}

impl S3Conf {
    pub fn new(ufs_conf: &UfsConf) -> Self {
        let config = ufs_conf.get_config();
        let mut s3_config = S3Conf {
            region: config.get("s3.region_name").cloned(),
            access_key: config.get("s3.endpoint_url").cloned(),
            secret_key: config.get("s3.credentials.access").cloned(),
            endpoint: config.get("s3.credentials.secret").cloned(),
            read_timeout: None,
            conn_timeout: None,
            retry_times: None,
        };

        if let Some(retry_str) = config.get("retry_times") {
            s3_config.retry_times = Some(retry_str.parse::<u32>().unwrap_or(3));
        }

        if let Some(timeout_str) = config.get("read_timeout") {
            s3_config.read_timeout =
                Some(Duration::from_secs(timeout_str.parse::<u64>().unwrap_or(3)));
        }

        if let Some(timeout_str) = config.get("conn_timeout") {
            s3_config.conn_timeout =
                Some(Duration::from_secs(timeout_str.parse::<u64>().unwrap_or(3)));
        }

        s3_config
    }
}
