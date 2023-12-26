// Copyright 2023 RisingWave Labs
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

use crate::TempoConfig;
pub struct TempoGen;

impl TempoGen {
    pub fn gen_tempo_yml(&self, config: &TempoConfig) -> String {
        let http_listen_address = &config.listen_address;
        let http_listen_port = config.port;

        let otlp_host = &config.listen_address;
        let otlp_port = config.otlp_port;

        format!(
            r#"# --- THIS FILE IS AUTO GENERATED BY RISEDEV ---
server:
  http_listen_address: "{http_listen_address}"
  http_listen_port: {http_listen_port}

distributor:
  receivers:
      otlp:
        protocols:
          grpc:
            endpoint: "{otlp_host}:{otlp_port}"
    "#
        )
    }
}
