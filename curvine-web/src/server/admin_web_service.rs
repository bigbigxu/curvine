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

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use axum::middleware;
use curvine_config::ClusterConf;
use curvine_runtime::runtime::Runtime;

use crate::cluster::{clusters_dir_for, ClusterRegistry};
use crate::router::admin_handler::require_ai_session_auth;
use crate::router::AdminRouterHandler;
use crate::server::WebHandlerService;

#[derive(Clone)]
pub struct AdminWebService {
    conf: ClusterConf,
    rt: Arc<Runtime>,
    /// Overrides the import directory; `None` derives it from `conf` (either
    /// `web.clusters_dir` or `<cwd>/clusters`).
    clusters_dir: Option<PathBuf>,
    /// Console session map shared by the main router and the CIVI AI router,
    /// so a web login also grants `/api/ai/*`.
    auth_sessions: Arc<Mutex<HashMap<String, i64>>>,
}

impl AdminWebService {
    pub fn new(conf: ClusterConf, rt: Arc<Runtime>) -> Self {
        Self {
            conf,
            rt,
            clusters_dir: None,
            auth_sessions: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Same as [`AdminWebService::new`] but with an explicit cluster import
    /// directory, which the binary derives from the `--conf` location.
    pub fn with_clusters_dir(conf: ClusterConf, rt: Arc<Runtime>, dir: PathBuf) -> Self {
        Self {
            conf,
            rt,
            clusters_dir: Some(dir),
            auth_sessions: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl WebHandlerService for AdminWebService {
    type Item = AdminRouterHandler;

    fn get_handler(&self) -> Self::Item {
        let dir = self
            .clusters_dir
            .clone()
            .unwrap_or_else(|| clusters_dir_for(&self.conf, None));
        let max = self.conf.web.max_clusters;
        let registry = ClusterRegistry::new(self.conf.clone(), self.rt.clone(), dir, max)
            .expect("failed to initialize the cluster registry");
        AdminRouterHandler::with_registry_and_auth(
            self.rt.clone(),
            Arc::new(registry),
            Arc::clone(&self.auth_sessions),
        )
    }

    /// CIVI AI assistant for the console.
    ///
    /// The provider resolves the conversation's target cluster (sent by the
    /// console as part of the chat payload) against the cluster registry, so
    /// the agent answers for the cluster selected in the top bar. The routes
    /// are gated by the console session cookie — merged routers keep their
    /// own layers, so the main router's `require_auth` does not cover them.
    fn ai_router(&self) -> Option<axum::Router> {
        let handler = self.get_handler();
        let ai = crate::ai::build_router(Arc::new(handler));
        Some(ai.layer(middleware::from_fn_with_state(
            Arc::clone(&self.auth_sessions),
            require_ai_session_auth,
        )))
    }
}
