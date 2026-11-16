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

//! Built-in "self-service analysis" skills (Phase 2).
//!
//! A skill is a curated prompt template surfaced as a one-click entry in the
//! chat panel. It guides the model towards a specific analysis workflow and
//! the tools it should prefer. Skills never bypass permissions — they can
//! only reference whitelisted tools.

use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct Skill {
    pub id: &'static str,
    pub title: &'static str,
    pub title_zh: &'static str,
    /// Prompt sent to the model when the user clicks the skill.
    pub prompt: &'static str,
    /// Chinese variant of the prompt; the console picks by locale so the
    /// conversation language matches the UI language.
    pub prompt_zh: &'static str,
    /// Tools the skill typically needs (informational, for UI hints).
    pub tools: Vec<&'static str>,
}

pub fn builtin_skills() -> Vec<Skill> {
    vec![
        Skill {
            id: "capacity",
            title: "Capacity analysis",
            title_zh: "容量盘点",
            prompt: "Analyze the cluster storage capacity: use the cluster overview and worker \
                     status tools, report total capacity, used/available space, usage ratio, \
                     reserved bytes, and point out any workers that are lost or near capacity.",
            prompt_zh: "分析集群存储容量：使用集群总览和 worker 状态工具，报告总容量、已用/可用空间、\n                     使用率、预留字节数，并指出丢失或接近满载的 worker。",
            tools: vec!["cluster_overview", "worker_status"],
        },
        Skill {
            id: "worker_health",
            title: "Worker health check",
            title_zh: "Worker 健康诊断",
            prompt: "Run a worker health check: use the worker status tool, compare live vs lost \
                     workers, summarize per-worker usage, and flag anything that needs \
                     attention (lost nodes, nearly-full disks, very few live workers).",
            prompt_zh: "做一次 worker 健康检查：使用 worker 状态工具，对比在线与丢失的 worker，\n                     汇总各 worker 的使用情况，并标出需要注意的问题（丢失节点、接近满盘、\n                     在线 worker 过少等）。",
            tools: vec!["worker_status", "cluster_overview"],
        },
        Skill {
            id: "dir_inventory",
            title: "Directory inventory",
            title_zh: "目录数据盘点",
            prompt: "Take an inventory of the filesystem root: use the list_dir tool to browse \
                     the top-level directories, then summarize what kinds of data live where \
                     (do not recurse deeper than two levels).",
            prompt_zh: "盘点文件系统根目录：使用 list_dir 工具浏览顶层目录，然后总结各类数据 \
                     分布在哪里（不要递归超过两层）。",
            tools: vec!["list_dir"],
        },
        Skill {
            id: "session_diagnosis",
            title: "Session / zombie mount diagnosis",
            title_zh: "会话与僵尸挂载排查",
            prompt: "Diagnose client sessions: use the session summary tool, list sessions with \
                     their host, mount point and lease state, and highlight any sessions that \
                     look like zombie mounts (expired lease but still registered).",
            prompt_zh: "诊断客户端会话：使用会话汇总工具，列出各会话的主机、挂载点和租约状态，\n                     并标出疑似僵尸挂载的会话（租约已过期但仍注册）。",
            tools: vec!["session_summary"],
        },
        Skill {
            id: "config_review",
            title: "Configuration review",
            title_zh: "配置巡检",
            prompt: "Review the cluster configuration (secrets are already redacted): use the \
                     cluster_config tool and summarize the most operationally relevant settings, \
                     flagging unusual or risky values.",
            prompt_zh: "审查集群配置（敏感字段已脱敏）：使用 cluster_config 工具，总结与运维最 \
                     相关的配置项，标出异常或有风险的取值。",
            tools: vec!["cluster_config"],
        },
    ]
}
