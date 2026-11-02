// Copyright 2025 OPPO.
//
// Licensed under the Apache License, Version 2.0.
// License text: http://www.apache.org/licenses/LICENSE-2.0

//! Placement: which transfer instance owns a job, decided from the configured
//! endpoint list.
//!
//! The endpoint list is static configuration (`transfer.endpoints`), so the ring
//! is built once at startup: no registry, no heartbeat, no refresh. Instance
//! changes are config changes, and every instance must be configured with the
//! same list.
//!
//! Placement is computed once, at submit, and stored on the job record. Every
//! later decision reads that stored address, so a temporary disagreement
//! between two instances' configs cannot make them hand the same job back and
//! forth.

use std::sync::Arc;
use std::time::Duration;

use curvine_error::{FsError, FsResult};
use curvine_rpc::client::ClientFactory;
use curvine_rpc::RpcUtils;
use parking_lot::RwLock;
use prost::Message as PMessage;

use crate::transfer::ConsistentHashRing;

const RELAY_TIMEOUT: Duration = Duration::from_secs(5);

pub struct Forwarder {
    /// This instance's own rpc `host:port`, in the same form as the ring.
    self_addr: String,
    ring: Arc<RwLock<Option<Arc<ConsistentHashRing>>>>,
    endpoints: Vec<String>,
    factory: Arc<ClientFactory>,
}

impl Forwarder {
    pub fn new(
        endpoints: Vec<String>,
        self_addr: impl Into<String>,
        factory: Arc<ClientFactory>,
    ) -> Self {
        Self {
            self_addr: self_addr.into(),
            ring: Arc::new(RwLock::new(None)),
            endpoints,
            factory,
        }
    }

    pub fn self_addr(&self) -> &str {
        &self.self_addr
    }

    /// Placement owner of a routing key, from the configured endpoints.
    pub fn owner_of(&self, key: &str) -> Option<String> {
        self.ring().owner(key).map(|entry| entry.addr.clone())
    }

    /// Address this instance should relay `key` to, or None when it owns it.
    pub fn relay_target(&self, key: &str) -> Option<String> {
        let owner = self.owner_of(key)?;
        (owner != self.self_addr).then_some(owner)
    }

    /// True when this instance is the placement owner of `key`. An empty ring
    /// (no configured endpoints) means nobody can claim ownership, so everyone
    /// serves locally instead of stalling.
    pub fn owns(&self, key: &str) -> bool {
        match self.owner_of(key) {
            Some(owner) => owner == self.self_addr,
            None => true,
        }
    }

    /// Relays one request to `addr` and returns that instance's own response.
    pub async fn relay<T, R>(
        &self,
        addr: &str,
        code: curvine_fs_api::RpcCode,
        req: T,
    ) -> FsResult<R>
    where
        T: PMessage + Default,
        R: PMessage + Default,
    {
        if addr == self.self_addr {
            return Err(FsError::common("refusing to relay a request to itself"));
        }
        let inet = curvine_net::net::InetAddr::from_str(addr)?;
        let client = self
            .factory
            .get(&inet)
            .await
            .map_err(|err| FsError::common(format!("connect transfer owner {addr}: {err}")))?;
        RpcUtils::proto_rpc(&client, RELAY_TIMEOUT, code, req).await
    }

    /// The ring is immutable: built once from the configured endpoints.
    fn ring(&self) -> Arc<ConsistentHashRing> {
        if let Some(ring) = self.ring.read().as_ref() {
            return ring.clone();
        }
        let ring = Arc::new(ConsistentHashRing::build(
            self.endpoints
                .iter()
                .map(|addr| (addr.clone(), addr.clone(), 0u64)),
        ));
        *self.ring.write() = Some(ring.clone());
        ring
    }
}

/// True when the caller opted out of relaying for this request.
pub fn is_forwarded(forwarded: Option<bool>) -> bool {
    forwarded.unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn placement_is_stable_and_spread_over_the_configured_list() {
        // Ring building is pure; assert directly without a client factory.
        let ring = ConsistentHashRing::build(
            ["10.0.0.1:9010", "10.0.0.2:9010", "10.0.0.3:9010"]
                .into_iter()
                .map(|addr| (addr.to_string(), addr.to_string(), 0u64)),
        );
        let mut owners = std::collections::HashSet::new();
        for index in 0..60 {
            let key = format!("request-{index}");
            let first = ring.owner(&key).unwrap().addr.clone();
            // Deterministic: the same key always maps to the same instance.
            assert_eq!(first, ring.owner(&key).unwrap().addr);
            owners.insert(first);
        }
        assert!(
            owners.len() > 1,
            "placement must spread over the configured instances"
        );
    }

    #[test]
    fn empty_endpoint_list_claims_nothing() {
        let ring = ConsistentHashRing::default();
        assert!(ring.owner("request-1").is_none());
    }
}
