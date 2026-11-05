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

use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::Arc;
use std::time::Duration;

use curvine_error::{FsError, FsResult};
use curvine_rpc::client::ClientFactory;
use curvine_rpc::RpcUtils;
use parking_lot::RwLock;
use prost::Message as PMessage;

use crate::transfer::ConsistentHashRing;

const RELAY_TIMEOUT: Duration = Duration::from_secs(5);

/// True when two `host:port` strings name the same endpoint.
///
/// The two sides of a placement comparison are produced by different sources:
/// `self_addr` is built from `transfer.hostname` (a bound IP when the process
/// resolves its own NIC, which is what the container entrypoint exports), while
/// ring entries are copied verbatim from `transfer.endpoints` (service DNS names
/// in the shipped Kubernetes deployment). Comparing the strings alone therefore
/// never matches, and every lease gets handed back forever. Resolving both sides
/// first makes a name and the IP it points at compare equal.
fn same_endpoint(a: &str, b: &str) -> bool {
    same_endpoint_with(a, b, |addr| {
        addr.to_socket_addrs()
            .map(|addrs| addrs.collect::<Vec<_>>())
            .unwrap_or_default()
    })
}

/// Resolver-injected form of [`same_endpoint`], so the comparison can be tested
/// without depending on the ambient DNS zone.
fn same_endpoint_with(a: &str, b: &str, resolve: impl Fn(&str) -> Vec<SocketAddr>) -> bool {
    if a == b {
        return true;
    }
    let right = resolve(b);
    if right.is_empty() {
        return false;
    }
    resolve(a).iter().any(|addr| right.contains(addr))
}

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
        (!self.owns_addr(&owner)).then_some(owner)
    }

    /// True when `addr` names this instance. Addresses are compared after
    /// resolution, so an endpoint written as a DNS name still matches the IP
    /// this instance bound to.
    pub fn owns_addr(&self, addr: &str) -> bool {
        same_endpoint(addr, &self.self_addr)
    }

    /// True when `transfer.endpoints` lists this instance at all. When it does
    /// not, no hand-back can ever land here, so a caller that would otherwise
    /// return a lease must run the job locally instead of looping forever.
    pub fn in_ring(&self) -> bool {
        let ring = self.ring();
        ring.all_addrs()
            .iter()
            .any(|addr| same_endpoint(addr, &self.self_addr))
    }

    /// True when this instance is the placement owner of `key`. An empty ring
    /// (no configured endpoints) means nobody can claim ownership, so everyone
    /// serves locally instead of stalling.
    pub fn owns(&self, key: &str) -> bool {
        match self.owner_of(key) {
            Some(owner) => self.owns_addr(&owner),
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
        if self.owns_addr(addr) {
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

    /// The shipped Kubernetes deployment writes the two sides of a placement
    /// comparison differently: `transfer.endpoints` holds service DNS names
    /// while `transfer.hostname` is the pod IP exported by the entrypoint
    /// (`hostname -i`). They name the same instance, so placement must
    /// recognise itself; comparing the raw strings does not, which is what
    /// livelocked every job between three instances.
    #[test]
    fn placement_matches_a_service_name_and_the_ip_it_resolves_to() {
        const MASTER_0: &str = "cv-master-0.cv-master.curvine.svc.cluster.local:8010";
        const MASTER_2: &str = "cv-master-2.cv-master.curvine.svc.cluster.local:8010";
        const MASTER_2_OTHER_PORT: &str = "cv-master-2.cv-master.curvine.svc.cluster.local:8011";
        let resolve = |addr: &str| -> Vec<SocketAddr> {
            let resolved = match addr {
                MASTER_0 => "10.209.150.229:8010",
                MASTER_2 => "10.209.150.228:8010",
                other => other,
            };
            resolved.parse().ok().into_iter().collect()
        };
        // The identity this instance bound to, taken from `hostname -i`.
        let self_addr = "10.209.150.228:8010";

        assert!(
            same_endpoint_with(MASTER_2, self_addr, resolve),
            "an endpoint naming this instance by DNS must be recognised as self"
        );
        assert!(
            !same_endpoint_with(MASTER_0, self_addr, resolve),
            "a sibling instance must stay elsewhere"
        );
        assert!(
            !same_endpoint_with(MASTER_2_OTHER_PORT, self_addr, resolve),
            "a different port is a different endpoint"
        );
    }

    #[test]
    fn same_endpoint_compares_ip_literals_without_dns() {
        assert!(same_endpoint("127.0.0.1:8010", "127.0.0.1:8010"));
        assert!(!same_endpoint("127.0.0.1:8010", "127.0.0.1:8011"));
        assert!(!same_endpoint("127.0.0.1:8010", "not a host:8010"));
    }

    /// End-to-end form of the mixed-spelling bug: the ring entry and the bound
    /// address name one instance but are written differently, so the shipped
    /// `owner == self_addr` comparison answered "placed elsewhere" for every
    /// job and the scheduler handed each lease straight back, forever.
    #[test]
    fn placement_endpoint_and_self_addr_may_be_spelled_differently() {
        use curvine_rpc::client::{ClientConf, ClientFactory};
        use std::net::ToSocketAddrs;

        // The address this instance binds to, in numeric form (what the
        // entrypoint exports as `CURVINE_TRANSFER_HOSTNAME`).
        let self_addr = "localhost:8010"
            .to_socket_addrs()
            .unwrap()
            .next()
            .unwrap()
            .to_string();
        let endpoint = "localhost:8010".to_string();
        assert_ne!(endpoint, self_addr, "the two spellings must differ");

        let factory = Arc::new(ClientFactory::new(ClientConf::default()));
        let forwarder = Forwarder::new(vec![endpoint.clone()], self_addr.clone(), factory);
        let key = "client-request-1";

        assert_eq!(forwarder.owner_of(key).as_deref(), Some(endpoint.as_str()));
        assert!(
            forwarder.owns(key),
            "placement owner {endpoint} and self_addr {self_addr} are the same instance"
        );
    }
}
