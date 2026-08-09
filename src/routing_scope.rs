// Copyright ScyllaDB, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Routing scope for directing requests to specific subsets of nodes in a cluster.
//!
//! Routing scopes allow user to specify which nodes should be used for load balancing,
//! with optional fallback to a wider scope if no nodes are available in the preferred one.

pub(crate) const MAX_ROUTING_SCOPE_CHAIN_DEPTH: usize = 16;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoutingScope {
    dc: Option<String>,
    rack: Option<String>,
    fallback: Option<Box<RoutingScope>>,
    fallback_chain_valid: bool,
}

impl RoutingScope {
    /// Routes across the whole cluster.
    ///
    /// The client queries `/localnodes` on configured seed hosts and
    /// already-known live nodes, then unions the returned node lists.
    /// Provide at least one working seed host from every datacenter that should
    /// receive traffic.
    pub fn from_cluster() -> Self {
        Self {
            dc: None,
            rack: None,
            fallback: None,
            fallback_chain_valid: true,
        }
    }

    pub fn from_datacenter(dc: String) -> Self {
        if dc.is_empty() {
            Self::from_cluster()
        } else {
            Self {
                dc: Some(dc),
                ..Self::from_cluster()
            }
        }
    }

    pub fn from_rack(dc: String, rack: String) -> Self {
        if dc.is_empty() {
            Self::from_cluster()
        } else if rack.is_empty() {
            Self::from_datacenter(dc)
        } else {
            Self {
                dc: Some(dc),
                rack: Some(rack),
                ..Self::from_cluster()
            }
        }
    }

    /// Sets a fallback for the routing scope that is used if no nodes are available in the preferred scope.
    ///
    /// This function can be called multiple times to create a chain of fallback scopes.
    /// Each call of this function adds the new fallback scope at the end of the existing fallback chain.
    /// Requests are always routed to the most preferred scope in the chain that has available nodes.
    ///
    /// Keep in mind that subsequent fallback scope should ideally be broader than or equal to the
    /// previous one, e.g., (rack -> datacenter -> cluster) or (rack -> another rack -> datacenter -> cluster).
    /// Making a fallback narrower, e.g., (datacenter -> rack) or (cluster -> datacenter),
    /// may be redundant if the set of nodes in the next scope is a subset of the previous one.
    /// Fallback chains are limited to 16 scopes. An append that would exceed
    /// that bound invalidates the chain so client-side routing fails closed.
    pub fn with_fallback(mut self, new_fallback: RoutingScope) -> Self {
        let Some(current_depth) = self.scope_chain().map(|chain| chain.len()) else {
            self.fallback_chain_valid = false;
            return self;
        };
        let Some(new_depth) = new_fallback.scope_chain().map(|chain| chain.len()) else {
            self.fallback_chain_valid = false;
            return self;
        };
        if current_depth
            .checked_add(new_depth)
            .is_none_or(|depth| depth > MAX_ROUTING_SCOPE_CHAIN_DEPTH)
        {
            // The API predates fallible builders. Preserve its signature but
            // remember that the requested chain was rejected so discovery can
            // fail closed instead of silently authorizing a shorter chain.
            self.fallback_chain_valid = false;
            return self;
        }

        let mut tail = &mut self;
        for _ in 1..current_depth {
            tail = tail
                .fallback
                .as_deref_mut()
                .expect("validated fallback depth has a next scope");
        }
        tail.fallback = Some(Box::new(new_fallback));
        self
    }

    /// Appends the datacenter and rack parameters to the given URL as query parameters, if they are set in the scope.
    /// append_pair performs URL encoding.
    pub fn build_localnodes_url(&self, mut base_url: url::Url) -> url::Url {
        base_url.set_path("/localnodes");
        if self.dc.is_some() {
            let mut query = base_url.query_pairs_mut();
            if let Some(dc) = &self.dc {
                query.append_pair("dc", dc);
            }
            if let Some(rack) = &self.rack {
                query.append_pair("rack", rack);
            }
        }
        base_url
    }

    pub(crate) fn is_cluster(&self) -> bool {
        self.dc.is_none() && self.rack.is_none()
    }

    pub fn fallback(&self) -> Option<&RoutingScope> {
        self.fallback.as_deref()
    }

    pub fn dc(&self) -> Option<&str> {
        self.dc.as_deref()
    }

    pub fn rack(&self) -> Option<&str> {
        self.rack.as_deref()
    }

    pub(crate) fn scope_chain(&self) -> Option<Vec<&RoutingScope>> {
        let mut chain = Vec::with_capacity(MAX_ROUTING_SCOPE_CHAIN_DEPTH);
        let mut current = Some(self);
        while let Some(scope) = current {
            if !scope.fallback_chain_valid || chain.len() >= MAX_ROUTING_SCOPE_CHAIN_DEPTH {
                return None;
            }
            chain.push(scope);
            current = scope.fallback.as_deref();
        }
        Some(chain)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cluster_scope() {
        let scope = RoutingScope::from_cluster();
        assert_eq!(scope.dc(), None);
        assert_eq!(scope.rack(), None);
        assert!(scope.fallback().is_none());
        let url = url::Url::parse("http://localhost/").unwrap();
        assert_eq!(
            scope.build_localnodes_url(url).as_str(),
            "http://localhost/localnodes"
        );
    }

    #[test]
    fn test_datacenter_scope() {
        let scope = RoutingScope::from_datacenter("dc1".to_string());
        assert_eq!(scope.dc(), Some("dc1"));
        assert_eq!(scope.rack(), None);
        assert!(scope.fallback().is_none());
        let url = url::Url::parse("http://localhost/").unwrap();
        assert_eq!(
            scope.build_localnodes_url(url).as_str(),
            "http://localhost/localnodes?dc=dc1"
        );
    }

    #[test]
    fn test_rack_scope() {
        let scope = RoutingScope::from_rack("dc1".to_string(), "rack1".to_string());
        assert_eq!(scope.dc(), Some("dc1"));
        assert_eq!(scope.rack(), Some("rack1"));
        assert!(scope.fallback().is_none());
        let url = url::Url::parse("http://localhost/").unwrap();
        assert_eq!(
            scope.build_localnodes_url(url).as_str(),
            "http://localhost/localnodes?dc=dc1&rack=rack1"
        );
    }

    #[test]
    fn test_with_fallback() {
        let scope = RoutingScope::from_rack("dc1".to_string(), "rack1".to_string())
            .with_fallback(RoutingScope::from_datacenter("dc1".to_string()))
            .with_fallback(RoutingScope::from_cluster());

        assert_eq!(scope.dc(), Some("dc1"));
        assert_eq!(scope.rack(), Some("rack1"));

        let first_fallback = scope.fallback().expect("Should have a fallback");
        assert_eq!(first_fallback.dc(), Some("dc1"));
        assert_eq!(first_fallback.rack(), None);

        let second_fallback = first_fallback
            .fallback()
            .expect("Should have a second fallback");
        assert_eq!(second_fallback.dc(), None);
        assert_eq!(second_fallback.rack(), None);
        assert!(second_fallback.fallback().is_none());
    }

    #[test]
    fn test_localnodes_query_encoding() {
        let scope = RoutingScope::from_rack("dc 1".to_string(), "rack&1".to_string());
        let url = url::Url::parse("http://localhost/").unwrap();
        assert_eq!(
            scope.build_localnodes_url(url).as_str(),
            "http://localhost/localnodes?dc=dc+1&rack=rack%261"
        );
    }

    #[test]
    fn test_impossible_to_create_cyclic_fallback() {
        // Because `RoutingScope` uses `Box` which has exclusive ownership, it is impossible to create a self-referential cycle.
        // Even if a user attempts to create a "cycle" by cloning the scope and passing it
        // to itself, it creates a finite, linear chain of completely separate allocations.

        let base = RoutingScope::from_rack("dc1".to_string(), "rack1".to_string());

        let scope = base.clone().with_fallback(base);

        // We can prove it's not a cycle because by showing there is None at the end of the fallback chain.
        assert!(scope.fallback().unwrap().fallback().is_none());
    }

    #[test]
    fn test_fallback_associativity() {
        let rs1 = RoutingScope::from_rack("dc1".to_string(), "rack1".to_string());
        let rs2 = RoutingScope::from_datacenter("dc1".to_string());
        let rs3 = RoutingScope::from_rack("dc2".to_string(), "rack2".to_string());
        let rs4 = RoutingScope::from_datacenter("dc2".to_string());
        let rs5 = RoutingScope::from_cluster();

        // Chain 1: rs1.with_fallback(rs2.with_fallback(rs3)).with_fallback(rs4.with_fallback(rs5))
        let chain1 = rs1
            .clone()
            .with_fallback(rs2.clone().with_fallback(rs3.clone()))
            .with_fallback(rs4.clone().with_fallback(rs5.clone()));

        // Chain 2: rs1.with_fallback(rs2).with_fallback(rs3).with_fallback(rs4).with_fallback(rs5)
        let chain2 = rs1
            .clone()
            .with_fallback(rs2.clone())
            .with_fallback(rs3.clone())
            .with_fallback(rs4.clone())
            .with_fallback(rs5.clone());

        // Chain 3: rs1.with_fallback(rs2.with_fallback(rs3.with_fallback(rs4.with_fallback(rs5))))
        let chain3 = rs1.clone().with_fallback(
            rs2.clone().with_fallback(
                rs3.clone()
                    .with_fallback(rs4.clone().with_fallback(rs5.clone())),
            ),
        );

        // Chain 4: rs1.with_fallback(rs2).with_fallback(rs3.with_fallback(rs4)).with_fallback(rs5)
        let chain4 = rs1
            .clone()
            .with_fallback(
                rs2.clone()
                    .with_fallback(rs3.clone().with_fallback(rs4.clone())),
            )
            .with_fallback(rs5.clone());

        assert_eq!(chain1, chain2);
        assert_eq!(chain2, chain3);
        assert_eq!(chain3, chain4);
    }

    #[test]
    fn fallback_depth_boundary_is_bounded_and_overflow_is_invalid() {
        let mut boundary = RoutingScope::from_datacenter("dc-0".to_string());
        for index in 1..MAX_ROUTING_SCOPE_CHAIN_DEPTH {
            boundary = boundary.with_fallback(RoutingScope::from_datacenter(format!("dc-{index}")));
        }
        assert_eq!(
            boundary.scope_chain().unwrap().len(),
            MAX_ROUTING_SCOPE_CHAIN_DEPTH
        );

        let overflow = boundary.with_fallback(RoutingScope::from_cluster());
        assert!(overflow.scope_chain().is_none());
    }
}
