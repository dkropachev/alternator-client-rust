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

//! Integration tests using a real multi-node CCM cluster: load-balancing /
//! routing and HTTP connection reuse, both driven through counting proxies.

#[path = "../common/proxy.rs"]
pub mod proxy;

#[path = "common/scope_utils.rs"]
pub mod scope_utils;

#[path = "common/cluster_utils.rs"]
pub mod cluster_utils;

pub mod load_balancing_tests;

pub mod connection_reuse;
