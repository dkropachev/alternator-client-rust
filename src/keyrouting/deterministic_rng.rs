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

/// Small deterministic generator used to derive a stable affinity-node order
/// from a partition-key hash.
#[derive(Debug)]
pub(crate) struct DeterministicRng {
    state: u64,
}

impl DeterministicRng {
    /// Creates a deterministic generator seeded with the given value.
    pub(crate) fn new(seed: i64) -> Self {
        let state = match seed as u64 {
            0 => 0x9e37_79b9_7f4a_7c15,
            state => state,
        };
        Self { state }
    }

    fn next_u64(&mut self) -> u64 {
        let mut value = self.state;
        value ^= value >> 12;
        value ^= value << 25;
        value ^= value >> 27;
        self.state = value;
        value.wrapping_mul(0x2545_f491_4f6c_dd1d)
    }

    /// Returns an index in `0..upper_bound`.
    ///
    /// # Panics
    ///
    /// Panics if `upper_bound` is zero.
    pub(crate) fn index(&mut self, upper_bound: usize) -> usize {
        assert!(upper_bound > 0, "upper bound must be positive");
        ((self.next_u64() as u128 * upper_bound as u128) >> 64) as usize
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_seed_produces_same_indices() {
        let mut first = DeterministicRng::new(42);
        let mut second = DeterministicRng::new(42);

        for upper_bound in 1..=64 {
            assert_eq!(first.index(upper_bound), second.index(upper_bound));
        }
    }

    #[test]
    fn indices_stay_within_bounds() {
        let mut rng = DeterministicRng::new(-1);

        for upper_bound in 1..=64 {
            for _ in 0..100 {
                assert!(rng.index(upper_bound) < upper_bound);
            }
        }
    }

    #[test]
    #[should_panic(expected = "upper bound must be positive")]
    fn zero_upper_bound_panics() {
        DeterministicRng::new(1).index(0);
    }
}
