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

//! Hashes DynamoDB AttributeValue objects using MurmurHash3.
//!
//! Supports the partition key types allowed by ScyllaDB Alternator:
//!
//! * S (String) - Type prefix 0x01 + UTF-8 bytes
//! * N (Number) - Type prefix 0x02 + UTF-8 bytes of string representation
//! * B (Binary) - Type prefix 0x03 + raw bytes
//!
//! Other DynamoDB types (BOOL, NULL, SS, NS, BS, L, M) are not supported as partition keys in
//! Alternator and will return `None`.
//!
//! # Composite Partition Keys
//!
//! This hasher operates on individual `AttributeValue` objects. For tables with composite
//! keys (partition key + sort key), only the partition key should be hashed for routing purposes,
//! since DynamoDB partitions data by partition key only. The sort key determines ordering within a
//! partition but does not affect which node stores the data.
//!
//! # Number Representation
//!
//! Number values (N type) are hashed using their exact string representation as stored in
//! DynamoDB. This means that numerically equivalent values with different representations will
//! produce different hashes:
//!
//! * `"42"` and `"42.0"` produce different hashes
//! * `"1e2"` and `"100"` produce different hashes
//! * `"1.0"` and `"1.00"` produce different hashes
//!
//! This behavior preserves the exact representation stored in DynamoDB and matches how DynamoDB
//! itself handles number comparisons in certain contexts.
//!
//! # Stable affinity hash format
//!
//! Affinity routing encodes supported partition-key values as follows:
//!
//! * Type prefixes must use the exact byte values (0x01 for S, 0x02 for N, 0x03 for B)
//! * Strings must be encoded as UTF-8 bytes
//! * The MurmurHash3 implementation must use the x64_128 variant with seed 0, returning the
//!   first 64 bits
//!
//! # Performance Characteristics
//!
//! Time complexity is O(n) for all supported types where n is the byte length.
//!
//! Space complexity is O(n) as the entire value is converted to bytes before hashing.

use aws_sdk_dynamodb::types::AttributeValue;
use std::io::Cursor;

// Type prefixes for the affinity hash format.
const TYPE_STRING: u8 = 0x01;
const TYPE_NUMBER: u8 = 0x02;
const TYPE_BINARY: u8 = 0x03;

/// Computes the affinity hash for a DynamoDB `AttributeValue` partition key.
pub(crate) fn hash_attribute_value(value: &AttributeValue) -> Option<u64> {
    let (prefix, bytes): (u8, &[u8]) = match value {
        AttributeValue::S(s) => (TYPE_STRING, s.as_bytes()),
        AttributeValue::N(n) => (TYPE_NUMBER, n.as_bytes()),
        AttributeValue::B(b) => (TYPE_BINARY, b.as_ref()),
        _ => return None,
    };
    let mut data = Vec::with_capacity(1 + bytes.len());
    data.push(prefix);
    data.extend_from_slice(bytes);
    let hash = murmur3::murmur3_x64_128(&mut Cursor::new(data), 0)
        .expect("reading from an in-memory partition key cannot fail");
    Some(hash as u64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_sdk_dynamodb::primitives::Blob;
    use aws_sdk_dynamodb::types::AttributeValue;

    // These fixed vectors pin this crate's affinity hash format. Only S, N,
    // and B are covered because they are the partition-key types Alternator
    // supports.
    fn assert_hash(value: &AttributeValue, expected_signed: i64) {
        let actual = hash_attribute_value(value).expect("supported type");
        assert_eq!(
            actual as i64, expected_signed,
            "got {:#018x}, expected {:#018x}",
            actual, expected_signed as u64
        );
    }

    // ----- Strings (partition key supported) -----

    #[test]
    fn fixed_vector_string_hello() {
        assert_hash(&AttributeValue::S("hello".into()), 8815023923555918238);
    }

    #[test]
    fn fixed_vector_string_empty() {
        assert_hash(&AttributeValue::S("".into()), 8849112093580131862);
    }

    #[test]
    fn fixed_vector_string_user_123() {
        assert_hash(&AttributeValue::S("user_123".into()), -4025731529809423594);
    }

    #[test]
    fn fixed_vector_string_unicode() {
        assert_hash(
            &AttributeValue::S("こんにちは".into()),
            -8746014667889746860,
        );
    }

    // ----- Numbers (partition key supported) -----

    #[test]
    fn fixed_vector_number_42() {
        assert_hash(&AttributeValue::N("42".into()), -5061732451827723051);
    }

    #[test]
    fn fixed_vector_number_negative() {
        assert_hash(&AttributeValue::N("-12345".into()), 2496798676881075539);
    }

    #[test]
    fn fixed_vector_number_decimal() {
        assert_hash(&AttributeValue::N("3.14159".into()), 2139945193071104172);
    }

    #[test]
    fn fixed_vector_number_scientific() {
        assert_hash(&AttributeValue::N("1.23E10".into()), -8571981415737439826);
    }

    // ----- Binary (partition key supported) -----

    #[test]
    fn fixed_vector_binary_bytes() {
        assert_hash(
            &AttributeValue::B(Blob::new(vec![0x01, 0x02, 0x03])),
            5026299041734804437,
        );
    }

    #[test]
    fn fixed_vector_binary_empty() {
        assert_hash(&AttributeValue::B(Blob::new(vec![])), 8244620721157455449);
    }

    #[test]
    fn fixed_vector_binary_high_bytes() {
        assert_hash(
            &AttributeValue::B(Blob::new(vec![0xFF, 0x00, 0x80])),
            14533934253577680,
        );
    }

    // ----- Type collision prevention -----

    #[test]
    fn fixed_vector_string_12345_distinct_from_number_12345() {
        // Same bytes, different type prefix → different hash.
        assert_hash(&AttributeValue::S("12345".into()), -6122888897254035317);
        assert_hash(&AttributeValue::N("12345".into()), -3190731486301745196);
    }

    #[test]
    fn fixed_vector_binary_12345_distinct_from_string() {
        assert_hash(
            &AttributeValue::B(Blob::new(b"12345".to_vec())),
            -3752463870508600385,
        );
    }

    // ----- Minimal-implementation behavior -----
    // We do not support BOOL/NULL/SS/NS/BS/L/M. Verify they return None.

    #[test]
    fn unsupported_types_return_none() {
        assert!(hash_attribute_value(&AttributeValue::Bool(true)).is_none());
        assert!(hash_attribute_value(&AttributeValue::Null(true)).is_none());
        assert!(hash_attribute_value(&AttributeValue::Ss(vec!["a".into()])).is_none());
        assert!(hash_attribute_value(&AttributeValue::Ns(vec!["1".into()])).is_none());
        assert!(hash_attribute_value(&AttributeValue::Bs(vec![Blob::new(vec![0])])).is_none());
        assert!(hash_attribute_value(&AttributeValue::L(vec![])).is_none());
        assert!(hash_attribute_value(&AttributeValue::M(Default::default())).is_none());
    }

    #[test]
    fn deterministic() {
        let v = AttributeValue::S("alice".into());
        assert_eq!(hash_attribute_value(&v), hash_attribute_value(&v));
    }
}
