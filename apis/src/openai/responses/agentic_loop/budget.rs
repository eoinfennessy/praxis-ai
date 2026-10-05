// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Praxis Contributors

//! Conservative admission for the first, plain Responses agentic path.
//!
//! All paths that retain payload outside this small set of owners are rejected
//! before dispatch. These factors include the raw body, parsed JSON trees,
//! canonical input copies, response normalization, Store persistence, and SSE
//! framing. Follow-up owner patches can replace these coarse reserves with
//! exact charges.

/// Copies and parsing capacity held while classifying and validating input.
pub(super) const INPUT_WIRE_MULTIPLIER: usize = 32;
/// The router buffer is reserved at one eighth of the configured ceiling.
pub(super) const IRR_RESPONSE_DIVISOR: usize = 8;
/// Copies and serialization capacity held while processing model output.
const OUTPUT_WIRE_MULTIPLIER: usize = 64;
/// Reserve for each JSON value/key, including collection spare capacity.
const JSON_NODE_RESERVE: usize = 256;
/// Parsed provider values may own a four-slot Vec or map for only a few wire
/// bytes. Leave room for those allocations and their response-state copies.
const OUTPUT_NODE_RESERVE: usize = 8_192;

/// Request-scoped charge that survives every inference round.
#[derive(Clone, Copy, Debug)]
pub(crate) struct SimpleBudget {
    /// The smallest configured limit of every reachable loop instance.
    limit: usize,
    /// Input charge retained throughout execution.
    input_charge: usize,
    /// Additional request-side owners admitted before their payload is copied.
    additional_input_charge: usize,
    /// Additional allowance for the response Store's request input snapshot.
    store_input_charge: usize,
    /// Conservative sum of provider payload charges from all rounds.
    output_charge: usize,
    /// Separate allowance for Store's persisted response and history copies.
    store_output_charge: usize,
    /// Whether Store persists this create response and owns another output projection.
    store_response: bool,
}

impl SimpleBudget {
    /// Headroom after all request-wide reserves, including the transport.
    pub(crate) fn remaining_bytes(self) -> Option<usize> {
        self.limit.checked_sub(self.charge()?)
    }

    /// Admit a request after the allocation-free ingress scan.
    #[cfg(test)]
    pub(crate) fn new(limit: usize, input_charge: usize) -> Option<Self> {
        Self::new_with_store(limit, input_charge, false)
    }

    /// Admit a create that will retain Store input and response projections.
    pub(crate) fn new_with_store(limit: usize, input_charge: usize, store_response: bool) -> Option<Self> {
        let store_input_charge = if store_response { input_charge } else { 0 };
        // Core's response Vec may keep spare capacity alongside the live raw
        // body. Reserve three times its validated `limit / 8` transport cap.
        if input_charge
            .checked_add(store_input_charge)?
            .checked_add(response_reserve(limit)?)?
            > limit
        {
            return None;
        }
        Some(Self {
            limit,
            input_charge,
            additional_input_charge: 0,
            store_input_charge,
            output_charge: 0,
            store_output_charge: 0,
            store_response,
        })
    }

    /// Lower the effective limit when another loop instance is reached.
    pub(crate) fn lower_limit(&mut self, limit: usize) -> bool {
        self.limit = self.limit.min(limit);
        self.charge().is_some_and(|charge| charge <= self.limit)
    }

    /// Reserve an independently owned request-side payload before allocating it.
    /// Failed reservations leave the previous charge intact.
    pub(crate) fn reserve_additional_input(&mut self, charge: usize) -> bool {
        let Some(next) = self.additional_input_charge.checked_add(charge) else {
            return false;
        };
        let Some(total) = self.charge().and_then(|total| total.checked_add(charge)) else {
            return false;
        };
        if total > self.limit {
            return false;
        }
        self.additional_input_charge = next;
        true
    }

    /// Preflight one more provider chunk before any response parser sees it.
    pub(crate) fn admit_output(&mut self, bytes: &[u8]) -> bool {
        let Some(additional) = output_charge(bytes) else {
            return false;
        };
        let Some(next_output) = self.output_charge.checked_add(additional) else {
            return false;
        };
        let Some(next_store) = self
            .store_output_charge
            .checked_add(if self.store_response { additional } else { 0 })
        else {
            return false;
        };
        let Some(total) = next_output
            .checked_add(next_store)
            .and_then(|charge| charge.checked_add(self.input_charge))
            .and_then(|charge| charge.checked_add(self.additional_input_charge))
            .and_then(|charge| charge.checked_add(self.store_input_charge))
            .and_then(|charge| charge.checked_add(response_reserve(self.limit)?))
        else {
            return false;
        };
        if total > self.limit {
            return false;
        }
        self.output_charge = next_output;
        self.store_output_charge = next_store;
        true
    }

    /// Return the current charge including the IRR transport reserve.
    fn charge(self) -> Option<usize> {
        self.output_charge
            .checked_add(self.store_output_charge)?
            .checked_add(self.input_charge)?
            .checked_add(self.additional_input_charge)?
            .checked_add(self.store_input_charge)?
            .checked_add(response_reserve(self.limit)?)
    }
}

/// Account for core's buffered response capacity before parsing or copying.
fn response_reserve(limit: usize) -> Option<usize> {
    (limit / IRR_RESPONSE_DIVISOR).checked_mul(3)
}

/// Bound the simultaneously live raw and parsed create-body projections.
/// This scans borrowed bytes and allocates no request payload.
pub(crate) fn input_charge(bytes: &[u8]) -> Option<usize> {
    bytes
        .len()
        .checked_mul(INPUT_WIRE_MULTIPLIER)?
        .checked_add(json_node_count(bytes)?.checked_mul(JSON_NODE_RESERVE)?)
}

/// Charge provider structure before its first JSON parser allocates.
pub(crate) fn output_charge(bytes: &[u8]) -> Option<usize> {
    bytes
        .len()
        .checked_mul(OUTPUT_WIRE_MULTIPLIER)?
        .checked_add(json_node_count(bytes)?.checked_mul(OUTPUT_NODE_RESERVE)?)
}

/// Count syntactic JSON nodes without allocating or trusting the payload.
#[expect(
    clippy::too_many_lines,
    reason = "the lexical scan keeps string and number state without allocating"
)]
fn json_node_count(bytes: &[u8]) -> Option<usize> {
    let mut nodes = 0_usize;
    let mut in_string = false;
    let mut escaped = false;
    let mut in_number = false;
    for &byte in bytes {
        if in_string {
            if escaped {
                escaped = false;
            } else if byte == b'\\' {
                escaped = true;
            } else if byte == b'"' {
                in_string = false;
            }
            continue;
        }
        match byte {
            b'"' => {
                nodes = nodes.checked_add(1)?;
                in_string = true;
                in_number = false;
            },
            b'{' | b'[' | b't' | b'f' | b'n' => {
                nodes = nodes.checked_add(1)?;
                in_number = false;
            },
            b'-' | b'0'..=b'9' if !in_number => {
                nodes = nodes.checked_add(1)?;
                in_number = true;
            },
            b'-' | b'0'..=b'9' | b'.' | b'e' | b'E' | b'+' if in_number => {},
            _ => in_number = false,
        }
    }
    Some(nodes)
}

#[cfg(test)]
#[expect(clippy::unwrap_used, reason = "unit tests construct validated budgets")]
mod tests {
    use super::*;

    #[test]
    fn cumulative_output_and_lower_limit() {
        assert!(input_charge(br#"{"input":"hello","store":false}"#).is_some());
        let mut budget = SimpleBudget::new(4_096, 192).unwrap();
        assert!(budget.admit_output(&[b' '; 37]));
        assert!(!budget.admit_output(&[b' '; 1]));
        assert!(budget.lower_limit(4_096));
        assert!(!budget.lower_limit(4_092));
    }

    #[test]
    fn restored_owner_reduces_output_headroom() {
        let mut budget = SimpleBudget::new(8_388_608, 1_000).unwrap();
        let before = budget.remaining_bytes().unwrap();
        assert!(budget.reserve_additional_input(128_000));
        assert_eq!(budget.remaining_bytes(), Some(before - 128_000));
    }

    #[test]
    fn store_reserves_independent_input_and_output_projections() {
        assert!(SimpleBudget::new(4_096, 1_500).is_some());
        assert!(SimpleBudget::new_with_store(4_096, 1_500, true).is_none());

        let mut plain = SimpleBudget::new(4_096, 100).unwrap();
        let mut stored = SimpleBudget::new_with_store(4_096, 100, true).unwrap();
        assert!(plain.admit_output(&[b' '; 37]));
        assert!(!stored.admit_output(&[b' '; 37]));
    }

    #[test]
    fn additional_input_reservation_is_cumulative_and_atomic() {
        let mut budget = SimpleBudget::new(4_096, 100).unwrap();
        assert!(budget.reserve_additional_input(2_000));
        assert!(!budget.reserve_additional_input(1_000));
        assert!(budget.reserve_additional_input(400));
        assert!(!budget.lower_limit(4_000));
        assert!(!budget.reserve_additional_input(usize::MAX));
    }

    #[test]
    fn checked_arithmetic_fails_closed() {
        let mut budget = SimpleBudget::new(usize::MAX, 0).unwrap();
        budget.output_charge = usize::MAX;
        assert!(!budget.admit_output(b" "));
    }

    #[test]
    fn compact_nested_provider_output_is_rejected_before_parse() {
        let nested = format!("{}0{}", "[".repeat(20), "]".repeat(20));
        let values = std::iter::repeat_n(nested.as_str(), 16_000)
            .collect::<Vec<_>>()
            .join(",");
        let body = format!(
            r#"{{"object":"response","status":"completed","output":[{{"type":"message","content":[{{"type":"output_text","text":"hello"}}],"extra":[{values}]}}]}}"#
        );
        let mut budget = SimpleBudget::new(67_108_864, 2_272).unwrap();
        assert!(body.len() * OUTPUT_WIRE_MULTIPLIER < budget.limit);
        assert!(!budget.admit_output(body.as_bytes()));
    }
}
