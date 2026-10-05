// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Praxis Contributors

//! Conservative admission for the first, plain Responses agentic path.
//!
//! All paths that retain payload outside this small set of owners are rejected
//! before dispatch. These factors include the raw body, parsed JSON trees,
//! canonical input copies, response normalization, and SSE framing. Follow-up
//! owner patches can replace these coarse reserves with exact charges.

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
    /// Conservative sum of provider payload charges from all rounds.
    output_charge: usize,
}

impl SimpleBudget {
    /// Admit a request after the allocation-free ingress scan.
    pub(crate) fn new(limit: usize, input_charge: usize) -> Option<Self> {
        // Core's response Vec may keep spare capacity alongside the live raw
        // body. Reserve three times its validated `limit / 8` transport cap.
        if input_charge.checked_add(response_reserve(limit)?)? > limit {
            return None;
        }
        Some(Self {
            limit,
            input_charge,
            output_charge: 0,
        })
    }

    /// Lower the effective limit when another loop instance is reached.
    pub(crate) fn lower_limit(&mut self, limit: usize) -> bool {
        self.limit = self.limit.min(limit);
        self.charge().is_some_and(|charge| charge <= self.limit)
    }

    /// Preflight one more provider chunk before any response parser sees it.
    pub(crate) fn admit_output(&mut self, bytes: &[u8]) -> bool {
        let Some(next) = output_charge(bytes).and_then(|charge| self.output_charge.checked_add(charge)) else {
            return false;
        };
        let Some(total) = next
            .checked_add(self.input_charge)
            .and_then(|charge| charge.checked_add(response_reserve(self.limit)?))
        else {
            return false;
        };
        if total > self.limit {
            return false;
        }
        self.output_charge = next;
        true
    }

    /// Return the current charge including the IRR transport reserve.
    fn charge(self) -> Option<usize> {
        self.output_charge
            .checked_add(self.input_charge)?
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
fn output_charge(bytes: &[u8]) -> Option<usize> {
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
