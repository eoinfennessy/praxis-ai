// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Praxis Contributors

//! Conservative admission for the first, plain Responses agentic path.
//!
//! All paths that retain payload outside this small set of owners are rejected
//! before dispatch. These factors include the raw body, parsed JSON trees,
//! canonical input copies, response normalization, and SSE framing. Follow-up
//! owner patches can replace these coarse reserves with exact charges.

/// Copies and parsing capacity held while classifying and validating input.
const INPUT_WIRE_MULTIPLIER: usize = 32;
/// Copies and serialization capacity held while processing model output.
const OUTPUT_WIRE_MULTIPLIER: usize = 64;
/// Reserve for each JSON value/key, including collection spare capacity.
const JSON_NODE_RESERVE: usize = 256;

/// Request-scoped charge that survives every inference round.
#[derive(Clone, Copy, Debug)]
pub(crate) struct SimpleBudget {
    /// The smallest configured limit of every reachable loop instance.
    limit: usize,
    /// Input charge retained throughout execution.
    input_charge: usize,
    /// Sum of provider wire bytes from all rounds.
    output_wire_bytes: usize,
}

impl SimpleBudget {
    /// Admit a request after the allocation-free ingress scan.
    pub(crate) fn new(limit: usize, input_charge: usize) -> Option<Self> {
        // Core's response Vec may keep spare capacity alongside the live raw
        // body. Reserve three times its validated `limit / 8` transport cap.
        input_charge
            .checked_add((limit / 8).checked_mul(3)?)
            .filter(|charge| *charge <= limit)?;
        Some(Self {
            limit,
            input_charge,
            output_wire_bytes: 0,
        })
    }

    /// Lower the effective limit when another loop instance is reached.
    pub(crate) fn lower_limit(&mut self, limit: usize) -> bool {
        self.limit = self.limit.min(limit);
        self.input_charge
            .checked_add((self.limit / 8).saturating_mul(3))
            .is_some_and(|peak| peak <= self.limit)
            && self.charge().is_some_and(|charge| charge <= self.limit)
    }

    /// Preflight one more provider chunk before any response parser sees it.
    pub(crate) fn admit_output(&mut self, bytes: usize) -> bool {
        let Some(next) = self.output_wire_bytes.checked_add(bytes) else {
            return false;
        };
        let Some(total) = next
            .checked_mul(OUTPUT_WIRE_MULTIPLIER)
            .and_then(|output| output.checked_add(self.input_charge))
        else {
            return false;
        };
        if total > self.limit {
            return false;
        }
        self.output_wire_bytes = next;
        true
    }

    fn charge(self) -> Option<usize> {
        self.output_wire_bytes
            .checked_mul(OUTPUT_WIRE_MULTIPLIER)?
            .checked_add(self.input_charge)
    }
}

/// Bound the simultaneously live raw and parsed create-body projections.
/// This scans borrowed bytes and allocates no request payload.
pub(crate) fn input_charge(bytes: &[u8]) -> Option<usize> {
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
            b'{' | b'[' => {
                nodes = nodes.checked_add(1)?;
                in_number = false;
            },
            b'-' | b'0'..=b'9' if !in_number => {
                nodes = nodes.checked_add(1)?;
                in_number = true;
            },
            b'-' | b'0'..=b'9' | b'.' | b'e' | b'E' | b'+' if in_number => {},
            b't' | b'f' | b'n' => {
                nodes = nodes.checked_add(1)?;
                in_number = false;
            },
            _ => in_number = false,
        }
    }
    bytes
        .len()
        .checked_mul(INPUT_WIRE_MULTIPLIER)?
        .checked_add(nodes.checked_mul(JSON_NODE_RESERVE)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cumulative_output_and_lower_limit() {
        assert!(input_charge(br#"{"input":"hello","store":false}"#).is_some());
        let mut budget = SimpleBudget::new(4_096, 192).unwrap();
        assert!(budget.admit_output(61));
        assert!(!budget.admit_output(1));
        assert!(budget.lower_limit(4_096));
        assert!(!budget.lower_limit(4_095));
    }

    #[test]
    fn checked_arithmetic_fails_closed() {
        let mut budget = SimpleBudget::new(usize::MAX, 0).unwrap();
        assert!(!budget.admit_output(usize::MAX));
    }
}
