# Assumptions and scope boundary

## Primary data-quality assumption

> The pipeline treats the generation values published in the AESO CSD dataset as the source-of-record observations for ingestion and validation. It does not attempt to reconcile them against settlement-meter data.

AESO's own documentation distinguishes Current Supply & Demand (CSD) observations from settlement-meter data. CSD generally represents what was generated at a unit, is lower quality than settlement-meter data, and is the only public observation available for some assets.

The pipeline therefore verifies properties it can establish from this input alone:

- the exact published structure;
- parseable, unambiguous timestamps;
- present asset identifiers;
- finite numeric generation values;
- unique asset/timestamp keys within a batch;
- complete source provenance;
- deterministic quarantine rather than silent loss.

It does not assert the physical correctness of each MW value. That would require a second authoritative source, asset-specific tolerance rules, and a defined reconciliation process.

## Secondary implementation assumptions

- `Date (MST)` means a fixed UTC-07:00 clock, consistent with its explicit label. It is converted to an absolute instant and stored as `TIMESTAMPTZ`.
- An AESO ZIP input contains exactly one CSV. Zero or multiple CSV members are rejected as malformed input.
- The first occurrence of an `(asset_id, timestamp)` key is accepted; later occurrences in the same file are quarantined as duplicates.
- A finite negative `Volume` is structurally valid. No undocumented physical lower or upper bound is imposed.
- If `--retrieved-at` is omitted, local file modification time is the best available retrieval-time approximation and is recorded as such by the CLI behavior.

