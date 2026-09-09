# AESO schema compatibility and migration policy

The current adapter contract is `aeso-csd-hourly-v1`. It recognizes the exact 12-column hourly CSD header documented in `README.md`, including column order, and maps `Date (MST)`, `Asset Short Name`, `Asset Name`, and `Volume` into the clean schema.

## Compatibility rules

| Upstream change | Current behavior | Required migration |
|---|---|---|
| Values change but the 12-column contract is unchanged | Compatible; normal validation and overlap policy apply | None |
| A column is added, removed, renamed, or reordered | Fail closed with `WRONG_COLUMN_STRUCTURE`; no row is mapped under a guessed schema | Inspect a real new file, introduce a versioned adapter, and add fixtures for both contracts |
| Timestamp format or timezone meaning changes | Timestamp rows fail parsing, or the header fails if renamed | Introduce a new adapter version with explicit timezone semantics; never reinterpret previously loaded instants in place |
| `Volume` representation changes | Nonconforming rows receive `NON_NUMERIC_GENERATION` | Version the parsing rule only after confirming the published contract and adding tests |
| AESO republishes different bytes with overlapping asset/timestamp keys | The different hash is a new batch; existing clean rows win and incoming overlaps receive `CROSS_BATCH_OVERLAP` | Review as a correction workflow before adding an explicit replacement policy |
| The exact same bytes are presented again | `skipped_idempotent`; zero rows and zero provenance records are added | None |

Schema drift is intentionally visible. The adapter must not accept an upstream change by fuzzy header matching because that could silently put values into the wrong clean columns.

## Adapter migration procedure

1. Download and inspect a real artifact exhibiting the new contract.
2. Preserve `aeso-csd-hourly-v1` behavior for already-supported historical files.
3. Add a new adapter/schema identifier, such as `aeso-csd-hourly-v2`, rather than changing the meaning of v1 in place.
4. Add synthetic tests for every changed field plus a real-header fixture.
5. Define how adapter selection occurs before enabling the new contract. Header-based selection must use exact, versioned signatures.
6. Record the selected identifier in `provenance.adapter_schema_version`.
7. Re-run the complete CI matrix from a clean checkout before release.

## Local DuckDB migration behavior

Database schema version 2 adds:

- `pipeline_schema`, which records the component database version;
- `provenance.adapter_schema_version`;
- `provenance.overlap_policy`;
- a unique source-hash index for artifact idempotency;
- a unique `(asset_id, timestamp)` index as a final overlap guard.

A version-1 database is upgraded automatically when its existing hashes and clean keys are already unique. Legacy provenance rows are labeled `legacy-unversioned` and `legacy-unenforced` because the pipeline cannot infer guarantees that were not previously recorded.

If a legacy database already contains duplicate source hashes or overlapping clean keys, startup raises `DatabaseMigrationError`. The migration deliberately does not delete, merge, or choose a winner. The operator must either:

- retain the old database as an immutable audit artifact and ingest into a new database; or
- review the conflicts, explicitly choose authoritative records, back up the file, and then migrate.

This refusal is a compatibility guarantee: upgrading the component never silently changes historical observations.
