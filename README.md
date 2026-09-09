# AESO CSD generation pipeline

A small, local-first ingestion and validation component for the Alberta Electric System Operator (AESO) Historical Generation Data (CSD) dataset. It accepts one hourly CSV or AESO ZIP archive, normalizes the observed source layout, quarantines invalid records, records file-level provenance, and writes a JSON validation report plus one DuckDB database.

The component is intentionally bounded: it handles one local hourly input at a time and does not download, schedule, reconcile, or visualize the wider historical archive.

## Reproduce it

Python 3.11 or newer is required. From the repository root:

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe -m pytest
.\.venv\Scripts\python.exe -m aeso_pipeline.run --input data\sample\aeso_hourly_sample.csv --db data\aeso.duckdb
```

`requirements.txt` pins the complete runtime/test dependency set, including transitive packages used by pytest and DuckDB's timezone conversion.

The final line is the single reproducible pipeline command. It creates:

- `data/aeso.duckdb`, containing `clean_generation`, `rejected_rows`, and `provenance`;
- `data/aeso.validation.json`, containing counts, status, reason-code totals, file hash, and source metadata.

The downloaded AESO ZIP can be used directly, without manually extracting it:

```powershell
.\.venv\Scripts\python.exe -m aeso_pipeline.run `
  --input "data\raw\CSD Generation (Hourly) - 2026-06.zip" `
  --db data\aeso.duckdb `
  --source-url "https://aeso.box.com/s/qofgn9axnnw6uq3ip1goiq2ngb11txe5/file/2331073528682"
```

On macOS/Linux, replace `.\.venv\Scripts\python.exe` with `.venv/bin/python` and use forward slashes in paths.

`--retrieved-at` optionally accepts an offset-aware ISO-8601 timestamp. If omitted, the input file's modification time is used as the best local approximation of retrieval time. `--report` overrides the default report path.

## Real source inspection

The adapter was written after inspecting the real `CSD Generation (Hourly) - 2026-06.zip` archive from the [AESO public Box folder](https://aeso.box.com/s/qofgn9axnnw6uq3ip1goiq2ngb11txe5/folder/196178549071). The archive contains one 18.69 MB CSV with this exact ordered header:

```text
Date (MST),Date (MPT),Asset Short Name,Asset Name,Asset Grouping,Volume,Maximum Capability,System Capability,Fuel Type,Sub Fuel Type,Planning Area,Region
```

The small file in `data/sample/` preserves that observed header and a few representative public rows. The full source archive is deliberately not committed.

## Flow and separation of concerns

```text
CSV or one-CSV ZIP
  -> raw reader + SHA-256
  -> source-hash idempotency check
  -> AESO CSD hourly adapter
  -> generic named validators
  -> reject-incoming cross-batch overlap check
  -> valid / rejected split
  -> atomic DuckDB write
  -> JSON validation report
```

- `aeso_pipeline/readers.py` handles containers and raw CSV rows.
- `aeso_pipeline/adapter.py` is the only component that knows AESO's raw field names, MST timestamp meaning, and adapter schema version. `GenerationAdapter` defines the extension interface.
- `aeso_pipeline/validation.py` contains small dataset-independent validation functions and stable reason codes.
- `aeso_pipeline/pipeline.py` applies deterministic validation order and quarantine behavior.
- `aeso_pipeline/storage.py` owns the DuckDB schema and transactional bulk persistence.
- `aeso_pipeline/run.py` provides the CLI and constructs complete provenance metadata.

A future load, price, intertie, or outage adapter can implement the same adapter interface without rewriting the validators or persistence layer.

## Raw-to-clean mapping

| Observed raw column | Clean column | Rule |
|---|---|---|
| `Date (MST)` | `timestamp` | Parse exact `YYYY-MM-DD HH:MM:SS` as fixed UTC-07:00, then store the absolute instant as `TIMESTAMPTZ` |
| `Asset Short Name` | `asset_id` | Trim surrounding whitespace; blank is rejected |
| `Asset Name` | `asset_name` | Trim surrounding whitespace; blank becomes `NULL` |
| `Volume` | `generation_mw` | Parse as finite `DOUBLE`; text, blank, NaN, and infinity are rejected |
| input filename | `source_file` | Supplied by the CLI |
| `--source-url` | `source_url` | Defaults to the official hourly Box folder |
| pipeline clock | `ingested_at` | Offset-aware UTC batch timestamp |
| adapter | `source_interval` | Literal `1hour` |

`Date (MST)` is used instead of the naive `Date (MPT)` value because MST is a fixed offset. This avoids daylight-saving ambiguity while preserving the source's stated time basis. DuckDB stores an absolute instant; display timezone depends on the DuckDB session. Run `SET TimeZone = 'UTC';` when UTC rendering is desired.

The other source columns are intentionally outside the narrow clean target. A rejected record retains all raw fields as JSON so it can be diagnosed without reopening the source.

## DuckDB tables

### `clean_generation`

| Column | DuckDB type |
|---|---|
| `timestamp` | `TIMESTAMPTZ` |
| `asset_id` | `VARCHAR` |
| `asset_name` | `VARCHAR` |
| `generation_mw` | `DOUBLE` |
| `source_file` | `VARCHAR` |
| `source_url` | `VARCHAR` |
| `ingested_at` | `TIMESTAMPTZ` |
| `source_interval` | `VARCHAR` |

### `rejected_rows`

Stores `batch_id`, source line, best-effort raw timestamp/asset/name/generation values, the full raw record as JSON text, one `reason_code`, explanatory detail, source metadata, and ingestion time. Nothing invalid is silently discarded.

### `provenance`

One row per committed batch for which all required metadata was supplied: `batch_id`, filename, URL, retrieval and ingestion timestamps, input/clean/rejected counts, interval, SHA-256, ZIP member name, final status, `adapter_schema_version`, and `overlap_policy`.

`loaded` means no rejects, `partial` means clean and rejected records both exist, and `rejected` means no record was accepted. Missing provenance is represented in `rejected_rows`; deliberately, no incomplete provenance row is fabricated. `pipeline_schema` records the local database schema version.

## Repeatable batch semantics

Artifact idempotency is keyed by the SHA-256 of the exact input bytes. Before parsing a known artifact, the pipeline checks `provenance`; a replay returns `status: skipped_idempotent`, the original batch information, `rows_written: 0`, and `provenance_written: false`. A unique database index provides a final enforcement boundary, so renaming or copying an identical file does not bypass idempotency.

Different artifacts can still contain the same `(asset_id, timestamp)`. The explicit policy is `reject_incoming`:

- the previously committed clean observation remains unchanged;
- each incoming overlap is quarantined with `CROSS_BATCH_OVERLAP`;
- non-overlapping rows in the same incoming batch continue to `clean_generation`;
- the policy and adapter schema version are recorded in provenance and the JSON report.

The CLI exposes `--overlap-policy reject_incoming`. It is currently the only supported policy so that corrected/revised AESO publications cannot overwrite history without a separately designed correction workflow.

## Deterministic validation

| Reason code | Trigger | Handling |
|---|---|---|
| `MISSING_TIMESTAMP` | Timestamp is null, empty, or whitespace | Quarantine row |
| `INVALID_TIMESTAMP` | Nonblank timestamp fails the adapter parser | Quarantine row |
| `MISSING_ASSET_ID` | Unit identifier is null, empty, or whitespace | Quarantine row |
| `NON_NUMERIC_GENERATION` | Volume is blank, nonnumeric, NaN, or infinite | Quarantine row |
| `DUPLICATE_RECORD` | Repeated `(asset_id, timestamp)` in one input | Keep first; quarantine later occurrence |
| `CROSS_BATCH_OVERLAP` | A different artifact contains a key already in `clean_generation` | Keep existing; quarantine incoming row |
| `WRONG_COLUMN_STRUCTURE` | Header count/order differs, or a row has the wrong field count | Quarantine row or reject structurally invalid batch |
| `EMPTY_FILE` | The file has no header at all | Reject batch and still record provenance |
| `OTHER_MALFORMED_ROW` | CSV/container/adapter exception not covered above | Quarantine with exception type and message |
| `MISSING_PROVENANCE_METADATA` | Filename, URL, retrieval time, or interval is absent | Reject batch; do not write an incomplete provenance row |

For a structurally valid row, precedence is timestamp missing, timestamp invalid, asset missing, generation invalid, within-batch duplicate, then cross-batch overlap. Exact-artifact idempotency is evaluated before row validation. One stable reason is assigned per rejected row, so aggregate reports do not change according to incidental validator ordering.

Negative finite generation is accepted. It can be meaningful operationally, and imposing a physical range would exceed the documented validation scope.

## Tests

Run:

```powershell
.\.venv\Scripts\python.exe -m pytest
```

The suite has one focused test for every required failure type, plus integration tests for a fully valid batch, a mixed-quality split, exact-artifact replay, cross-batch overlap, safe legacy migration, migration refusal on ambiguous legacy data, timezone normalization, quoted comma-containing values, and direct ZIP ingestion. Synthetic fixtures trigger the failures independently; correctness does not depend on the real month being dirty.

GitHub Actions runs the full suite from a clean checkout on Python 3.11 and 3.13 for every push and pull request. See `.github/workflows/ci.yml`.

## Upstream schema compatibility

The current exact source contract is identified as `aeso-csd-hourly-v1`. Additive, removed, renamed, reordered, or semantically changed upstream fields fail closed rather than being guessed. The versioning procedure, compatibility matrix, and local DuckDB migration guarantees are documented in [SCHEMA_COMPATIBILITY.md](SCHEMA_COMPATIBILITY.md).

## Data-quality assumption

> The pipeline treats the generation values published in the AESO CSD dataset as the source-of-record observations for ingestion and validation. It does not attempt to reconcile them against settlement-meter data.

AESO describes CSD data as generally representative of unit generation but distinct from, and lower quality than, settlement-meter data. For this bounded component, the defensible checks are structure, type, provenance, uniqueness, and internal consistency. Claiming that every numeric observation is physically correct would require a different authoritative dataset and reconciliation rules, both out of scope. See [ASSUMPTIONS.md](ASSUMPTIONS.md).

## Useful queries

```sql
SELECT reason_code, count(*)
FROM rejected_rows
GROUP BY reason_code
ORDER BY reason_code;

SELECT source_file, retrieved_at, row_count, clean_row_count,
       rejected_row_count, source_sha256, adapter_schema_version,
       overlap_policy, status
FROM provenance
ORDER BY ingested_at DESC;

SELECT asset_id, timestamp, count(*) AS occurrences
FROM clean_generation
GROUP BY asset_id, timestamp
HAVING count(*) > 1;
```

## Known boundaries

- The bounded file is normalized in memory before bulk persistence. For many months or five-minute data, process chunks while retaining the same validation functions.
- `reject_incoming` preserves history but does not implement a corrected-publication replacement workflow; that requires explicit domain authorization and version semantics.
- Remote download, credential handling, scheduling, automatic schema adaptation, physical plausibility thresholds, and settlement reconciliation are intentionally not included.
- The adapter fails closed on header changes. That makes source drift visible instead of silently mis-mapping columns.
