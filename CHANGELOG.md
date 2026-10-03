# Changelog

All notable changes to this project are documented here.

## [0.3.0] - 2026-10-03

### Added

- Read-only `aeso-inspect` CLI with stable JSON health and scale metrics.
- Installable `aeso-ingest` and `aeso-inspect` console commands.
- Recruiter-friendly project overview, verified full-month results, architecture diagram, and fast demo path.

### Changed

- CI now validates editable package installation and both command-line entry points.
- Setuptools package discovery is explicit so local data directories cannot be mistaken for Python packages.

### Security

- Upgraded DuckDB to 1.4.2 to include the upstream encryption implementation security fixes.
- Upgraded pytest to 9.0.3 to address the upstream temporary-file cleanup advisory.

## [0.2.0] - 2026-09-09

### Added

- SHA-256 source-artifact idempotency.
- Explicit `reject_incoming` cross-batch overlap policy.
- Versioned AESO source contract and DuckDB schema migration safeguards.
- Python 3.11 and 3.13 GitHub Actions matrix.

## [0.1.0] - 2026-09-08

### Added

- Initial CSV/ZIP ingestion, normalization, deterministic validation, quarantine, provenance, DuckDB persistence, and JSON reporting.
