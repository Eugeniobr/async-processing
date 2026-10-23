# Load-validation evidence package — 2026-10-23

`load-validation-20261023.tar.gz` is the reviewed, portable evidence package for the
corrected local k6 runs documented in
[../load-report-2026-10-23.md](../load-report-2026-10-23.md).

- Archive SHA-256: `44ff30478e7080933ae5f4b0c03fcfb52a4f609d0dca8c85ad8383d57914ed9d`
- Archive verifier: `load-validation-20261023.tar.gz.sha256`
- Member verifier: `load-validation-20261023.manifest.sha256`, also embedded as
  `MANIFEST.sha256` in the archive.
- Source originals retained locally: `.load-evidence/20261023-load-185755/` (ignored by
  Git). The archive is a curated subset of the two corrected runs; it excludes preliminary
  generator-defect runs and local diagnostic logs.

The package contains the two raw k6 streams, summaries, analysis and exact-ID reconciliation,
queue/policy snapshots, final drain result, cleanup status, and environment facts. A secret
marker scan was run before archiving; no credentials, tokens, or personal data were included.
The identifiers are synthetic test IDs.

Verify it after checkout:

```bash
sha256sum -c docs/evidence/load-validation-20261023.tar.gz.sha256
tar -xOf docs/evidence/load-validation-20261023.tar.gz \
  load-validation-20261023/MANIFEST.sha256 > /tmp/load-validation-20261023.manifest.sha256
mkdir -p /tmp/load-validation-20261023
tar -xzf docs/evidence/load-validation-20261023.tar.gz -C /tmp/load-validation-20261023
(cd /tmp/load-validation-20261023/load-validation-20261023 && \
  grep -v '^#' /tmp/load-validation-20261023.manifest.sha256 | sha256sum -c -)
```

The reported HTTP latency measures acceptance only. Completion timing is separately measured
until the transaction becomes visible through the persisted-data query. All load events used
USD with rate `1`; the deterministic USD branch made no external exchange-rate request.
