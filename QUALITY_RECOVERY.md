# Wallet quality recovery — stage 1

Implemented locally on 2026-10-29. Not deployed; no paid history backfill,
external storage, copyability approval or trading-policy change is performed.

Production candidate prepared on 2026-11-02 from `b5da49c`, excluding unrelated
copy-bot and replication-study changes. Validation: 897 tests passed and the
JavaScript syntax check passed. Deployment verification is recorded separately.

## Storage and acquisition

- `EquityHistoryCache` keeps canonical points and immutable observations.
  Conflicting account/PnL values, changed PnL origins and extensions without
  a shared anchor are quarantined instead of replacing the accepted past.
  An empty update retains old observations but cannot verify a current curve.
- Calculation-window pruning is a SELECT filter, not deletion of the archive.
  An earlier historical request restarts the cursor without erasing observed
  fills, funding or retention evidence.
- Existing live `userFills` requests use unaggregated executions. Their raw
  fills, the window response and available TWAP slices are deduplicated and
  archived before their data is simplified for the UI/cache. No new live API
  request is added and live observations do not claim interval completeness.
- Archival stops below `WALLET_RAW_ARCHIVE_MIN_FREE_BYTES` (default 2 GiB).
  The error is surfaced in dashboard/runtime health and the monitor. This is
  a stop-and-alert reserve, not a substitute for off-host retention/backup.
  `WALLET_RAW_ARCHIVE_ENABLED` defaults to 1; both settings enter the manifest.
- The background history owner acquires `userNonFundingLedgerUpdates` into
  the checkpointed `ledger` stream. Short pages are not completion: it walks
  to an empty response, retains inclusive boundaries and refuses saturated
  timestamps. The completion watermark never exceeds the requested end.
- Ledger event timestamps only guard potentially ambiguous intervals. They
  do not invent signed flows, historical account modes or capital valuations.
  In particular, net-zero transfers plus PnL are not an exact daily return.

## Independent evidence

- Equity verification reports curve/data reasons independently of trade
  episode completeness. Its separate PF verdict still requires complete
  episode and funding evidence. Missing funding or cashflow acquisition and
  conflicting source observations do not verify a curve.
- Source conflicts invalidate the exact curve metrics while preserving
  previously observed drawdown and every existing risk gate.
- Quarantined capital cannot establish a new Largest loser percentage.
  A previously measured value may be retained within its original 180D
  evidence window, without renewing its date or declaring coverage complete.
- Explicit 30D and 90D equity metrics have their own coverage checks. They
  are labelled by window and are not substituted into a 180D rank. Without
  ledger coverage the short-window result remains preliminary. No short
  window creates Elite or a trade admission.

## Quality versus data versus risk

- The existing `available_pf_trend_v1` arithmetic remains reproducible for
  frozen studies and old reports. Its conservative incomplete-data result
  is a diagnostic, not a verified quality rank.
- `available_pf_trend_evidence_v2` requires verified PF, verified contiguous
  window coverage and the existing 40-episode/10-loss minimum before issuing
  a point quality score. Unknown PF is `InsufficientData`, not `Cold`.
  A genuinely measured zero PF can still be Cold. Risk always wins.
- The common `qualityAssessment` payload separates quality, data completeness,
  risk and copyability, preserving the existing admission decision. Web and
  Telegram wallet-rank output consume that payload; the legacy group
  composite is explicitly a different measure.
- Missing 30D PF remains null/unknown, not a measured PF=0 or performance fail.
  This can change a legacy diagnostic composite, but cannot make an unknown
  performance/copyability gate pass. Risk thresholds are unchanged.

## Tests and operational rollout

Regression cases cover changed PnL origins, conflicting capital, no anchors,
empty responses, longer backfills, opposing flows, deposit/loss ambiguity,
100% capital loss, independent metric proofs, zero versus unknown PF,
partial-score diagnostics, ledger pagination/watermarks and preserved vetoes.

Before production rollout: verify the diff against the deployed revision,
take a backup of the wallet databases outside this disk, deploy the new
modules with the server and monitor, and inspect one full refresh plus one
live cycle. The new code/source/config fingerprint must not silently resume
an older frozen paper baseline. Migrations are additive; old tables remain.
Rolling back the executable code can restore its former destructive cache
behaviour, so archive copies must be retained before any rollback.

## Remaining stages — not claimed complete

1. Select an off-host archive/backup destination and a storage/transfer budget.
   This implementation does not create a bucket, credentials or paid jobs.
2. Audit a sample of missing historical intervals against an archive/indexer,
   then backfill only supported rows. Older equity cannot be recovered from
   fills alone without consistent prices, flows, funding and account mode.
3. Reconstruct event-time capital around external flows where the evidence
   exists. Otherwise keep the interval estimated/unknown, not verified.
4. Specify and version a separate copyability-discovery study: fresh entries,
   quoted executable prices, delay, fees/funding, exits and independent
   episodes. It may collect research observations without prior copyability,
   but must never issue live signals or change the existing study/registry.
   That new experiment is not enabled by this patch.
