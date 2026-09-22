from __future__ import annotations

import json
import math
import sqlite3
from datetime import datetime, timezone
from contextlib import closing
from pathlib import Path
from typing import Any, Iterable

from paper_portfolio import observed_drawdown, paired_portfolio_comparison


SCHEMA_VERSION = 6


def _json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def _addresses(record: dict[str, Any]) -> list[str]:
    candidates: list[Any] = []
    for key in ("initialWalletAddresses", "walletAddresses", "freshWalletAddresses"):
        value = record.get(key)
        if isinstance(value, list) and value:
            candidates = value
            break
    fingerprint = record.get("consensusFingerprint")
    if not candidates and isinstance(fingerprint, dict) and isinstance(fingerprint.get("walletAddresses"), list):
        candidates = fingerprint["walletAddresses"]
    return sorted({str(value).lower() for value in candidates if str(value).strip()})


def _record_status(record: dict[str, Any]) -> str:
    explicit = str(record.get("status") or "").lower()
    if explicit in {"open", "closed", "expired", "unpriced", "delivery_failed", "skipped"}:
        return explicit
    if record.get("deliveryError"):
        return "delivery_failed"
    if record.get("exitAtMs") or record.get("closedAtMs") or record.get("closedAt"):
        return "closed"
    outcomes = record.get("outcomes")
    if isinstance(outcomes, dict) and outcomes:
        return "measuring"
    return "open"


class ExecutionJournal:
    """Append-preserving SQLite journal for reproducible paper execution.

    Signal rows are upserted as their lifecycle advances; horizon outcomes are
    immutable by `(signal_id, horizon)` except for a later higher-quality mark
    replacing an earlier degraded measurement.  Evaluations use deterministic
    IDs, so a repeated monitor cycle cannot multiply the same skipped setup.
    """

    def __init__(self, path: Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=15)
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA synchronous=FULL")
        connection.execute("PRAGMA busy_timeout=15000")
        self._ensure_schema(connection)
        return connection

    def load_stream(
        self, stream: str, *, since_ms: int = 0, include_unfinished: bool = False
    ) -> dict[str, dict[str, Any]]:
        """Restore measurement-in-flight records without growing alerts.json."""
        with closing(self._connect()) as connection:
            rows = connection.execute(
                "SELECT signal_id, source_json FROM signals "
                "WHERE stream = ? AND (detected_at_ms >= ? OR "
                "(? = 1 AND (status NOT IN ('closed', 'skipped', 'expired') "
                "OR execution_status = 'closed_funding_unverified')))",
                (stream, since_ms, 1 if include_unfinished else 0),
            ).fetchall()
        records: dict[str, dict[str, Any]] = {}
        for signal_id, source_json in rows:
            try:
                record = json.loads(source_json)
            except (TypeError, ValueError):
                continue
            if isinstance(record, dict):
                records[str(signal_id).split(":", 1)[-1]] = record
        return records

    def freeze_status(
        self, *, config_hash: str, alert_config_hash: str,
        tracked_wallets: list[str], now_ms: int,
    ) -> dict[str, Any]:
        """Compare this cycle with the first prospective, persisted baseline."""
        with closing(self._connect()) as connection, connection:
            baseline = connection.execute(
                """SELECT config_hash, alert_config_hash, tracked_wallets_json,
                observed_at_ms
                FROM audit_cycles ORDER BY observed_at_ms LIMIT 1"""
            ).fetchone()
            paused = connection.execute(
                "SELECT value FROM journal_meta WHERE key = 'experiment_paused'"
            ).fetchone()
            if baseline is None:
                return {
                    "status": "baseline", "allowNewEntries": False,
                    "reasons": ["baseline_exclusion"], "baselineAtMs": now_ms,
                }
            reasons = []
            if config_hash != baseline[0]:
                reasons.append("rules_changed")
            if alert_config_hash != baseline[1]:
                reasons.append("alert_config_changed")
            if _json(sorted(tracked_wallets)) != baseline[2]:
                reasons.append("wallet_universe_changed")
            if reasons:
                connection.execute(
                    "INSERT OR REPLACE INTO journal_meta(key, value) VALUES('experiment_paused', '1')"
                )
                connection.execute(
                    "INSERT OR REPLACE INTO journal_meta(key, value) VALUES('pause_reasons', ?)",
                    (_json(reasons),),
                )
            if paused and paused[0] == "1" and not reasons:
                stored = connection.execute(
                    "SELECT value FROM journal_meta WHERE key = 'pause_reasons'"
                ).fetchone()
                reasons = json.loads(stored[0]) if stored else ["previous_source_change"]
            return {
                "status": "paused" if reasons else "consistent",
                "allowNewEntries": not reasons,
                "reasons": reasons,
                "baselineAtMs": int(baseline[3]),
            }

    @staticmethod
    def _ensure_schema(connection: sqlite3.Connection) -> None:
        connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS journal_meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS rule_manifests (
                config_hash TEXT PRIMARY KEY,
                rule_version TEXT NOT NULL,
                manifest_json TEXT NOT NULL,
                first_seen_at_ms INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS audit_cycles (
                observed_at_ms INTEGER PRIMARY KEY,
                config_hash TEXT NOT NULL,
                alert_config_hash TEXT NOT NULL,
                tracked_wallets_json TEXT NOT NULL,
                observed_wallets_json TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS paper_oracle_samples (
                coin TEXT NOT NULL,
                observed_at_ms INTEGER NOT NULL,
                oracle_price REAL NOT NULL,
                PRIMARY KEY (coin, observed_at_ms)
            );
            CREATE TABLE IF NOT EXISTS paper_funding_rates (
                coin TEXT NOT NULL,
                funding_at_ms INTEGER NOT NULL,
                funding_rate REAL NOT NULL,
                PRIMARY KEY (coin, funding_at_ms)
            );
            CREATE TABLE IF NOT EXISTS paper_portfolio_snapshots (
                experiment_arm TEXT NOT NULL,
                observed_at_ms INTEGER NOT NULL,
                complete INTEGER NOT NULL,
                equity_usd REAL,
                source_json TEXT NOT NULL,
                PRIMARY KEY (experiment_arm, observed_at_ms)
            );
            CREATE TABLE IF NOT EXISTS signals (
                signal_id TEXT PRIMARY KEY,
                stream TEXT NOT NULL,
                experiment_arm TEXT,
                signal_key TEXT,
                rule_version TEXT NOT NULL,
                config_hash TEXT NOT NULL,
                coin TEXT,
                market_coin TEXT,
                side TEXT,
                detected_at_ms INTEGER,
                price_available_at_ms INTEGER,
                executable_entry_at_ms INTEGER,
                entry_latency_ms INTEGER,
                reference_price REAL,
                entry_price REAL,
                wallet_vwap REAL,
                execution_status TEXT,
                status TEXT NOT NULL,
                initial_wallets_json TEXT NOT NULL,
                eligible_wallets_json TEXT NOT NULL,
                rejection_reasons_json TEXT NOT NULL,
                cost_model_json TEXT NOT NULL,
                exit_at_ms INTEGER,
                exit_price REAL,
                exit_reason TEXT,
                delivered INTEGER,
                delivery_error TEXT,
                source_json TEXT NOT NULL,
                created_at_ms INTEGER NOT NULL,
                updated_at_ms INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS outcomes (
                signal_id TEXT NOT NULL,
                horizon TEXT NOT NULL,
                measured_at_ms INTEGER,
                mark_price REAL,
                gross_return_pct REAL,
                net_return_pct REAL,
                double_cost_net_return_pct REAL,
                delayed_entry_net_return_pct REAL,
                price_source TEXT,
                measurement_basis TEXT NOT NULL DEFAULT 'mark_price_proxy',
                degraded INTEGER NOT NULL DEFAULT 0,
                source_json TEXT NOT NULL,
                PRIMARY KEY (signal_id, horizon),
                FOREIGN KEY (signal_id) REFERENCES signals(signal_id)
            );
            CREATE TABLE IF NOT EXISTS evaluations (
                evaluation_id TEXT PRIMARY KEY,
                evaluated_at_ms INTEGER NOT NULL,
                rule_version TEXT NOT NULL,
                config_hash TEXT NOT NULL,
                experiment_arm TEXT NOT NULL,
                signal_key TEXT NOT NULL,
                coin TEXT,
                side TEXT,
                eligible INTEGER NOT NULL,
                wallet_addresses_json TEXT NOT NULL,
                reasons_json TEXT NOT NULL,
                inputs_json TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_signals_detected ON signals(detected_at_ms);
            CREATE INDEX IF NOT EXISTS idx_signals_arm ON signals(experiment_arm, detected_at_ms);
            CREATE INDEX IF NOT EXISTS idx_evaluations_time ON evaluations(evaluated_at_ms);
            """
        )
        # Databases created by the earlier staging revision must keep their
        # observations while gaining explicit model-vs-execution labels.
        signal_columns = {
            row[1] for row in connection.execute("PRAGMA table_info(signals)")
        }
        if "execution_status" not in signal_columns:
            connection.execute("ALTER TABLE signals ADD COLUMN execution_status TEXT")
        outcome_columns = {
            row[1] for row in connection.execute("PRAGMA table_info(outcomes)")
        }
        if "measurement_basis" not in outcome_columns:
            connection.execute(
                "ALTER TABLE outcomes ADD COLUMN measurement_basis TEXT NOT NULL DEFAULT 'mark_price_proxy'"
            )
        connection.execute(
            "INSERT OR REPLACE INTO journal_meta(key, value) VALUES('schema_version', ?)",
            (str(SCHEMA_VERSION),),
        )

    def sync(
        self,
        *,
        manifest: dict[str, Any],
        config_hash: str,
        streams: dict[str, dict[str, Any]],
        evaluations: Iterable[dict[str, Any]] = (),
        context: dict[str, Any] | None = None,
        oracle_samples: Iterable[dict[str, Any]] = (),
        portfolio_snapshots: Iterable[dict[str, Any]] = (),
        now_ms: int,
    ) -> None:
        rule_version = str(manifest.get("ruleVersion") or "unknown")
        cost_model = manifest.get("costModel") if isinstance(manifest.get("costModel"), dict) else {}
        with closing(self._connect()) as connection, connection:
            connection.execute(
                """
                INSERT OR IGNORE INTO rule_manifests
                (config_hash, rule_version, manifest_json, first_seen_at_ms)
                VALUES (?, ?, ?, ?)
                """,
                (config_hash, rule_version, _json(manifest), now_ms),
            )
            if context is not None:
                connection.execute(
                    """INSERT OR IGNORE INTO audit_cycles
                    (observed_at_ms, config_hash, alert_config_hash,
                     tracked_wallets_json, observed_wallets_json)
                    VALUES (?, ?, ?, ?, ?)""",
                    (
                        now_ms, config_hash,
                        str(context.get("alertConfigHash") or "unknown"),
                        _json(context.get("trackedWalletAddresses") or []),
                        _json(context.get("observedWalletAddresses") or []),
                    ),
                )
            for sample in oracle_samples:
                if not isinstance(sample, dict):
                    continue
                coin = str(sample.get("coin") or "")
                observed_at = sample.get("observedAtMs")
                price = sample.get("oraclePrice")
                if (
                    not coin or not isinstance(observed_at, int)
                    or isinstance(observed_at, bool)
                    or not isinstance(price, (int, float))
                    or isinstance(price, bool)
                    or not math.isfinite(float(price)) or price <= 0
                ):
                    continue
                connection.execute(
                    """INSERT OR IGNORE INTO paper_oracle_samples
                    (coin, observed_at_ms, oracle_price) VALUES (?, ?, ?)""",
                    (coin, observed_at, float(price)),
                )
            for snapshot in portfolio_snapshots:
                if not isinstance(snapshot, dict):
                    continue
                arm = str(snapshot.get("arm") or "")
                observed_at = snapshot.get("observedAtMs")
                equity = snapshot.get("equityUsd")
                if not arm or not isinstance(observed_at, int) or isinstance(observed_at, bool):
                    continue
                if equity is not None and (
                    not isinstance(equity, (int, float)) or not math.isfinite(equity)
                ):
                    continue
                connection.execute(
                    """INSERT OR IGNORE INTO paper_portfolio_snapshots
                    (experiment_arm, observed_at_ms, complete, equity_usd, source_json)
                    VALUES (?, ?, ?, ?, ?)""",
                    (arm, observed_at, 1 if snapshot.get("complete") else 0,
                     equity, _json(snapshot)),
                )
            for stream, records in streams.items():
                if not isinstance(records, dict):
                    continue
                for key, value in records.items():
                    if not isinstance(value, dict):
                        continue
                    self._upsert_signal(
                        connection,
                        stream=stream,
                        key=str(key),
                        record=value,
                        # Never relabel a historical record with today's
                        # policy. The old stream predates version stamping.
                        rule_version=str(value.get("ruleVersion") or "legacy_unversioned"),
                        config_hash=str(value.get("configHash") or "unknown"),
                        cost_model=value.get("costModel") if isinstance(value.get("costModel"), dict) else cost_model,
                        now_ms=now_ms,
                    )
            for evaluation in evaluations:
                if not isinstance(evaluation, dict):
                    continue
                connection.execute(
                    """
                    INSERT OR IGNORE INTO evaluations
                    (evaluation_id, evaluated_at_ms, rule_version, config_hash,
                     experiment_arm, signal_key, coin, side, eligible,
                     wallet_addresses_json, reasons_json, inputs_json)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        str(evaluation.get("evaluationId")),
                        int(evaluation.get("evaluatedAtMs") or now_ms),
                        rule_version,
                        config_hash,
                        str(evaluation.get("experimentArm") or "unknown"),
                        str(evaluation.get("signalKey") or ""),
                        str(evaluation.get("coin") or ""),
                        str(evaluation.get("side") or ""),
                        1 if evaluation.get("eligible") else 0,
                        _json(evaluation.get("walletAddresses") or []),
                        _json(evaluation.get("reasons") or []),
                        _json(evaluation.get("inputs") or {}),
                    ),
                )

    def load_oracle_samples(
        self, coin: str, *, start_ms: int, end_ms: int
    ) -> list[dict[str, Any]]:
        with closing(self._connect()) as connection:
            rows = connection.execute(
                """SELECT observed_at_ms, oracle_price FROM paper_oracle_samples
                WHERE coin = ? AND observed_at_ms BETWEEN ? AND ?
                ORDER BY observed_at_ms""",
                (coin, start_ms, end_ms),
            ).fetchall()
        return [
            {"coin": coin, "observedAtMs": int(at), "oraclePrice": float(price)}
            for at, price in rows
        ]

    def load_funding_rates(
        self, coin: str, *, start_ms: int, end_ms: int
    ) -> list[dict[str, Any]]:
        with closing(self._connect()) as connection:
            rows = connection.execute(
                """SELECT funding_at_ms, funding_rate FROM paper_funding_rates
                WHERE coin = ? AND funding_at_ms BETWEEN ? AND ?
                ORDER BY funding_at_ms""",
                (coin, start_ms, end_ms),
            ).fetchall()
        return [
            {"coin": coin, "time": int(at), "fundingRate": float(rate)}
            for at, rate in rows
        ]

    def cache_funding_rates(self, coin: str, rows: Iterable[dict[str, Any]]) -> None:
        """Persist only validated official rows; missing hours are never inferred."""
        validated = []
        for row in rows:
            if not isinstance(row, dict) or str(row.get("coin") or "") != coin:
                raise ValueError("invalid funding coin")
            at = row.get("time")
            try:
                rate = float(row["fundingRate"])
            except (KeyError, TypeError, ValueError, OverflowError) as exc:
                raise ValueError("invalid funding rate") from exc
            if not isinstance(at, int) or isinstance(at, bool) or at <= 0 or not math.isfinite(rate):
                raise ValueError("invalid funding row")
            validated.append((coin, at, rate))
        with closing(self._connect()) as connection, connection:
            connection.executemany(
                """INSERT OR REPLACE INTO paper_funding_rates
                (coin, funding_at_ms, funding_rate) VALUES (?, ?, ?)""",
                validated,
            )

    def load_portfolio_snapshots(self, arm: str) -> list[dict[str, Any]]:
        with closing(self._connect()) as connection:
            rows = connection.execute(
                """SELECT source_json FROM paper_portfolio_snapshots
                WHERE experiment_arm = ? ORDER BY observed_at_ms""",
                (arm,),
            ).fetchall()
        return [json.loads(row[0]) for row in rows]

    def paper_report(self) -> dict[str, Any]:
        """Descriptive audit; never converts incomplete rows into zero PnL."""
        with closing(self._connect()) as connection:
            signal_rows = connection.execute(
                "SELECT source_json FROM signals WHERE stream = 'paper'"
            ).fetchall()
            evaluation_rows = connection.execute(
                """SELECT experiment_arm, eligible, reasons_json FROM evaluations"""
            ).fetchall()
            cycles = connection.execute(
                """SELECT observed_at_ms, config_hash, alert_config_hash,
                tracked_wallets_json FROM audit_cycles ORDER BY observed_at_ms"""
            ).fetchall()
            pause_row = connection.execute(
                "SELECT value FROM journal_meta WHERE key = 'experiment_paused'"
            ).fetchone()
        arms: dict[str, dict[str, Any]] = {
            arm: {
                "observations": 0, "open": 0, "skipped": 0,
                "closedFundingUnverified": 0, "closedModeledNet": 0,
                "modeledNetUsd": 0.0, "doubleCostNetUsd": 0.0,
                "delayedEntryCompleted": 0, "delayedEntryNetUsd": 0.0,
                "delayedEntryNoPosition": 0, "delayedEntrySkippedOrUnpriced": 0,
                "decisionDays": set(), "assets": set(),
                "evaluations": 0, "eligibleEvaluations": 0,
                "skipReasons": {},
            }
            for arm in ("ranked_consensus", "consensus_unranked", "fresh_entry")
        }
        positive_by_asset: dict[str, dict[str, float]] = {arm: {} for arm in arms}
        positive_by_wallet: dict[str, dict[str, float]] = {arm: {} for arm in arms}
        for (source_json,) in signal_rows:
            try:
                row = json.loads(source_json)
            except (TypeError, ValueError):
                continue
            if not isinstance(row, dict):
                continue
            arm = str(row.get("experimentArm") or "")
            if arm not in arms:
                continue
            bucket = arms[arm]
            bucket["observations"] += 1
            status = str(row.get("executionStatus") or "")
            if row.get("status") == "skipped":
                bucket["skipped"] += 1
                reason = str(row.get("entrySkipReason") or "unspecified")
                bucket["skipReasons"][reason] = bucket["skipReasons"].get(reason, 0) + 1
            elif status == "closed_funding_unverified":
                bucket["closedFundingUnverified"] += 1
            elif status == "closed_modeled_net":
                result = row.get("executionResult")
                stress = row.get("doubleCostResult")
                if isinstance(result, dict) and result.get("complete") and isinstance(stress, dict) and stress.get("complete"):
                    net, stress_net = result.get("netUsd"), stress.get("netUsd")
                    if all(isinstance(value, (int, float)) and math.isfinite(value) for value in (net, stress_net)):
                        bucket["closedModeledNet"] += 1
                        bucket["modeledNetUsd"] += net
                        bucket["doubleCostNetUsd"] += stress_net
                        if net > 0:
                            asset = str(row.get("marketCoin") or row.get("coin") or "unknown")
                            positive_by_asset[arm][asset] = positive_by_asset[arm].get(asset, 0.0) + net
                            wallets = _addresses(row)
                            if wallets:
                                share = net / len(wallets)
                                for wallet in wallets:
                                    positive_by_wallet[arm][wallet] = positive_by_wallet[arm].get(wallet, 0.0) + share
                        delayed = row.get("delayedEntryResult")
                        delayed_net = delayed.get("netUsd") if isinstance(delayed, dict) and delayed.get("complete") else None
                        if isinstance(delayed_net, (int, float)) and math.isfinite(delayed_net):
                            bucket["delayedEntryCompleted"] += 1
                            bucket["delayedEntryNetUsd"] += delayed_net
                        elif row.get("delayedEntryStatus") == "closed_before_delay":
                            bucket["delayedEntryNoPosition"] += 1
                        else:
                            bucket["delayedEntrySkippedOrUnpriced"] += 1
                    else:
                        bucket["closedFundingUnverified"] += 1
                else:
                    bucket["closedFundingUnverified"] += 1
            elif row.get("status") == "closed":
                bucket["closedFundingUnverified"] += 1
            else:
                bucket["open"] += 1
            started_at = row.get("startedAt")
            if isinstance(started_at, (int, float)) and started_at > 0:
                day = datetime.fromtimestamp(started_at / 1000, tz=timezone.utc).date().isoformat()
                bucket["decisionDays"].add(day)
            if row.get("coin"):
                bucket["assets"].add(str(row["coin"]))
        for arm, eligible, reasons_json in evaluation_rows:
            if arm not in arms:
                continue
            bucket = arms[arm]
            bucket["evaluations"] += 1
            if eligible:
                bucket["eligibleEvaluations"] += 1
            else:
                try:
                    reasons = json.loads(reasons_json)
                except (TypeError, ValueError):
                    reasons = []
                for reason in reasons if isinstance(reasons, list) else []:
                    name = str(reason)
                    bucket["skipReasons"][name] = bucket["skipReasons"].get(name, 0) + 1
        for bucket in arms.values():
            bucket["decisionDays"] = len(bucket["decisionDays"])
            bucket["assets"] = len(bucket["assets"])
            bucket["modeledNetUsd"] = round(bucket["modeledNetUsd"], 6)
            bucket["doubleCostNetUsd"] = round(bucket["doubleCostNetUsd"], 6)
            bucket["delayedEntryNetUsd"] = round(bucket["delayedEntryNetUsd"], 6)
        portfolio_history = {
            arm: self.load_portfolio_snapshots(arm) for arm in arms
        }
        stress_history = {
            arm: {
                scenario: self.load_portfolio_snapshots(f"{arm}:{scenario}")
                for scenario in ("double_cost", "delayed_entry")
            }
            for arm in arms
        }
        for arm, bucket in arms.items():
            snapshots = portfolio_history[arm]
            bucket["drawdown"] = observed_drawdown(snapshots)
            bucket["portfolioSnapshotCount"] = len(snapshots)
            bucket["incompletePortfolioSnapshots"] = sum(
                1 for snapshot in snapshots if not snapshot.get("complete")
            )
            last = snapshots[-1] if snapshots else None
            bucket["lastPortfolioEquityUsd"] = (
                last.get("equityUsd") if isinstance(last, dict) and last.get("complete") else None
            )
            for scenario, scenario_rows in stress_history[arm].items():
                prefix = "doubleCost" if scenario == "double_cost" else "delayedEntry"
                bucket[f"{prefix}PortfolioDrawdown"] = observed_drawdown(scenario_rows)
                bucket[f"{prefix}PortfolioSnapshotCount"] = len(scenario_rows)
                bucket[f"{prefix}IncompletePortfolioSnapshots"] = sum(
                    1 for row in scenario_rows if not row.get("complete")
                )
                latest = scenario_rows[-1] if scenario_rows else None
                bucket[f"last{prefix[0].upper()}{prefix[1:]}PortfolioEquityUsd"] = (
                    latest.get("equityUsd")
                    if isinstance(latest, dict) and latest.get("complete") else None
                )
            asset_total = sum(positive_by_asset[arm].values())
            wallet_total = sum(positive_by_wallet[arm].values())
            bucket["largestAssetPositiveContributionPct"] = (
                round(max(positive_by_asset[arm].values()) / asset_total * 100, 3)
                if asset_total > 0 else None
            )
            bucket["largestWalletPositiveContributionPct"] = (
                round(max(positive_by_wallet[arm].values()) / wallet_total * 100, 3)
                if wallet_total > 0 else None
            )
            bucket["walletContributionMethod"] = "equal_split_of_positive_trade_net_not_causal_attribution"
        baseline_at = cycles[0][0] if cycles else None
        last_at = cycles[-1][0] if cycles else None
        source_frozen = bool(cycles) and len({(row[1], row[2], row[3]) for row in cycles}) == 1
        paused = bool(pause_row and pause_row[0] == "1")
        missing = []
        if baseline_at is None or last_at is None or last_at - baseline_at < 25 * 24 * 60 * 60 * 1000:
            missing.append("prospective_25_day_checkpoint")
        if not source_frozen or paused:
            missing.append("single_unpaused_rule_and_universe_version")
        for arm, bucket in arms.items():
            if bucket["closedModeledNet"] < 30 or bucket["decisionDays"] < 20:
                missing.append(f"{arm}:minimum_closed_episodes_and_decision_days")
            if bucket["open"] or bucket["closedFundingUnverified"] or bucket["delayedEntrySkippedOrUnpriced"]:
                missing.append(f"{arm}:complete_executable_outcomes_and_delay_stress")
            if not bucket["drawdown"]["complete"] or bucket["incompletePortfolioSnapshots"]:
                missing.append(f"{arm}:continuous_mark_to_close_portfolio_equity")
            if any(
                not bucket[f"{prefix}PortfolioDrawdown"]["complete"]
                or bucket[f"{prefix}IncompletePortfolioSnapshots"]
                for prefix in ("doubleCost", "delayedEntry")
            ):
                missing.append(f"{arm}:continuous_portfolio_cost_and_delay_stress")
            if any(
                value is None or value > 50
                for value in (
                    bucket["largestAssetPositiveContributionPct"],
                    bucket["largestWalletPositiveContributionPct"],
                )
            ):
                missing.append(f"{arm}:positive_contribution_not_dominated_by_one_asset_or_wallet")
        missing.extend([
            "matched_opportunity_portfolio_comparison",
            "clustered_uncertainty_interval_for_net_return_difference",
        ])
        return {
            "baselineAtMs": baseline_at,
            "lastObservedAtMs": last_at,
            "sourceFrozen": source_frozen,
            "paused": paused,
            "arms": arms,
            "pairedPortfolioComparisons": {
                "rankedVsUnranked": paired_portfolio_comparison(
                    portfolio_history["ranked_consensus"],
                    portfolio_history["consensus_unranked"],
                ),
                "rankedVsFreshEntry": paired_portfolio_comparison(
                    portfolio_history["ranked_consensus"],
                    portfolio_history["fresh_entry"],
                ),
            },
            "pairedStressComparisons": {
                scenario: paired_portfolio_comparison(
                    stress_history["ranked_consensus"][scenario],
                    stress_history["consensus_unranked"][scenario],
                )
                for scenario in ("double_cost", "delayed_entry")
            },
            "conclusion": "insufficient_for_executable_strategy_comparison",
            "missingForDecision": missing,
        }

    def _upsert_signal(
        self,
        connection: sqlite3.Connection,
        *,
        stream: str,
        key: str,
        record: dict[str, Any],
        rule_version: str,
        config_hash: str,
        cost_model: dict[str, Any],
        now_ms: int,
    ) -> None:
        signal_id = f"{stream}:{key}"
        started_at = int(record.get("startedAt") or record.get("enteredAtMs") or 0)
        reference_price = float(record.get("entryPrice") or record.get("referencePrice") or 0.0)
        entry_price = float(record.get("executableEntryPrice") or 0.0)
        initial_wallets = _addresses(record)
        eligible_wallets = sorted(
            {
                str(value).lower()
                for value in (record.get("eligibleWalletAddresses") or [])
                if str(value).strip()
            }
        )
        rejection_reasons = record.get("rejectionReasons") or record.get("skipReasons") or []
        connection.execute(
            """
            INSERT INTO signals
            (signal_id, stream, experiment_arm, signal_key, rule_version, config_hash,
             coin, market_coin, side, detected_at_ms, price_available_at_ms,
             executable_entry_at_ms, entry_latency_ms, reference_price, entry_price, wallet_vwap,
             execution_status, status, initial_wallets_json, eligible_wallets_json,
             rejection_reasons_json, cost_model_json, exit_at_ms, exit_price,
             exit_reason, delivered, delivery_error, source_json, created_at_ms,
             updated_at_ms)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(signal_id) DO UPDATE SET
              status=excluded.status,
              exit_at_ms=COALESCE(excluded.exit_at_ms, signals.exit_at_ms),
              exit_price=COALESCE(excluded.exit_price, signals.exit_price),
              exit_reason=COALESCE(excluded.exit_reason, signals.exit_reason),
              delivered=COALESCE(excluded.delivered, signals.delivered),
              delivery_error=excluded.delivery_error,
              execution_status=COALESCE(excluded.execution_status, signals.execution_status),
              source_json=excluded.source_json,
              updated_at_ms=excluded.updated_at_ms
            """,
            (
                signal_id,
                stream,
                str(record.get("experimentArm") or ""),
                str(record.get("signalKey") or key.split(":", 1)[0]),
                rule_version,
                config_hash,
                str(record.get("coin") or ""),
                str(record.get("marketCoin") or record.get("coin") or ""),
                str(record.get("side") or ""),
                started_at or None,
                int(record.get("priceAvailableAtMs") or 0) or None,
                int(record.get("executableEntryAtMs") or 0) or None,
                int(record.get("entryLatencyMs") or 0) if record.get("entryLatencyMs") is not None else None,
                reference_price or None,
                entry_price or None,
                float(record.get("walletVwap") or 0.0) or None,
                str(record.get("executionStatus") or "") or None,
                _record_status(record),
                _json(initial_wallets),
                _json(eligible_wallets),
                _json(rejection_reasons),
                _json(cost_model),
                int(record.get("exitAtMs") or record.get("closedAtMs") or 0) or None,
                float(record.get("exitPrice") or 0.0) or None,
                str(record.get("exitReason") or "") or None,
                None if record.get("delivered") is None else (1 if record.get("delivered") else 0),
                str(record.get("deliveryError") or ""),
                _json(record),
                now_ms,
                now_ms,
            ),
        )
        outcomes = record.get("outcomes")
        if not isinstance(outcomes, dict):
            return
        for horizon, outcome in outcomes.items():
            if not isinstance(outcome, dict):
                continue
            connection.execute(
                """
                INSERT INTO outcomes
                (signal_id, horizon, measured_at_ms, mark_price, gross_return_pct,
                 net_return_pct, double_cost_net_return_pct,
                 delayed_entry_net_return_pct, price_source, measurement_basis,
                 degraded, source_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(signal_id, horizon) DO UPDATE SET
                  measured_at_ms=excluded.measured_at_ms,
                  mark_price=excluded.mark_price,
                  gross_return_pct=excluded.gross_return_pct,
                  net_return_pct=excluded.net_return_pct,
                  double_cost_net_return_pct=excluded.double_cost_net_return_pct,
                  delayed_entry_net_return_pct=excluded.delayed_entry_net_return_pct,
                  price_source=excluded.price_source,
                  measurement_basis=excluded.measurement_basis,
                  degraded=excluded.degraded,
                  source_json=excluded.source_json
                WHERE outcomes.degraded = 1 OR excluded.degraded = 0
                """,
                (
                    signal_id,
                    str(horizon),
                    int(outcome.get("measuredAt") or 0) or None,
                    float(outcome.get("markPrice") or 0.0) or None,
                    float(outcome.get("grossReturnPct") or 0.0),
                    float(outcome.get("netReturnPct") or 0.0),
                    None if outcome.get("doubleCostNetReturnPct") is None else float(outcome["doubleCostNetReturnPct"]),
                    None if outcome.get("delayedEntryNetReturnPct") is None else float(outcome["delayedEntryNetReturnPct"]),
                    str(outcome.get("priceSource") or ""),
                    "mark_price_proxy",
                    1 if outcome.get("degraded") else 0,
                    _json(outcome),
                ),
            )
