import json
import sqlite3
import tempfile
import unittest
from contextlib import closing
from pathlib import Path

from execution_journal import ExecutionJournal


class ExecutionJournalTests(unittest.TestCase):
    def test_source_change_pauses_new_entries_even_after_revert(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            journal = ExecutionJournal(Path(directory) / "experiment.sqlite3")
            self.assertFalse(journal.freeze_status(
                config_hash="one", alert_config_hash="alert-one",
                tracked_wallets=["0xa"], now_ms=1000,
            )["allowNewEntries"])
            journal.sync(
                manifest={"ruleVersion": "v1"}, config_hash="one",
                streams={}, now_ms=1000,
                context={
                    "alertConfigHash": "alert-one",
                    "trackedWalletAddresses": ["0xa"],
                },
            )
            self.assertTrue(journal.freeze_status(
                config_hash="one", alert_config_hash="alert-one",
                tracked_wallets=["0xa"], now_ms=2000,
            )["allowNewEntries"])
            changed = journal.freeze_status(
                config_hash="two", alert_config_hash="alert-one",
                tracked_wallets=["0xa"], now_ms=3000,
            )
            self.assertEqual(changed["reasons"], ["rules_changed"])
            self.assertFalse(changed["allowNewEntries"])
            reverted = journal.freeze_status(
                config_hash="one", alert_config_hash="alert-one",
                tracked_wallets=["0xa"], now_ms=4000,
            )
            self.assertFalse(reverted["allowNewEntries"])

    def test_idempotent_open_signal_outcome_and_skipped_evaluation(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "experiment.sqlite3"
            journal = ExecutionJournal(path)
            manifest = {"ruleVersion": "test-v1", "costModel": {"roundTripPct": 0.2}}
            signal = {
                "ruleVersion": "test-v1",
                "configHash": "config-one",
                "experimentArm": "ranked_consensus",
                "coin": "BTC",
                "side": "long",
                "startedAt": 1000,
                "entryPrice": 100.0,
                "initialWalletAddresses": ["0xA", "0xB"],
                "topWalletAddresses": ["0xC"],
                "status": "open",
                "outcomes": {},
            }
            evaluation = {
                "evaluationId": "eval-one",
                "evaluatedAtMs": 1000,
                "experimentArm": "consensus_unranked",
                "signalKey": "BTC:long",
                "coin": "BTC",
                "side": "long",
                "eligible": False,
                "walletAddresses": ["0xA"],
                "reasons": ["insufficient_verified_activity"],
                "inputs": {"independentWalletCount": 1},
            }
            for _ in range(2):
                journal.sync(
                    manifest=manifest,
                    config_hash="config-one",
                    streams={"paper": {"signal-one": signal}},
                    evaluations=[evaluation],
                    context={"alertConfigHash": "hash-only", "trackedWalletAddresses": ["0xa"]},
                    oracle_samples=[{"coin": "BTC", "observedAtMs": 1000, "oraclePrice": 100}],
                    portfolio_snapshots=[{
                        "arm": "ranked_consensus", "observedAtMs": 1000,
                        "complete": True, "equityUsd": 10_000,
                    }],
                    now_ms=1000,
                )
            signal["outcomes"] = {
                "4h": {
                    "markPrice": 101.0,
                    "grossReturnPct": 1.0,
                    "netReturnPct": 0.8,
                    "doubleCostNetReturnPct": 0.6,
                    "measuredAt": 2000,
                }
            }
            journal.sync(
                manifest=manifest,
                config_hash="config-one",
                streams={"paper": {"signal-one": signal}},
                portfolio_snapshots=[{
                    "arm": "ranked_consensus", "observedAtMs": 2000,
                    "complete": True, "equityUsd": 4_000,
                }],
                now_ms=2000,
            )
            with closing(sqlite3.connect(path)) as db:
                self.assertEqual(db.execute("SELECT count(*) FROM signals").fetchone()[0], 1)
                self.assertEqual(db.execute("SELECT count(*) FROM evaluations").fetchone()[0], 1)
                self.assertEqual(db.execute("SELECT count(*) FROM outcomes").fetchone()[0], 1)
                self.assertEqual(
                    db.execute("SELECT measurement_basis FROM outcomes").fetchone()[0],
                    "mark_price_proxy",
                )
                self.assertEqual(db.execute("SELECT count(*) FROM audit_cycles").fetchone()[0], 1)
                self.assertEqual(db.execute("SELECT count(*) FROM paper_oracle_samples").fetchone()[0], 1)
                alert_hash, universe = db.execute(
                    "SELECT alert_config_hash, tracked_wallets_json FROM audit_cycles"
                ).fetchone()
                self.assertEqual(alert_hash, "hash-only")
                self.assertEqual(json.loads(universe), ["0xa"])
                reference, executable, addresses = db.execute(
                    "SELECT reference_price, entry_price, initial_wallets_json FROM signals"
                ).fetchone()
                self.assertEqual(reference, 100.0)
                self.assertIsNone(executable)  # a mark is not an executable fill
                self.assertEqual(json.loads(addresses), ["0xa", "0xb"])
            restored = journal.load_stream("paper")
            self.assertEqual(restored["signal-one"]["outcomes"]["4h"]["netReturnPct"], 0.8)
            self.assertEqual(journal.load_stream("paper", since_ms=1001), {})
            self.assertEqual(
                len(journal.load_stream("paper", since_ms=1001, include_unfinished=True)),
                1,
            )
            self.assertEqual(
                journal.load_oracle_samples("BTC", start_ms=0, end_ms=2000)[0]["oraclePrice"],
                100,
            )
            report = journal.paper_report()
            self.assertEqual(report["arms"]["ranked_consensus"]["open"], 1)
            self.assertEqual(report["arms"]["consensus_unranked"]["evaluations"], 1)
            self.assertEqual(report["arms"]["ranked_consensus"]["closedModeledNet"], 0)
            self.assertEqual(
                report["arms"]["ranked_consensus"]["drawdown"]["observedMaxDrawdownPct"],
                60.0,
            )
            self.assertEqual(report["conclusion"], "insufficient_for_executable_strategy_comparison")
            self.assertIn(
                "ranked_consensus:minimum_closed_episodes_and_decision_days",
                report["missingForDecision"],
            )
            self.assertEqual(report["arms"]["ranked_consensus"]["lastPortfolioEquityUsd"], 4_000)

    def test_report_does_not_treat_one_profitable_trade_as_strategy_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            journal = ExecutionJournal(Path(directory) / "experiment.sqlite3")
            journal.sync(
                manifest={"ruleVersion": "v4", "costModel": {}},
                config_hash="frozen",
                streams={"paper": {"one": {
                    "experimentArm": "ranked_consensus",
                    "coin": "BTC", "marketCoin": "BTC", "side": "long",
                    "startedAt": 1_000, "status": "closed",
                    "executionStatus": "closed_modeled_net",
                    "initialWalletAddresses": ["0xA"],
                    "executionResult": {"complete": True, "netUsd": 100.0},
                    "doubleCostResult": {"complete": True, "netUsd": 90.0},
                    "delayedEntryResult": {"complete": True, "netUsd": 80.0},
                }}},
                portfolio_snapshots=[{
                    "arm": "ranked_consensus", "observedAtMs": 1_000,
                    "complete": True, "equityUsd": 10_100.0,
                }],
                context={"alertConfigHash": "frozen", "trackedWalletAddresses": ["0xa"]},
                now_ms=1_000,
            )
            report = journal.paper_report()
            arm = report["arms"]["ranked_consensus"]
            self.assertEqual(arm["modeledNetUsd"], 100.0)
            self.assertEqual(arm["largestAssetPositiveContributionPct"], 100.0)
            self.assertEqual(arm["largestWalletPositiveContributionPct"], 100.0)
            self.assertEqual(arm["lastPortfolioEquityUsd"], 10_100.0)
            self.assertIn(
                "ranked_consensus:positive_contribution_not_dominated_by_one_asset_or_wallet",
                report["missingForDecision"],
            )
            self.assertEqual(report["conclusion"], "insufficient_for_executable_strategy_comparison")

    def test_historical_record_is_not_relabeled_as_current_rules(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "experiment.sqlite3"
            ExecutionJournal(path).sync(
                manifest={"ruleVersion": "new-v1", "costModel": {}},
                config_hash="new-hash",
                streams={"candidate": {"old-one": {"coin": "ETH", "startedAt": 1000}}},
                now_ms=2000,
            )
            with closing(sqlite3.connect(path)) as db:
                version, config = db.execute(
                    "SELECT rule_version, config_hash FROM signals"
                ).fetchone()
            self.assertEqual((version, config), ("legacy_unversioned", "unknown"))


if __name__ == "__main__":
    unittest.main()
