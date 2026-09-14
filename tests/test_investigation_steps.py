import concurrent.futures
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import investigation_steps as steps

RUN = "d69b8119-697c-4294-a5ef-4ea1f713a6c6"


def payment(method, version, expected, reported):
    return dict(payment_method=method, provider_payload_version=version,
                currency="SGD", expected_amount_cents=expected, reported_amount_cents=reported)


class InvestigationTests(unittest.TestCase):
    def test_drilldown_follows_wallet_not_hardcoded_card(self):
        rows = [payment("card", "v2", 1000, 1000), payment("wallet", "v3", 500, 5)]
        overview = steps.financial_breakdown(rows, "method")
        affected = [g for g in overview["groups"] if g["mismatch_count"]]
        self.assertEqual([g["payment_method"] for g in affected], ["wallet"])
        versions = steps.financial_breakdown(rows, "version", "wallet")
        self.assertEqual(versions["groups"][0]["provider_payload_version"], "v3")
        self.assertEqual(steps.financial_breakdown(rows, "samples", "wallet", "v3")["mismatch_count"], 1)

    def test_healthy_and_missing_cohort(self):
        rows = [payment("card", "v1", 50, 50)]
        self.assertEqual(steps.financial_breakdown(rows, "method")["groups"][0]["mismatch_count"], 0)
        with self.assertRaises(ValueError):
            steps.financial_breakdown(rows, "samples", "wallet", "v2")
        with self.assertRaises(ValueError):
            steps.financial_breakdown(rows, "samples", "card")

    def test_budget_concurrent_and_reserved_verification(self):
        with tempfile.TemporaryDirectory() as temp:
            old = steps.STORE
            steps.STORE = Path(temp) / "calls.sqlite3"
            try:
                @steps.audit_call
                def evidence(run_id):
                    return {"step_summary": "Checked"}
                @steps.audit_call
                def verify_financial_impact(run_id):
                    return {"step_summary": "Verified"}
                def call(_):
                    try:
                        evidence(RUN)
                        return True
                    except RuntimeError:
                        return False
                with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
                    self.assertEqual(sum(pool.map(call, range(15))), 11)
                verify_financial_impact(RUN)
                with self.assertRaises(RuntimeError):
                    verify_financial_impact(RUN)
                self.assertEqual(len(steps.read_steps(RUN)), 12)
                with self.assertRaises(ValueError):
                    evidence("../../etc")
            finally:
                steps.STORE = old

    def test_failure_is_recorded_and_counted(self):
        with tempfile.TemporaryDirectory() as temp:
            old = steps.STORE
            steps.STORE = Path(temp) / "calls.sqlite3"
            try:
                @steps.audit_call
                def evidence(run_id, level="method"):
                    raise ValueError("private backend details")
                with self.assertRaises(ValueError):
                    evidence(RUN)
                trail = steps.read_steps(RUN)
                self.assertEqual(trail[0]["status"], "failed")
                self.assertEqual(trail[0]["arguments"], {"level": "method"})
                self.assertNotIn("private", trail[0]["summary"])
                # Reopening the database retains the consumed call.
                with steps.connect() as db:
                    self.assertEqual(db.execute("SELECT COUNT(*) FROM calls").fetchone()[0], 1)
            finally:
                steps.STORE = old

    def test_offsetting_errors_remain_visible_and_currencies_separate(self):
        rows = [payment("card", "v1", 100, 90), payment("card", "v1", 100, 110)]
        usd = payment("card", "v1", 300, 300)
        usd["currency"] = "USD"
        groups = steps.financial_breakdown(rows + [usd], "method")["groups"]
        self.assertEqual(len(groups), 2)
        sgd = next(g for g in groups if g["currency"] == "SGD")
        self.assertEqual(sgd["difference_cents"], 0)
        self.assertEqual(sgd["mismatch_count"], 2)


if __name__ == "__main__":
    unittest.main()
