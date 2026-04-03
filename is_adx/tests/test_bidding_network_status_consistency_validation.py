"""bidding network status 配置口径校验测试。"""

from __future__ import annotations

import csv
import importlib.util
from pathlib import Path
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
PROJECT_DIR = REPO_ROOT / "projects" / "ab_dashboard"
SCRIPT_PATH = PROJECT_DIR / "scripts" / "validate_bidding_network_status_consistency.py"

spec = importlib.util.spec_from_file_location("validate_bidding_network_status_consistency", SCRIPT_PATH)
validator = importlib.util.module_from_spec(spec)
assert spec is not None and spec.loader is not None
spec.loader.exec_module(validator)


def write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as file_obj:
        writer = csv.DictWriter(file_obj, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


class BiddingNetworkStatusConsistencyValidationTests(unittest.TestCase):
    """覆盖 ALL UNIT 配置口径与 NULL 补齐的校验逻辑。"""

    def test_summarize_rows_uses_configured_units_and_synthesizes_null(self) -> None:
        rows = validator.summarize_rows(
            unit_rows=[
                {
                    "experiment_group": "no_is_adx",
                    "product": "com.demo.app",
                    "ad_format": "interstitial",
                    "network_type": "bidding",
                    "network": "AdMob",
                    "status_bucket": "AD_LOADED",
                    "request_pv": "10",
                    "denominator_request_pv": "100",
                    "share": "0.1",
                    "max_unit_id": "u1",
                },
                {
                    "experiment_group": "no_is_adx",
                    "product": "com.demo.app",
                    "ad_format": "interstitial",
                    "network_type": "bidding",
                    "network": "AdMob",
                    "status_bucket": "NULL",
                    "request_pv": "90",
                    "denominator_request_pv": "100",
                    "share": "0.9",
                    "max_unit_id": "u1",
                },
                {
                    "experiment_group": "no_is_adx",
                    "product": "com.demo.app",
                    "ad_format": "interstitial",
                    "network_type": "bidding",
                    "network": "Pangle",
                    "status_bucket": "AD_LOADED",
                    "request_pv": "40",
                    "denominator_request_pv": "50",
                    "share": "0.8",
                    "max_unit_id": "u2",
                },
            ],
            configured_units_by_channel={
                ("com.demo.app", "interstitial", "bidding", "AdMob"): {"u1", "u2"},
                ("com.demo.app", "interstitial", "bidding", "Pangle"): {"u2"},
            },
        )
        admob_null = next(
            row
            for row in rows
            if row["network"] == "AdMob"
            and row["status_bucket"] == "NULL"
            and row["experiment_group"] == "no_is_adx"
        )
        self.assertEqual(admob_null["configured_unit_count"], 2)
        self.assertEqual(admob_null["observed_unit_count"], 1)
        self.assertEqual(admob_null["configured_total_request_pv"], 90)
        self.assertEqual(admob_null["configured_denominator_request_pv"], 150)
        self.assertAlmostEqual(admob_null["configured_share"], 90 / 150)
        self.assertEqual(admob_null["status"], "WARN")

    def test_write_validation_report_outputs_csv_and_summary(self) -> None:
        output_dir = PROJECT_DIR / "outputs" / "_tmp_bidding_status_validation"
        unit_path = output_dir / "bidding_network_status_share_by_unit.csv"
        mediation_path = output_dir / "mediation_report.csv"
        result_path = output_dir / "bidding_network_status_all_unit_validation.csv"

        write_csv(
            mediation_path,
            [
                {
                    "Application": "Demo App",
                    "Package Name": "com.demo.app",
                    "Network": "Google Bidding",
                    "Network Type": "Bidding",
                    "Custom Network/Campaign Name": "",
                    "Ad Unit Name": "Demo Inter P1 (u1)",
                    "Ad Type": "Interstitial",
                }
            ],
        )
        write_csv(
            unit_path,
            [
                {
                    "experiment_group": "have_is_adx",
                    "product": "com.demo.app",
                    "ad_format": "interstitial",
                    "max_unit_id": "u1",
                    "network_type": "bidding",
                    "network": "AdMob",
                    "status_bucket": "NULL",
                    "request_pv": "20",
                    "denominator_request_pv": "20",
                    "share": "1.0",
                }
            ],
        )

        rows = validator.write_validation_report(
            unit_path=unit_path,
            mediation_path=mediation_path,
            output_path=result_path,
        )

        self.assertTrue(result_path.exists())
        self.assertEqual(rows[0]["status"], "PASS")
        self.assertIn("配置口径", validator.build_conclusion(rows))


if __name__ == "__main__":
    unittest.main()
