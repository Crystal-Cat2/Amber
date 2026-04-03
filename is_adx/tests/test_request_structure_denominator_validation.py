"""请求结构 overall / country / unit 分母一致性校验测试。"""

from __future__ import annotations

import csv
import importlib.util
from pathlib import Path
import shutil
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
PROJECT_DIR = REPO_ROOT / "projects" / "ab_dashboard"
SCRIPT_PATH = PROJECT_DIR / "scripts" / "validate_request_structure_denominators.py"

spec = importlib.util.spec_from_file_location("validate_request_structure_denominators", SCRIPT_PATH)
validator = importlib.util.module_from_spec(spec)
assert spec is not None and spec.loader is not None
spec.loader.exec_module(validator)


def write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as file_obj:
        writer = csv.DictWriter(file_obj, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


class RequestStructureDenominatorValidationTests(unittest.TestCase):
    """覆盖 metric1/2/3 的分母汇总规则。"""

    def test_summarize_metric1_uses_success_scope_all_and_unique_dimension_denominators(self) -> None:
        rows = validator.summarize_metric(
            "metric1",
            [
                {
                    "experiment_group": "no_is_adx",
                    "product": "com.demo.app",
                    "ad_format": "interstitial",
                    "success_scope": "all",
                    "network_cnt": "1",
                    "denominator_request_pv": "10",
                },
                {
                    "experiment_group": "no_is_adx",
                    "product": "com.demo.app",
                    "ad_format": "interstitial",
                    "success_scope": "all",
                    "network_cnt": "2",
                    "denominator_request_pv": "10",
                },
                {
                    "experiment_group": "no_is_adx",
                    "product": "com.demo.app",
                    "ad_format": "interstitial",
                    "success_scope": "has_success",
                    "network_cnt": "1",
                    "denominator_request_pv": "4",
                },
            ],
            [
                {
                    "experiment_group": "no_is_adx",
                    "product": "com.demo.app",
                    "ad_format": "interstitial",
                    "country": "US",
                    "network_cnt": "1",
                    "denominator_request_pv": "6",
                },
                {
                    "experiment_group": "no_is_adx",
                    "product": "com.demo.app",
                    "ad_format": "interstitial",
                    "country": "US",
                    "network_cnt": "2",
                    "denominator_request_pv": "6",
                },
                {
                    "experiment_group": "no_is_adx",
                    "product": "com.demo.app",
                    "ad_format": "interstitial",
                    "country": "JP",
                    "network_cnt": "1",
                    "denominator_request_pv": "4",
                },
            ],
            [
                {
                    "experiment_group": "no_is_adx",
                    "product": "com.demo.app",
                    "ad_format": "interstitial",
                    "max_unit_id": "u1",
                    "network_cnt": "1",
                    "denominator_request_pv": "3",
                },
                {
                    "experiment_group": "no_is_adx",
                    "product": "com.demo.app",
                    "ad_format": "interstitial",
                    "max_unit_id": "u1",
                    "network_cnt": "2",
                    "denominator_request_pv": "3",
                },
                {
                    "experiment_group": "no_is_adx",
                    "product": "com.demo.app",
                    "ad_format": "interstitial",
                    "max_unit_id": "u2",
                    "network_cnt": "1",
                    "denominator_request_pv": "7",
                },
            ],
        )
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["overall_den"], 10)
        self.assertEqual(rows[0]["country_den_sum"], 10)
        self.assertEqual(rows[0]["unit_den_sum"], 10)
        self.assertEqual(rows[0]["status"], "PASS")

    def test_summarize_metric2_deduplicates_denominator_by_network_cnt(self) -> None:
        rows = validator.summarize_metric(
            "metric2",
            [
                {
                    "experiment_group": "have_is_adx",
                    "product": "ios.demo.app",
                    "ad_format": "rewarded",
                    "success_scope": "all",
                    "network_cnt": "1",
                    "bidding_cnt": "0",
                    "waterfall_cnt": "1",
                    "denominator_request_pv": "3",
                },
                {
                    "experiment_group": "have_is_adx",
                    "product": "ios.demo.app",
                    "ad_format": "rewarded",
                    "success_scope": "all",
                    "network_cnt": "1",
                    "bidding_cnt": "1",
                    "waterfall_cnt": "0",
                    "denominator_request_pv": "3",
                },
                {
                    "experiment_group": "have_is_adx",
                    "product": "ios.demo.app",
                    "ad_format": "rewarded",
                    "success_scope": "all",
                    "network_cnt": "2",
                    "bidding_cnt": "1",
                    "waterfall_cnt": "1",
                    "denominator_request_pv": "7",
                },
            ],
            [
                {
                    "experiment_group": "have_is_adx",
                    "product": "ios.demo.app",
                    "ad_format": "rewarded",
                    "country": "US",
                    "network_cnt": "1",
                    "bidding_cnt": "0",
                    "waterfall_cnt": "1",
                    "denominator_request_pv": "2",
                },
                {
                    "experiment_group": "have_is_adx",
                    "product": "ios.demo.app",
                    "ad_format": "rewarded",
                    "country": "US",
                    "network_cnt": "1",
                    "bidding_cnt": "1",
                    "waterfall_cnt": "0",
                    "denominator_request_pv": "2",
                },
                {
                    "experiment_group": "have_is_adx",
                    "product": "ios.demo.app",
                    "ad_format": "rewarded",
                    "country": "JP",
                    "network_cnt": "1",
                    "bidding_cnt": "0",
                    "waterfall_cnt": "1",
                    "denominator_request_pv": "1",
                },
                {
                    "experiment_group": "have_is_adx",
                    "product": "ios.demo.app",
                    "ad_format": "rewarded",
                    "country": "US",
                    "network_cnt": "2",
                    "bidding_cnt": "1",
                    "waterfall_cnt": "1",
                    "denominator_request_pv": "7",
                },
            ],
            [
                {
                    "experiment_group": "have_is_adx",
                    "product": "ios.demo.app",
                    "ad_format": "rewarded",
                    "max_unit_id": "u1",
                    "network_cnt": "1",
                    "bidding_cnt": "0",
                    "waterfall_cnt": "1",
                    "denominator_request_pv": "3",
                },
                {
                    "experiment_group": "have_is_adx",
                    "product": "ios.demo.app",
                    "ad_format": "rewarded",
                    "max_unit_id": "u1",
                    "network_cnt": "1",
                    "bidding_cnt": "1",
                    "waterfall_cnt": "0",
                    "denominator_request_pv": "3",
                },
                {
                    "experiment_group": "have_is_adx",
                    "product": "ios.demo.app",
                    "ad_format": "rewarded",
                    "max_unit_id": "u2",
                    "network_cnt": "2",
                    "bidding_cnt": "1",
                    "waterfall_cnt": "1",
                    "denominator_request_pv": "7",
                },
            ],
        )
        self.assertEqual(rows[0]["overall_den"], 10)
        self.assertEqual(rows[0]["country_den_sum"], 10)
        self.assertEqual(rows[0]["unit_den_sum"], 10)
        self.assertEqual(rows[0]["status"], "PASS")

    def test_summarize_metric3_deduplicates_denominator_by_type_bucket(self) -> None:
        rows = validator.summarize_metric(
            "metric3",
            [
                {
                    "experiment_group": "no_is_adx",
                    "product": "ios.demo.app",
                    "ad_format": "interstitial",
                    "success_scope": "all",
                    "network_cnt": "2",
                    "network_type": "bidding",
                    "type_network_cnt": "1",
                    "status_bucket": "AD_LOADED",
                    "denominator_request_pv": "3",
                },
                {
                    "experiment_group": "no_is_adx",
                    "product": "ios.demo.app",
                    "ad_format": "interstitial",
                    "success_scope": "all",
                    "network_cnt": "2",
                    "network_type": "bidding",
                    "type_network_cnt": "1",
                    "status_bucket": "FAILED_TO_LOAD",
                    "denominator_request_pv": "3",
                },
                {
                    "experiment_group": "no_is_adx",
                    "product": "ios.demo.app",
                    "ad_format": "interstitial",
                    "success_scope": "all",
                    "network_cnt": "2",
                    "network_type": "waterfall",
                    "type_network_cnt": "1",
                    "status_bucket": "AD_LOADED",
                    "denominator_request_pv": "7",
                },
            ],
            [
                {
                    "experiment_group": "no_is_adx",
                    "product": "ios.demo.app",
                    "ad_format": "interstitial",
                    "country": "US",
                    "network_cnt": "2",
                    "network_type": "bidding",
                    "type_network_cnt": "1",
                    "status_bucket": "AD_LOADED",
                    "denominator_request_pv": "3",
                },
                {
                    "experiment_group": "no_is_adx",
                    "product": "ios.demo.app",
                    "ad_format": "interstitial",
                    "country": "US",
                    "network_cnt": "2",
                    "network_type": "bidding",
                    "type_network_cnt": "1",
                    "status_bucket": "FAILED_TO_LOAD",
                    "denominator_request_pv": "3",
                },
                {
                    "experiment_group": "no_is_adx",
                    "product": "ios.demo.app",
                    "ad_format": "interstitial",
                    "country": "JP",
                    "network_cnt": "2",
                    "network_type": "waterfall",
                    "type_network_cnt": "1",
                    "status_bucket": "AD_LOADED",
                    "denominator_request_pv": "7",
                },
            ],
            [
                {
                    "experiment_group": "no_is_adx",
                    "product": "ios.demo.app",
                    "ad_format": "interstitial",
                    "max_unit_id": "u1",
                    "network_cnt": "2",
                    "network_type": "bidding",
                    "type_network_cnt": "1",
                    "status_bucket": "AD_LOADED",
                    "denominator_request_pv": "3",
                },
                {
                    "experiment_group": "no_is_adx",
                    "product": "ios.demo.app",
                    "ad_format": "interstitial",
                    "max_unit_id": "u1",
                    "network_cnt": "2",
                    "network_type": "bidding",
                    "type_network_cnt": "1",
                    "status_bucket": "FAILED_TO_LOAD",
                    "denominator_request_pv": "3",
                },
                {
                    "experiment_group": "no_is_adx",
                    "product": "ios.demo.app",
                    "ad_format": "interstitial",
                    "max_unit_id": "u2",
                    "network_cnt": "2",
                    "network_type": "waterfall",
                    "type_network_cnt": "1",
                    "status_bucket": "AD_LOADED",
                    "denominator_request_pv": "7",
                },
            ],
        )
        self.assertEqual(rows[0]["overall_den"], 10)
        self.assertEqual(rows[0]["country_den_sum"], 10)
        self.assertEqual(rows[0]["unit_den_sum"], 10)
        self.assertEqual(rows[0]["status"], "PASS")

    def test_write_validation_report_writes_csv_for_all_metrics(self) -> None:
        output_dir = PROJECT_DIR / "outputs" / "_tmp_denominator_validation_test"
        if output_dir.exists():
            shutil.rmtree(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        try:
            write_csv(
                output_dir / "metric1_request_network_cnt.csv",
                [
                    {"experiment_group": "no_is_adx", "product": "com.demo.app", "ad_format": "interstitial", "success_scope": "all", "network_cnt": "1", "denominator_request_pv": "10"},
                    {"experiment_group": "no_is_adx", "product": "com.demo.app", "ad_format": "interstitial", "success_scope": "all", "network_cnt": "2", "denominator_request_pv": "10"},
                ],
            )
            write_csv(
                output_dir / "metric1_request_network_cnt_country.csv",
                [
                    {"experiment_group": "no_is_adx", "product": "com.demo.app", "ad_format": "interstitial", "country": "US", "network_cnt": "1", "denominator_request_pv": "6"},
                    {"experiment_group": "no_is_adx", "product": "com.demo.app", "ad_format": "interstitial", "country": "JP", "network_cnt": "1", "denominator_request_pv": "4"},
                ],
            )
            write_csv(
                output_dir / "metric1_request_network_cnt_unit.csv",
                [
                    {"experiment_group": "no_is_adx", "product": "com.demo.app", "ad_format": "interstitial", "max_unit_id": "u1", "network_cnt": "1", "denominator_request_pv": "3"},
                    {"experiment_group": "no_is_adx", "product": "com.demo.app", "ad_format": "interstitial", "max_unit_id": "u2", "network_cnt": "1", "denominator_request_pv": "7"},
                ],
            )
            write_csv(
                output_dir / "metric2_network_type_status_cnt.csv",
                [
                    {"experiment_group": "no_is_adx", "product": "com.demo.app", "ad_format": "interstitial", "success_scope": "all", "network_cnt": "1", "bidding_cnt": "0", "waterfall_cnt": "1", "denominator_request_pv": "10"},
                ],
            )
            write_csv(
                output_dir / "metric2_network_type_status_cnt_country.csv",
                [
                    {"experiment_group": "no_is_adx", "product": "com.demo.app", "ad_format": "interstitial", "country": "US", "network_cnt": "1", "bidding_cnt": "0", "waterfall_cnt": "1", "denominator_request_pv": "10"},
                ],
            )
            write_csv(
                output_dir / "metric2_network_type_status_cnt_unit.csv",
                [
                    {"experiment_group": "no_is_adx", "product": "com.demo.app", "ad_format": "interstitial", "max_unit_id": "u1", "network_cnt": "1", "bidding_cnt": "0", "waterfall_cnt": "1", "denominator_request_pv": "10"},
                ],
            )
            write_csv(
                output_dir / "metric3_network_distribution.csv",
                [
                    {"experiment_group": "no_is_adx", "product": "com.demo.app", "ad_format": "interstitial", "success_scope": "all", "network_cnt": "1", "network_type": "bidding", "type_network_cnt": "1", "status_bucket": "AD_LOADED", "denominator_request_pv": "10"},
                ],
            )
            write_csv(
                output_dir / "metric3_network_distribution_country.csv",
                [
                    {"experiment_group": "no_is_adx", "product": "com.demo.app", "ad_format": "interstitial", "country": "US", "network_cnt": "1", "network_type": "bidding", "type_network_cnt": "1", "status_bucket": "AD_LOADED", "denominator_request_pv": "10"},
                ],
            )
            write_csv(
                output_dir / "metric3_network_distribution_unit.csv",
                [
                    {"experiment_group": "no_is_adx", "product": "com.demo.app", "ad_format": "interstitial", "max_unit_id": "u1", "network_cnt": "1", "network_type": "bidding", "type_network_cnt": "1", "status_bucket": "AD_LOADED", "denominator_request_pv": "10"},
                ],
            )

            output_path = output_dir / "request_structure_denominator_validation.csv"
            rows = validator.write_validation_report(output_dir=output_dir, output_path=output_path)

            self.assertEqual(len(rows), 3)
            self.assertTrue(output_path.exists())
            with output_path.open("r", encoding="utf-8-sig", newline="") as file_obj:
                written_rows = list(csv.DictReader(file_obj))
            self.assertEqual(len(written_rows), 3)
            self.assertEqual({row["metric"] for row in written_rows}, {"metric1", "metric2", "metric3"})
            self.assertEqual({row["status"] for row in written_rows}, {"PASS"})
        finally:
            if output_dir.exists():
                shutil.rmtree(output_dir)


if __name__ == "__main__":
    unittest.main()
