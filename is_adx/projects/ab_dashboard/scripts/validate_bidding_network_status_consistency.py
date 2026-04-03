"""校验 bidding network status 的 ALL UNIT 配置口径与 NULL 补齐情况。"""

from __future__ import annotations

import csv
import sys
from pathlib import Path
from typing import Any

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.append(str(SCRIPT_DIR))

from mediation_scope import load_mediation_configuration


PROJECT_DIR = Path(__file__).resolve().parents[1]
OUTPUT_DIR = PROJECT_DIR / "outputs"
UNIT_CSV = OUTPUT_DIR / "bidding_network_status_share_by_unit.csv"
MEDIATION_CSV = Path(r"D:\Downloads\mediation_report_2026-03-25_09_41_32.csv")
VALIDATION_OUTPUT_CSV = OUTPUT_DIR / "bidding_network_status_all_unit_validation.csv"
KEY_FIELDS = ("experiment_group", "product", "ad_format", "network_type", "network", "status_bucket")
OUTPUT_FIELDS = [
    "experiment_group",
    "product",
    "ad_format",
    "network_type",
    "network",
    "status_bucket",
    "configured_unit_count",
    "observed_unit_count",
    "configured_total_request_pv",
    "configured_denominator_request_pv",
    "configured_share",
    "status",
]


def load_rows(path: Path) -> list[dict[str, Any]]:
    with path.open("r", encoding="utf-8-sig", newline="") as file_obj:
        return list(csv.DictReader(file_obj))


def to_float(value: Any) -> float:
    if value in (None, ""):
        return 0.0
    return float(value)


def infer_platform(product: str) -> str:
    return "ios" if str(product).startswith("ios.") else "android"


def summarize_rows(
    *,
    unit_rows: list[dict[str, Any]],
    configured_units_by_channel: dict[tuple[str, str, str, str], set[str]],
) -> list[dict[str, Any]]:
    denominator_by_unit: dict[tuple[str, str, str, str, str], float] = {}
    observed_counts: dict[tuple[str, str, str, str, str, str, str, str], float] = {}
    observed_units_by_key: dict[tuple[str, str, str, str, str], set[str]] = {}

    for row in unit_rows:
        experiment_group = str(row.get("experiment_group") or "").strip()
        product = str(row.get("product") or "").strip()
        ad_format = str(row.get("ad_format") or "").strip().lower()
        network_type = str(row.get("network_type") or "").strip().lower()
        network = str(row.get("network") or "").strip()
        status_bucket = str(row.get("status_bucket") or "").strip().upper()
        unit_id = str(row.get("max_unit_id") or "").strip()
        if not all((experiment_group, product, ad_format, network_type, network, status_bucket, unit_id)):
            continue
        platform = infer_platform(product)
        denominator_by_unit[(product, platform, ad_format, unit_id, experiment_group)] = max(
            denominator_by_unit.get((product, platform, ad_format, unit_id, experiment_group), 0.0),
            to_float(row.get("denominator_request_pv")),
        )
        observed_counts[(product, platform, ad_format, unit_id, experiment_group, network_type, network, status_bucket)] = (
            observed_counts.get((product, platform, ad_format, unit_id, experiment_group, network_type, network, status_bucket), 0.0)
            + to_float(row.get("request_pv"))
        )
        observed_units_by_key.setdefault((experiment_group, product, ad_format, network_type, network), set()).add(unit_id)

    summary_rows: list[dict[str, Any]] = []
    all_keys = sorted(
        {
            (experiment_group, product, ad_format, network_type, network)
            for experiment_group, product, ad_format, network_type, network in observed_units_by_key
        }
        | {
            (experiment_group, product, ad_format, network_type, network)
            for experiment_group in {"no_is_adx", "have_is_adx"}
            for product, ad_format, network_type, network in configured_units_by_channel
        }
    )
    for experiment_group, product, ad_format, network_type, network in all_keys:
        configured_units = configured_units_by_channel.get((product, ad_format, network_type, network), set())
        status_order = ["AD_LOADED", "FAILED_TO_LOAD", "AD_LOAD_NOT_ATTEMPTED", "NULL"]
        for status_bucket in status_order:
            configured_total_request_pv = 0.0
            configured_denominator_request_pv = 0.0
            for unit_id in configured_units:
                denominator = denominator_by_unit.get((product, infer_platform(product), ad_format, unit_id, experiment_group), 0.0)
                if denominator <= 0:
                    continue
                configured_denominator_request_pv += denominator
                real_sum = 0.0
                for real_status in ("AD_LOADED", "FAILED_TO_LOAD", "AD_LOAD_NOT_ATTEMPTED"):
                    request_pv = observed_counts.get((product, infer_platform(product), ad_format, unit_id, experiment_group, network_type, network, real_status), 0.0)
                    if status_bucket == real_status:
                        configured_total_request_pv += request_pv
                    real_sum += request_pv
                if status_bucket == "NULL":
                    observed_null = observed_counts.get((product, infer_platform(product), ad_format, unit_id, experiment_group, network_type, network, "NULL"), 0.0)
                    configured_total_request_pv += observed_null
            observed_unit_count = len(observed_units_by_key.get((experiment_group, product, ad_format, network_type, network), set()))
            configured_unit_count = len(configured_units)
            if configured_denominator_request_pv <= 0 and configured_total_request_pv <= 0:
                continue
            summary_rows.append(
                {
                    "experiment_group": experiment_group,
                    "product": product,
                    "ad_format": ad_format,
                    "network_type": network_type,
                    "network": network,
                    "status_bucket": status_bucket,
                    "configured_unit_count": configured_unit_count,
                    "observed_unit_count": observed_unit_count,
                    "configured_total_request_pv": int(round(configured_total_request_pv)),
                    "configured_denominator_request_pv": int(round(configured_denominator_request_pv)),
                    "configured_share": (configured_total_request_pv / configured_denominator_request_pv) if configured_denominator_request_pv else 0.0,
                    "status": "WARN" if observed_unit_count < configured_unit_count else "PASS",
                }
            )
    return summary_rows


def write_validation_report(
    *,
    unit_path: Path = UNIT_CSV,
    mediation_path: Path = MEDIATION_CSV,
    output_path: Path = VALIDATION_OUTPUT_CSV,
) -> list[dict[str, Any]]:
    _, configured_units_by_channel = load_mediation_configuration(mediation_path)
    rows = summarize_rows(
        unit_rows=load_rows(unit_path),
        configured_units_by_channel=configured_units_by_channel,
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8", newline="") as file_obj:
        writer = csv.DictWriter(file_obj, fieldnames=OUTPUT_FIELDS)
        writer.writeheader()
        writer.writerows(rows)
    return rows


def build_conclusion(rows: list[dict[str, Any]]) -> str:
    warn_count = sum(1 for row in rows if str(row.get("status") or "") == "WARN")
    if warn_count == 0:
        return "ALL UNIT 配置口径校验通过"
    return f"ALL UNIT 配置口径存在 {warn_count} 条 WARN，详见 bidding_network_status_all_unit_validation.csv"


def main() -> None:
    rows = write_validation_report()
    print(f"已生成校验结果：{VALIDATION_OUTPUT_CSV}")
    print(build_conclusion(rows))


if __name__ == "__main__":
    main()
