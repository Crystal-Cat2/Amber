"""校验 request structure 在 overall / country / unit 三套输出下的分母一致性。"""

from __future__ import annotations

import csv
from pathlib import Path
from typing import Any


PROJECT_DIR = Path(__file__).resolve().parents[1]
OUTPUT_DIR = PROJECT_DIR / "outputs"
VALIDATION_OUTPUT_CSV = OUTPUT_DIR / "request_structure_denominator_validation.csv"
BASE_FIELDS = ("experiment_group", "product", "ad_format")
STATUS_PASS = "PASS"
STATUS_FAIL = "FAIL"
METRIC_ORDER = {"metric1": 0, "metric2": 1, "metric3": 2}
GROUP_ORDER = {"no_is_adx": 0, "have_is_adx": 1}
FORMAT_ORDER = {"interstitial": 0, "rewarded": 1}
PLATFORM_ORDER = {"android": 0, "ios": 1}
METRIC_CONFIGS = {
    "metric1": {
        "overall_csv": "metric1_request_network_cnt.csv",
        "country_csv": "metric1_request_network_cnt_country.csv",
        "unit_csv": "metric1_request_network_cnt_unit.csv",
        "overall_group_fields": (),
        "country_group_fields": ("country",),
        "unit_group_fields": ("max_unit_id",),
    },
    "metric2": {
        "overall_csv": "metric2_network_type_status_cnt.csv",
        "country_csv": "metric2_network_type_status_cnt_country.csv",
        "unit_csv": "metric2_network_type_status_cnt_unit.csv",
        "overall_group_fields": ("network_cnt",),
        "country_group_fields": ("country", "network_cnt"),
        "unit_group_fields": ("max_unit_id", "network_cnt"),
    },
    "metric3": {
        "overall_csv": "metric3_network_distribution.csv",
        "country_csv": "metric3_network_distribution_country.csv",
        "unit_csv": "metric3_network_distribution_unit.csv",
        "overall_group_fields": ("network_cnt", "network_type", "type_network_cnt"),
        "country_group_fields": ("country", "network_cnt", "network_type", "type_network_cnt"),
        "unit_group_fields": ("max_unit_id", "network_cnt", "network_type", "type_network_cnt"),
    },
}
OUTPUT_FIELDS = [
    "metric",
    "experiment_group",
    "platform",
    "product",
    "ad_format",
    "overall_den",
    "country_den_sum",
    "unit_den_sum",
    "country_diff",
    "unit_diff",
    "status",
]


def load_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as file_obj:
        return list(csv.DictReader(file_obj))


def to_int(value: Any) -> int:
    if value in (None, ""):
        return 0
    return int(round(float(value)))


def infer_platform(product: str) -> str:
    return "ios" if str(product).startswith("ios.") else "android"


def base_key(row: dict[str, Any]) -> tuple[str, str, str]:
    return tuple(str(row.get(field) or "") for field in BASE_FIELDS)


def unique_denominator_sum(rows: list[dict[str, Any]], group_fields: tuple[str, ...]) -> int:
    if not rows:
        return 0
    if not group_fields:
        return to_int(rows[0].get("denominator_request_pv"))

    denominator_by_group: dict[tuple[str, ...], int] = {}
    for row in rows:
        group_key = tuple(str(row.get(field) or "") for field in group_fields)
        if group_key not in denominator_by_group:
            denominator_by_group[group_key] = to_int(row.get("denominator_request_pv"))
    return sum(denominator_by_group.values())


def filter_overall_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if any("success_scope" in row for row in rows):
        return [row for row in rows if str(row.get("success_scope") or "") == "all"]
    return rows


def summarize_metric(
    metric_name: str,
    overall_rows: list[dict[str, Any]],
    country_rows: list[dict[str, Any]],
    unit_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    config = METRIC_CONFIGS[metric_name]
    filtered_overall_rows = filter_overall_rows(overall_rows)
    combo_keys = sorted(
        {
            base_key(row)
            for row in filtered_overall_rows + country_rows + unit_rows
        }
    )
    summary_rows: list[dict[str, Any]] = []
    for combo in combo_keys:
        experiment_group, product, ad_format = combo
        overall_combo_rows = [row for row in filtered_overall_rows if base_key(row) == combo]
        country_combo_rows = [row for row in country_rows if base_key(row) == combo]
        unit_combo_rows = [row for row in unit_rows if base_key(row) == combo]
        overall_den = unique_denominator_sum(overall_combo_rows, config["overall_group_fields"])
        country_den_sum = unique_denominator_sum(country_combo_rows, config["country_group_fields"])
        unit_den_sum = unique_denominator_sum(unit_combo_rows, config["unit_group_fields"])
        country_diff = country_den_sum - overall_den
        unit_diff = unit_den_sum - overall_den
        summary_rows.append(
            {
                "metric": metric_name,
                "experiment_group": experiment_group,
                "platform": infer_platform(product),
                "product": product,
                "ad_format": ad_format,
                "overall_den": overall_den,
                "country_den_sum": country_den_sum,
                "unit_den_sum": unit_den_sum,
                "country_diff": country_diff,
                "unit_diff": unit_diff,
                "status": STATUS_PASS if country_diff == 0 and unit_diff == 0 else STATUS_FAIL,
            }
        )
    return summary_rows


def sort_summary_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        rows,
        key=lambda row: (
            METRIC_ORDER.get(str(row["metric"]), 99),
            PLATFORM_ORDER.get(str(row["platform"]), 99),
            FORMAT_ORDER.get(str(row["ad_format"]), 99),
            GROUP_ORDER.get(str(row["experiment_group"]), 99),
            str(row["product"]),
        ),
    )


def write_validation_report(
    *,
    output_dir: Path = OUTPUT_DIR,
    output_path: Path = VALIDATION_OUTPUT_CSV,
) -> list[dict[str, Any]]:
    all_rows: list[dict[str, Any]] = []
    for metric_name, config in METRIC_CONFIGS.items():
        all_rows.extend(
            summarize_metric(
                metric_name,
                load_rows(output_dir / config["overall_csv"]),
                load_rows(output_dir / config["country_csv"]),
                load_rows(output_dir / config["unit_csv"]),
            )
        )
    sorted_rows = sort_summary_rows(all_rows)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8", newline="") as file_obj:
        writer = csv.DictWriter(file_obj, fieldnames=OUTPUT_FIELDS)
        writer.writeheader()
        writer.writerows(sorted_rows)
    return sorted_rows


def build_conclusion(rows: list[dict[str, Any]]) -> str:
    return (
        "3 个 metric 的 overall / country / unit 分母全部一致"
        if all(str(row["status"]) == STATUS_PASS for row in rows)
        else "存在不一致，详见 request_structure_denominator_validation.csv"
    )


def main() -> None:
    rows = write_validation_report()
    print(f"已生成校验结果：{VALIDATION_OUTPUT_CSV}")
    print(build_conclusion(rows))


if __name__ == "__main__":
    main()
