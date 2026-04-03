
from __future__ import annotations

import argparse
import csv
import subprocess
import sys
from collections import defaultdict
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path


ROOT = Path(__file__).resolve().parent
DEFAULT_BASE_SQL = ROOT / "2026-04-01_car_jam_tp_adx_ab_base.sql"
DEFAULT_DIAG_SQL = ROOT / "2026-04-01_car_jam_tp_adx_ab_diagnostics.sql"
DEFAULT_BASE_CSV = ROOT / "2026-04-01_car_jam_tp_adx_ab_base.csv"
DEFAULT_DIAG_CSV = ROOT / "2026-04-01_car_jam_tp_adx_ab_diagnostics.csv"
DEFAULT_MARKDOWN = ROOT / "2026-04-01_car_jam_tp_adx_ab_report.md"

GROUP_A = "no_tp_adx"
GROUP_B = "have_tp_adx"
TRADPLUS_NETWORK = "TradPlus_ADX"
TRADPLUS_SOURCE = "tradplus_external"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run local BigQuery for car_jam TP ADX AB report."
    )
    parser.add_argument("--base-sql", type=Path, default=DEFAULT_BASE_SQL)
    parser.add_argument("--diag-sql", type=Path, default=DEFAULT_DIAG_SQL)
    parser.add_argument("--base-csv", type=Path, default=DEFAULT_BASE_CSV)
    parser.add_argument("--diag-csv", type=Path, default=DEFAULT_DIAG_CSV)
    parser.add_argument("--markdown", type=Path, default=DEFAULT_MARKDOWN)
    parser.add_argument(
        "--skip-query",
        action="store_true",
        help="Skip bq query and render markdown from existing CSV files.",
    )
    return parser.parse_args()


def run_bq_query(sql_path: Path, csv_path: Path) -> None:
    sql_text = sql_path.read_text(encoding="utf-8")
    cmd = [
        "bq",
        "query",
        "--use_legacy_sql=false",
        "--format=csv",
        "--max_rows=1000000",
    ]
    result = subprocess.run(
        cmd,
        input=sql_text,
        text=True,
        capture_output=True,
        encoding="utf-8",
        errors="strict",
    )
    if result.returncode != 0:
        raise RuntimeError(
            "execution_path = local_bigquery\\n"
            "bq query failed.\\n"
            f"SQL: {sql_path}\\n"
            f"stderr:\\n{result.stderr.strip()}\\n\\n"
            "Please confirm:\\n"
            "1. gcloud auth login or local ADC is ready\\n"
            "2. MAX / TradPlus table names in SQL match your local environment"
        )
    csv_path.write_text(result.stdout, encoding="utf-8", newline="")


def read_csv_rows(csv_path: Path) -> list[dict[str, str]]:
    with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        rows: list[dict[str, str]] = []
        for row in reader:
            normalized = {}
            for key, value in row.items():
                if key is None:
                    continue
                normalized[str(key).strip().lstrip("\ufeff")] = value
            rows.append(normalized)
        return rows


def to_decimal(value: str | None) -> Decimal:
    if value in (None, "", "None"):
        return Decimal("0")
    return Decimal(str(value))


def q2(value: Decimal) -> str:
    return str(value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


def format_int(value: Decimal) -> str:
    return str(int(value.quantize(Decimal("1"), rounding=ROUND_HALF_UP)))


def summarize_group(rows: list[dict[str, str]]) -> tuple[Decimal, Decimal, Decimal]:
    impression = Decimal("0")
    revenue = Decimal("0")
    for row in rows:
        impression += to_decimal(row["impression"])
        revenue += to_decimal(row["revenue"])
    ecpm = Decimal("0") if impression == 0 else (revenue * Decimal("1000") / impression)
    return impression, revenue, ecpm


def build_daily_rows(base_rows: list[dict[str, str]]) -> list[dict[str, str]]:
    filtered = []
    for row in base_rows:
        if row["network"] == TRADPLUS_NETWORK and row["source"] != TRADPLUS_SOURCE:
            continue
        filtered.append(row)

    bucket: dict[tuple[str, str, str], list[dict[str, str]]] = defaultdict(list)
    for row in filtered:
        bucket[(row["date"], row["product"], row["ad_format"])].append(row)

    output = []
    for (date, product, ad_format), rows in sorted(bucket.items()):
        a_rows = [row for row in rows if row["experiment_group"] == GROUP_A]
        b_rows = [row for row in rows if row["experiment_group"] == GROUP_B]
        imp_a, rev_a, ecpm_a = summarize_group(a_rows)
        imp_b, rev_b, ecpm_b = summarize_group(b_rows)
        output.append(
            {
                "date": date,
                "product": product,
                "ad_format": ad_format,
                "imp_gap": format_int(imp_b - imp_a),
                "rev_gap": q2(rev_b - rev_a),
                "ecpm_gap": q2(ecpm_b - ecpm_a),
                "imp_a": format_int(imp_a),
                "rev_a": q2(rev_a),
                "ecpm_a": q2(ecpm_a),
                "imp_b": format_int(imp_b),
                "rev_b": q2(rev_b),
                "ecpm_b": q2(ecpm_b),
            }
        )
    return output


def build_total_rows(daily_rows: list[dict[str, str]]) -> list[dict[str, str]]:
    bucket: dict[tuple[str, str], dict[str, Decimal]] = defaultdict(
        lambda: defaultdict(Decimal)
    )
    for row in daily_rows:
        key = (row["product"], row["ad_format"])
        bucket[key]["imp_a"] += to_decimal(row["imp_a"])
        bucket[key]["rev_a"] += to_decimal(row["rev_a"])
        bucket[key]["imp_b"] += to_decimal(row["imp_b"])
        bucket[key]["rev_b"] += to_decimal(row["rev_b"])

    output = []
    for (product, ad_format), metrics in sorted(bucket.items()):
        imp_a = metrics["imp_a"]
        rev_a = metrics["rev_a"]
        imp_b = metrics["imp_b"]
        rev_b = metrics["rev_b"]
        ecpm_a = Decimal("0") if imp_a == 0 else (rev_a * Decimal("1000") / imp_a)
        ecpm_b = Decimal("0") if imp_b == 0 else (rev_b * Decimal("1000") / imp_b)
        output.append(
            {
                "product": product,
                "ad_format": ad_format,
                "imp_gap": format_int(imp_b - imp_a),
                "rev_gap": q2(rev_b - rev_a),
                "ecpm_gap": q2(ecpm_b - ecpm_a),
                "imp_a": format_int(imp_a),
                "rev_a": q2(rev_a),
                "ecpm_a": q2(ecpm_a),
                "imp_b": format_int(imp_b),
                "rev_b": q2(rev_b),
                "ecpm_b": q2(ecpm_b),
            }
        )
    return output


def build_tradplus_rows(base_rows: list[dict[str, str]]) -> list[dict[str, str]]:
    bucket: dict[tuple[str, str, str, str], dict[str, Decimal]] = defaultdict(
        lambda: defaultdict(Decimal)
    )
    for row in base_rows:
        if row["network"] != TRADPLUS_NETWORK:
            continue
        key = (row["date"], row["product"], row["ad_format"], row["experiment_group"])
        suffix = "raw" if row["source"] == TRADPLUS_SOURCE else "max"
        bucket[key][f"imp_{suffix}"] += to_decimal(row["impression"])
        bucket[key][f"rev_{suffix}"] += to_decimal(row["revenue"])

    output = []
    for (date, product, ad_format, experiment_group), metrics in sorted(bucket.items()):
        output.append(
            {
                "date": date,
                "product": product,
                "ad_format": ad_format,
                "experiment_group": experiment_group,
                "imp_max_tradplus": format_int(metrics["imp_max"]),
                "rev_max_tradplus": q2(metrics["rev_max"]),
                "imp_tradplus_raw": format_int(metrics["imp_raw"]),
                "rev_tradplus_raw": q2(metrics["rev_raw"]),
            }
        )
    return output


def markdown_table(rows: list[dict[str, str]], columns: list[tuple[str, str]]) -> str:
    if not rows:
        return "_no data_"
    header = "| " + " | ".join(title for _, title in columns) + " |"
    align = []
    for key, _ in columns:
        if key in {"date", "product", "ad_format", "experiment_group", "check_type", "unit_id"}:
            align.append("---")
        else:
            align.append("---:")
    body = [
        "| " + " | ".join(row.get(key, "") for key, _ in columns) + " |"
        for row in rows
    ]
    return "\n".join([header, "| " + " | ".join(align) + " |", *body])


def build_markdown(
    daily_rows: list[dict[str, str]],
    total_rows: list[dict[str, str]],
    tradplus_rows: list[dict[str, str]],
    diag_rows: list[dict[str, str]],
) -> str:
    sections = [
        "# car_jam dual-platform TradPlus ADX AB report",
        "",
        "## Notes",
        "- execution_path = @@local_bigquery@@",
        "- Range: @@2026-01-27@@ to query run day (UTC).",
        "- GAP = @@have_tp_adx - no_tp_adx@@, i.e. @@B - A@@.",
        "- Final totals keep only external TradPlus rows for @@TradPlus_ADX@@.",
        "- MAX-derived @@TradPlus_ADX@@ is shown only in the comparison table; external TradPlus is assigned to @@have_tp_adx@@.",
        "",
        "## Total",
        markdown_table(
            total_rows,
            [
                ("product", "product"),
                ("ad_format", "ad_format"),
                ("imp_gap", "impression_gap"),
                ("rev_gap", "revenue_gap"),
                ("ecpm_gap", "ecpm_gap"),
                ("imp_a", "impression -> no_tp_adx"),
                ("imp_b", "impression -> have_tp_adx"),
                ("rev_a", "revenue -> no_tp_adx"),
                ("rev_b", "revenue -> have_tp_adx"),
                ("ecpm_a", "ecpm -> no_tp_adx"),
                ("ecpm_b", "ecpm -> have_tp_adx"),
            ],
        ),
        "",
        "## Daily",
        markdown_table(
            daily_rows,
            [
                ("date", "date"),
                ("product", "product"),
                ("ad_format", "ad_format"),
                ("imp_gap", "impression_gap"),
                ("rev_gap", "revenue_gap"),
                ("ecpm_gap", "ecpm_gap"),
                ("imp_a", "impression -> no_tp_adx"),
                ("imp_b", "impression -> have_tp_adx"),
                ("rev_a", "revenue -> no_tp_adx"),
                ("rev_b", "revenue -> have_tp_adx"),
                ("ecpm_a", "ecpm -> no_tp_adx"),
                ("ecpm_b", "ecpm -> have_tp_adx"),
            ],
        ),
        "",
        "## TradPlus comparison",
        markdown_table(
            tradplus_rows,
            [
                ("date", "date"),
                ("product", "product"),
                ("ad_format", "ad_format"),
                ("experiment_group", "experiment_group"),
                ("imp_max_tradplus", "max_tradplus_imp"),
                ("rev_max_tradplus", "max_tradplus_rev"),
                ("imp_tradplus_raw", "tp_external_imp"),
                ("rev_tradplus_raw", "tp_external_rev"),
            ],
        ),
        "",
        "## Diagnostics",
        markdown_table(
            diag_rows,
            [
                ("check_type", "check_type"),
                ("product", "product"),
                ("unit_id", "unit_id"),
                ("metric_1", "metric_1"),
                ("metric_2", "metric_2"),
            ],
        ),
    ]
    return "\n".join(sections).replace("@@", "`") + "\n"


def main() -> int:
    args = parse_args()

    if not args.skip_query:
        run_bq_query(args.base_sql, args.base_csv)
        run_bq_query(args.diag_sql, args.diag_csv)

    base_rows = read_csv_rows(args.base_csv)
    diag_rows = read_csv_rows(args.diag_csv)
    daily_rows = build_daily_rows(base_rows)
    total_rows = build_total_rows(daily_rows)
    tradplus_rows = build_tradplus_rows(base_rows)
    markdown = build_markdown(daily_rows, total_rows, tradplus_rows, diag_rows)
    args.markdown.write_text(markdown, encoding="utf-8", newline="")

    print("execution_path = local_bigquery")
    print(f"base csv : {args.base_csv}")
    print(f"diag csv : {args.diag_csv}")
    print(f"markdown : {args.markdown}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
