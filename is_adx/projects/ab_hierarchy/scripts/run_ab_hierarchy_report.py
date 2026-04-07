from __future__ import annotations

import argparse
import csv
import sys
from collections import defaultdict
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path

from google.cloud import bigquery


ROOT = Path(__file__).resolve().parent.parent
SQL_PATH = ROOT / "sql" / "screw_puzzle_ab_hierarchy.sql"
DATA_DIR = ROOT / "data"
REPORTS_DIR = ROOT / "reports"

GROUP_A = "A"
GROUP_B = "B"


def today_tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run BQ query for screw_puzzle AB hierarchy report."
    )
    parser.add_argument("--sql", type=Path, default=SQL_PATH)
    parser.add_argument("--csv", type=Path, default=None)
    parser.add_argument("--markdown", type=Path, default=None)
    parser.add_argument(
        "--skip-query",
        action="store_true",
        help="Skip bq query and render from existing CSV.",
    )
    return parser.parse_args()


def run_bq_query(sql_path: Path, csv_path: Path) -> None:
    sql_text = sql_path.read_text(encoding="utf-8")
    client = bigquery.Client()
    rows = client.query(sql_text).result()
    schema_fields = [field.name for field in rows.schema]
    with csv_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(schema_fields)
        for row in rows:
            writer.writerow([row[col] for col in schema_fields])


def read_csv_rows(csv_path: Path) -> list[dict[str, str]]:
    with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
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


def filter_rows(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    """Keep source='ulp' all + source='max' where is_ironsource=false."""
    filtered = []
    for row in rows:
        source = row.get("source", "")
        is_iron = row.get("is_ironsource", "false").lower()
        if source == "ulp":
            filtered.append(row)
        elif source == "max" and is_iron in ("false", "0", ""):
            filtered.append(row)
    return filtered


def summarize_group(rows: list[dict[str, str]]) -> tuple[Decimal, Decimal, Decimal]:
    impression = Decimal("0")
    revenue = Decimal("0")
    for row in rows:
        impression += to_decimal(row.get("impression"))
        revenue += to_decimal(row.get("revenue"))
    ecpm = Decimal("0") if impression == 0 else (revenue * Decimal("1000") / impression)
    return impression, revenue, ecpm


def build_daily_rows(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    filtered = filter_rows(rows)
    bucket: dict[tuple[str, str, str], list[dict[str, str]]] = defaultdict(list)
    for row in filtered:
        bucket[(row["date"], row["product"], row["ad_format"])].append(row)

    output = []
    for (date, product, ad_format), grp_rows in sorted(bucket.items()):
        a_rows = [r for r in grp_rows if r["experiment_group"] == GROUP_A]
        b_rows = [r for r in grp_rows if r["experiment_group"] == GROUP_B]
        imp_a, rev_a, ecpm_a = summarize_group(a_rows)
        imp_b, rev_b, ecpm_b = summarize_group(b_rows)
        output.append({
            "date": date, "product": product, "ad_format": ad_format,
            "imp_gap": format_int(imp_b - imp_a),
            "rev_gap": q2(rev_b - rev_a),
            "ecpm_gap": q2(ecpm_b - ecpm_a),
            "imp_a": format_int(imp_a), "rev_a": q2(rev_a), "ecpm_a": q2(ecpm_a),
            "imp_b": format_int(imp_b), "rev_b": q2(rev_b), "ecpm_b": q2(ecpm_b),
        })
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
    for (product, ad_format), m in sorted(bucket.items()):
        imp_a, rev_a = m["imp_a"], m["rev_a"]
        imp_b, rev_b = m["imp_b"], m["rev_b"]
        ecpm_a = Decimal("0") if imp_a == 0 else (rev_a * 1000 / imp_a)
        ecpm_b = Decimal("0") if imp_b == 0 else (rev_b * 1000 / imp_b)
        output.append({
            "product": product, "ad_format": ad_format,
            "imp_gap": format_int(imp_b - imp_a),
            "rev_gap": q2(rev_b - rev_a),
            "ecpm_gap": q2(ecpm_b - ecpm_a),
            "imp_a": format_int(imp_a), "rev_a": q2(rev_a), "ecpm_a": q2(ecpm_a),
            "imp_b": format_int(imp_b), "rev_b": q2(rev_b), "ecpm_b": q2(ecpm_b),
        })
    return output


def markdown_table(rows: list[dict[str, str]], columns: list[tuple[str, str]]) -> str:
    if not rows:
        return "_no data_"
    header = "| " + " | ".join(title for _, title in columns) + " |"
    align = []
    for key, _ in columns:
        if key in {"date", "product", "ad_format"}:
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
) -> str:
    col_total = [
        ("product", "product"),
        ("ad_format", "ad_format"),
        ("imp_gap", "impression_gap"),
        ("rev_gap", "revenue_gap"),
        ("ecpm_gap", "ecpm_gap"),
        ("imp_a", "impression → A"),
        ("imp_b", "impression → B"),
        ("rev_a", "revenue → A"),
        ("rev_b", "revenue → B"),
        ("ecpm_a", "ecpm → A"),
        ("ecpm_b", "ecpm → B"),
    ]
    col_daily = [("date", "date")] + col_total

    sections = [
        "# screw_puzzle AB hierarchy report",
        "",
        "## Notes",
        "- Experiment: `lib_isx_group`, groups A / B",
        "- Start: UTC 2026-04-02 08:00 (Beijing 04-02 16:00)",
        "- Sources: MAX (all networks, excl ironSourceCustom) + ULP (IronSource)",
        "- GAP = B - A",
        "",
        "## Total",
        markdown_table(total_rows, col_total),
        "",
        "## Daily",
        markdown_table(daily_rows, col_daily),
    ]
    return "\n".join(sections) + "\n"


def main() -> int:
    args = parse_args()
    tag = today_tag()
    csv_path = args.csv or (DATA_DIR / f"ab_hierarchy_{tag}.csv")
    md_path = args.markdown or (REPORTS_DIR / f"ab_hierarchy_report_{tag}.md")

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    if not args.skip_query:
        print(f"Running BQ query: {args.sql}")
        run_bq_query(args.sql, csv_path)
        print(f"CSV saved: {csv_path}")

    raw_rows = read_csv_rows(csv_path)
    daily_rows = build_daily_rows(raw_rows)
    total_rows = build_total_rows(daily_rows)
    markdown = build_markdown(daily_rows, total_rows)
    md_path.write_text(markdown, encoding="utf-8", newline="")

    print(f"Markdown saved: {md_path}")
    print(f"Total rows: {len(raw_rows)}, Daily aggregated: {len(daily_rows)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
