import json
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Dict, List, Optional


ROOT = Path(r"D:\Work\Amber\is_adx\outputs\adhoc_tmp")

IOS_RAW = ROOT / "ios_sheet_raw.json"
ANDROID_RAW = ROOT / "android_sheet_raw.json"
OUTPUT_JSON = ROOT / "is_phase_summary.json"
OUTPUT_MD = ROOT / "is_phase_summary.md"

DATE_1899_12_30 = date(1899, 12, 30)
FORMATS = ("BANNER", "INTER", "REWARD")
PERIODS = ("0918-1218", "0101-last")


@dataclass
class DailyRow:
    dt: date
    fmt: str
    imp_no: float
    imp_have: float
    rev_no: float
    rev_have: float
    ecpm_no: float
    ecpm_have: float


def excel_serial_to_date(value: object) -> Optional[date]:
    if isinstance(value, (int, float)):
        return DATE_1899_12_30 + timedelta(days=int(value))
    return None


def to_float(value: object) -> Optional[float]:
    if value is None or value == "":
        return None
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(str(value).replace(",", ""))
    except ValueError:
        return None


def load_values(path: Path) -> List[List[object]]:
    raw = path.read_bytes()
    if raw.startswith(b"\xff\xfe") or raw.startswith(b"\xfe\xff"):
        text = raw.decode("utf-16")
    else:
        text = raw.decode("utf-8")
    data = json.loads(text)
    return data["data"]["valueRange"]["values"]


def extract_daily_rows(values: List[List[object]]) -> List[DailyRow]:
    rows: List[DailyRow] = []
    current_format: Optional[str] = None
    for row in values[2:]:
        if not row:
            continue
        head = row[0] if len(row) > 0 else None
        if isinstance(head, str) and head.upper() in FORMATS:
            current_format = head.upper()
            continue
        if current_format is None:
            continue
        dt = excel_serial_to_date(head)
        if dt is None:
            continue
        imp_no = to_float(row[5] if len(row) > 5 else None)
        imp_have = to_float(row[6] if len(row) > 6 else None)
        rev_no = to_float(row[7] if len(row) > 7 else None)
        rev_have = to_float(row[8] if len(row) > 8 else None)
        ecpm_no = to_float(row[9] if len(row) > 9 else None)
        ecpm_have = to_float(row[10] if len(row) > 10 else None)
        if None in (imp_no, imp_have, rev_no, rev_have, ecpm_no, ecpm_have):
            continue
        rows.append(
            DailyRow(
                dt=dt,
                fmt=current_format,
                imp_no=imp_no,
                imp_have=imp_have,
                rev_no=rev_no,
                rev_have=rev_have,
                ecpm_no=ecpm_no,
                ecpm_have=ecpm_have,
            )
        )
    return rows


def in_period(dt: date, period_name: str) -> bool:
    if period_name == "0918-1218":
        return date(2025, 9, 18) <= dt <= date(2025, 12, 18)
    if period_name == "0101-last":
        return dt >= date(2026, 1, 1)
    raise ValueError(period_name)


def ratio(have: float, no: float) -> Optional[float]:
    if no == 0:
        return None
    return have / no - 1.0


def aggregate(rows: List[DailyRow]) -> Dict[str, Dict[str, Dict[str, float]]]:
    result: Dict[str, Dict[str, Dict[str, float]]] = {}
    for period_name in PERIODS:
        period_bucket: Dict[str, Dict[str, float]] = {}
        for fmt in FORMATS:
            subset = sorted(
                [r for r in rows if r.fmt == fmt and in_period(r.dt, period_name)],
                key=lambda item: item.dt,
            )
            imp_no = sum(r.imp_no for r in subset)
            imp_have = sum(r.imp_have for r in subset)
            rev_no = sum(r.rev_no for r in subset)
            rev_have = sum(r.rev_have for r in subset)
            ecpm_no = (rev_no / imp_no * 1000.0) if imp_no else 0.0
            ecpm_have = (rev_have / imp_have * 1000.0) if imp_have else 0.0
            period_bucket[fmt] = {
                "date_count": len(subset),
                "start_date": subset[0].dt.isoformat() if subset else None,
                "end_date": subset[-1].dt.isoformat() if subset else None,
                "impression_no": imp_no,
                "impression_have": imp_have,
                "impression_gap": ratio(imp_have, imp_no),
                "revenue_no": rev_no,
                "revenue_have": rev_have,
                "revenue_gap": ratio(rev_have, rev_no),
                "ecpm_no": ecpm_no,
                "ecpm_have": ecpm_have,
                "ecpm_gap": ratio(ecpm_have, ecpm_no),
            }
        result[period_name] = period_bucket
    return result


def format_pct(value: Optional[float]) -> str:
    if value is None:
        return "-"
    return f"{value:.2%}"


def format_num(value: Optional[float], digits: int = 0) -> str:
    if value is None:
        return "-"
    return f"{value:,.{digits}f}"


def build_markdown(summary: Dict[str, Dict[str, Dict[str, Dict[str, float]]]]) -> str:
    parts: List[str] = []
    for period_name in ("0918-1218", "0101-last"):
        parts.append(f"## {period_name}")
        parts.append("")
        parts.append(
            "| Ad Format | 安卓 展示 no | 安卓 展示 have | 安卓 展示 GAP | 安卓 收入 no | 安卓 收入 have | 安卓 收入 GAP | 安卓 eCPM no | 安卓 eCPM have | 安卓 eCPM GAP | iOS 展示 no | iOS 展示 have | iOS 展示 GAP | iOS 收入 no | iOS 收入 have | iOS 收入 GAP | iOS eCPM no | iOS eCPM have | iOS eCPM GAP |"
        )
        parts.append(
            "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |"
        )
        for fmt in FORMATS:
            android = summary["android"][period_name][fmt]
            ios = summary["ios"][period_name][fmt]
            parts.append(
                "| {fmt} | {a_imp_no} | {a_imp_have} | {a_imp_gap} | {a_rev_no} | {a_rev_have} | {a_rev_gap} | {a_ecpm_no} | {a_ecpm_have} | {a_ecpm_gap} | {i_imp_no} | {i_imp_have} | {i_imp_gap} | {i_rev_no} | {i_rev_have} | {i_rev_gap} | {i_ecpm_no} | {i_ecpm_have} | {i_ecpm_gap} |".format(
                    fmt=fmt,
                    a_imp_no=format_num(android["impression_no"]),
                    a_imp_have=format_num(android["impression_have"]),
                    a_imp_gap=format_pct(android["impression_gap"]),
                    a_rev_no=format_num(android["revenue_no"], 6),
                    a_rev_have=format_num(android["revenue_have"], 6),
                    a_rev_gap=format_pct(android["revenue_gap"]),
                    a_ecpm_no=format_num(android["ecpm_no"], 6),
                    a_ecpm_have=format_num(android["ecpm_have"], 6),
                    a_ecpm_gap=format_pct(android["ecpm_gap"]),
                    i_imp_no=format_num(ios["impression_no"]),
                    i_imp_have=format_num(ios["impression_have"]),
                    i_imp_gap=format_pct(ios["impression_gap"]),
                    i_rev_no=format_num(ios["revenue_no"], 6),
                    i_rev_have=format_num(ios["revenue_have"], 6),
                    i_rev_gap=format_pct(ios["revenue_gap"]),
                    i_ecpm_no=format_num(ios["ecpm_no"], 6),
                    i_ecpm_have=format_num(ios["ecpm_have"], 6),
                    i_ecpm_gap=format_pct(ios["ecpm_gap"]),
                )
            )
        parts.append("")
    return "\n".join(parts)


def main() -> None:
    ios_rows = extract_daily_rows(load_values(IOS_RAW))
    android_rows = extract_daily_rows(load_values(ANDROID_RAW))
    ios_rows.sort(key=lambda item: (item.fmt, item.dt))
    android_rows.sort(key=lambda item: (item.fmt, item.dt))
    summary = {
        "ios": aggregate(ios_rows),
        "android": aggregate(android_rows),
    }
    OUTPUT_JSON.write_text(
        json.dumps(
            {
                "ios_daily_count": len(ios_rows),
                "android_daily_count": len(android_rows),
                "summary": summary,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    OUTPUT_MD.write_text(build_markdown(summary), encoding="utf-8")


if __name__ == "__main__":
    main()
