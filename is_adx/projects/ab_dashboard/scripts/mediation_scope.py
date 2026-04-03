"""mediation report 的 unit 配置解析与渠道映射。"""

from __future__ import annotations

import csv
import re
from pathlib import Path
from typing import Any


UNIT_ID_RE = re.compile(r"\(([0-9a-zA-Z]+)\)\s*$")

DIRECT_CHANNEL_ALIASES: dict[tuple[str, str], tuple[str, str] | None] = {
    ("google bidding", "bidding"): ("bidding", "AdMob"),
    ("admob", "non-bidding"): ("waterfall", "AdMob"),
    ("meta bidding", "bidding"): ("bidding", "Facebook"),
    ("ironsource bidding", "bidding"): ("bidding", "ironSource"),
    ("unity bidding", "bidding"): ("bidding", "UnityAds"),
    ("ogury bidding", "bidding"): ("bidding", "OguryPresage"),
    ("amazon publisher service", "non-bidding"): ("waterfall", "AmazonPublisherServices"),
    ("google ad manager", "non-bidding"): ("waterfall", "GoogleAdManager"),
    ("bigo ads bidding", "bidding"): ("bidding", "BigoAds"),
    ("liftoff monetize bidding", "bidding"): ("bidding", "Vungle"),
    ("dt exchange bidding", "bidding"): ("bidding", "Fyber"),
    ("applovin exchange", "bidding"): ("bidding", "AppLovin"),
    ("applovin bidding", "bidding"): ("bidding", "AppLovin"),
    ("bidmachine bidding", "bidding"): ("bidding", "BidMachine"),
    ("chartboost bidding", "bidding"): ("bidding", "Chartboost"),
    ("inmobi bidding", "bidding"): ("bidding", "InMobi"),
    ("mintegral bidding", "bidding"): ("bidding", "Mintegral"),
    ("moloco bidding", "bidding"): ("bidding", "Moloco"),
    ("pangle bidding", "bidding"): ("bidding", "Pangle"),
    ("pubmatic", "bidding"): ("bidding", "PubMatic"),
    ("pubmatic", "non-bidding"): ("waterfall", "PubMatic"),
    ("yandex bidding", "bidding"): ("bidding", "Yandex"),
    ("line", "non-bidding"): ("waterfall", "LINE"),
    ("hyprmx", "non-bidding"): ("waterfall", "HyprMX"),
}

CUSTOM_CHANNEL_ALIASES: dict[str, tuple[str, str] | None] = {
    "isadxcustomadapter": ("waterfall", "IsAdxCustomAdapter"),
    "maticoocustomadapter": ("waterfall", "MaticooCustomAdapter"),
    "pubmatic": ("waterfall", "PubMatic"),
    "liftoff_custom": ("waterfall", "Vungle"),
    "tpadxcustomadapter": None,
}


def normalize_ad_format(raw: Any) -> str:
    value = str(raw or "").strip().lower()
    if value == "interstitial":
        return "interstitial"
    if value == "rewarded":
        return "rewarded"
    return ""


def parse_unit_name(raw_name: Any) -> tuple[str, str]:
    value = str(raw_name or "").strip()
    match = UNIT_ID_RE.search(value)
    if not match:
        return "", ""
    unit_id = match.group(1).strip()
    label = value[: match.start()].strip()
    return unit_id, label


def normalize_configured_channel(row: dict[str, Any]) -> tuple[str, str] | None:
    network = str(row.get("Network") or "").strip().lower()
    network_type = str(row.get("Network Type") or "").strip().lower()
    if not network or not network_type:
        return None
    if network == "custom network (sdk)":
        custom_name = str(row.get("Custom Network/Campaign Name") or "").strip().lower()
        if not custom_name:
            return None
        return CUSTOM_CHANNEL_ALIASES.get(custom_name)
    return DIRECT_CHANNEL_ALIASES.get((network, network_type))


def load_mediation_configuration(
    path: Path,
) -> tuple[dict[str, str], dict[tuple[str, str, str, str], set[str]]]:
    if not path.exists():
        return {}, {}

    unit_name_map: dict[str, str] = {}
    configured_units_by_channel: dict[tuple[str, str, str, str], set[str]] = {}
    with path.open("r", encoding="utf-8-sig", newline="") as file_obj:
        for row in csv.DictReader(file_obj):
            product = str(row.get("Package Name") or "").strip()
            ad_format = normalize_ad_format(row.get("Ad Type"))
            unit_id, unit_label = parse_unit_name(row.get("Ad Unit Name"))
            channel = normalize_configured_channel(row)
            if unit_id and unit_label:
                unit_name_map.setdefault(unit_id, unit_label)
            if not product or not ad_format or not unit_id or channel is None:
                continue
            key = (product, ad_format, channel[0], channel[1])
            configured_units_by_channel.setdefault(key, set()).add(unit_id)
    return unit_name_map, configured_units_by_channel
