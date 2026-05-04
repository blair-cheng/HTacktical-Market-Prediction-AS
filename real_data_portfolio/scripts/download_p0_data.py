#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import socket
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime
from datetime import timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen
import xml.etree.ElementTree as ET

import pandas as pd


DEFAULT_ASSETS = ["SPY", "QQQ", "IWM", "TLT", "IEF", "SHY", "GLD", "UUP"]
KNOWN_ASSET_START_DATES = {
    "UUP": "2007-03-01",
}
DEFAULT_FRED_SERIES = {
    "DFF": "fed_funds",
    "DGS3MO": "yield_3m",
    "DGS2": "yield_2y",
    "DGS10": "yield_10y",
    "T10Y2Y": "yield_curve_10y_2y",
    "CPIAUCSL": "cpi",
    "UNRATE": "unrate",
}

FRED_API_URL = "https://api.stlouisfed.org/fred/series/observations"
FRED_CSV_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv"
TREASURY_YIELD_XML_URL = "https://home.treasury.gov/resource-center/data-chart-center/interest-rates/pages/xml"
VIX_CSV_URL = "https://cdn.cboe.com/api/global/us_indices/daily_prices/VIX_History.csv"
FEAR_GREED_URL = "https://production.dataviz.cnn.io/index/fearandgreed/graphdata"
STOOQ_DAILY_URL = "https://stooq.com/q/d/l/"
TIINGO_DAILY_URL = "https://api.tiingo.com/tiingo/daily/{symbol}/prices"
YAHOO_CHART_URLS = [
    "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}",
    "https://query2.finance.yahoo.com/v8/finance/chart/{symbol}",
]


@dataclass(frozen=True)
class DownloadConfig:
    start: str
    end: str
    data_dir: Path
    assets: list[str]
    price_source: str = "yahoo"
    stooq_api_key: str | None = None
    tiingo_api_key: str | None = None
    fred_api_key: str | None = None
    price_pause: float = 0.5
    skip_sources: tuple[str, ...] = ()
    force: bool = False
    strict: bool = False


def main() -> int:
    args = parse_args()
    config = DownloadConfig(
        start=normalize_date(args.start),
        end=normalize_date(args.end),
        data_dir=Path(args.data_dir),
        assets=[a.strip().upper() for a in args.assets.split(",") if a.strip()],
        price_source=args.price_source,
        stooq_api_key=args.stooq_api_key,
        tiingo_api_key=args.tiingo_api_key or os.getenv("TIINGO_API_KEY"),
        fred_api_key=args.fred_api_key or os.getenv("FRED_API_KEY"),
        price_pause=args.price_pause,
        skip_sources=parse_skip_sources(args.skip_sources),
        force=args.force,
        strict=args.strict,
    )

    raw_dir = config.data_dir / "raw"
    processed_dir = config.data_dir / "processed"
    raw_dir.mkdir(parents=True, exist_ok=True)
    processed_dir.mkdir(parents=True, exist_ok=True)

    download_tasks = [
        ("prices", lambda: download_prices(config, raw_dir / "prices")),
        ("fred", lambda: download_fred(config, raw_dir / "fred")),
        ("treasury_yields", lambda: download_treasury_yields(config, raw_dir / "treasury_yields")),
        ("bls_cpi", lambda: load_local_bls_cpi(raw_dir / "bls")),
        ("vix", lambda: download_vix(config, raw_dir / "vix")),
        ("fear_greed", lambda: download_fear_greed(config, raw_dir / "fear_greed")),
    ]

    frames = [
        safe_download(name, config, func)
        for name, func in download_tasks
        if name not in config.skip_sources
    ]
    frames = [frame for frame in frames if frame is not None and not frame.empty]

    market_state = merge_daily(frames, config.start, config.end)
    out_path = processed_dir / "market_state.csv"
    market_state.to_csv(out_path, index=False)

    print(f"wrote {out_path}")
    print(f"rows={len(market_state)} cols={len(market_state.columns)}")
    print(",".join(market_state.columns))
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download P0 real-portfolio data and build market_state.csv")
    parser.add_argument("--start", default="2005-01-01", help="Start date, YYYY-MM-DD")
    parser.add_argument("--end", default=date.today().isoformat(), help="End date, YYYY-MM-DD")
    parser.add_argument("--data-dir", default="data", help="Output data directory")
    parser.add_argument("--assets", default=",".join(DEFAULT_ASSETS), help="Comma-separated asset tickers")
    parser.add_argument(
        "--price-source",
        choices=["yahoo", "stooq", "tiingo"],
        default="yahoo",
        help="Price source. Use tiingo for stable research runs.",
    )
    parser.add_argument("--stooq-api-key", default=None, help="Required if --price-source stooq")
    parser.add_argument("--tiingo-api-key", default=None, help="Required if --price-source tiingo; or set TIINGO_API_KEY")
    parser.add_argument("--fred-api-key", default=None, help="Optional official FRED API key; or set FRED_API_KEY")
    parser.add_argument("--price-pause", type=float, default=0.5, help="Seconds to pause between price ticker requests")
    parser.add_argument(
        "--skip-sources",
        default="",
        help="Comma-separated sources to skip: prices,fred,treasury_yields,bls_cpi,vix,fear_greed",
    )
    parser.add_argument("--force", action="store_true", help="Refetch raw files even if cached")
    parser.add_argument("--strict", action="store_true", help="Fail if any source fails")
    return parser.parse_args()


def safe_download(name: str, config: DownloadConfig, func) -> pd.DataFrame | None:
    print(f"downloading {name}...", file=sys.stderr)
    try:
        frame = func()
    except Exception as exc:
        if name == "prices":
            raise
        if config.strict:
            raise
        print(f"warning: {name} failed: {exc}", file=sys.stderr)
        return None
    print(f"downloaded {name}: rows={len(frame)} cols={len(frame.columns)}", file=sys.stderr)
    return frame


def parse_skip_sources(value: str) -> tuple[str, ...]:
    allowed = {"prices", "fred", "treasury_yields", "bls_cpi", "vix", "fear_greed"}
    sources = tuple(s.strip() for s in value.split(",") if s.strip())
    unknown = [source for source in sources if source not in allowed]
    if unknown:
        raise ValueError(f"Unknown skip source(s): {unknown}. Allowed: {sorted(allowed)}")
    return sources


def download_prices(config: DownloadConfig, raw_dir: Path) -> pd.DataFrame:
    if config.price_source == "yahoo":
        return download_yahoo_prices(config, raw_dir.parent / "prices_yahoo")
    if config.price_source == "tiingo":
        return download_tiingo_prices(config, raw_dir.parent / "prices_tiingo")
    return download_stooq_prices(config, raw_dir)


def download_stooq_prices(config: DownloadConfig, raw_dir: Path) -> pd.DataFrame:
    if not config.stooq_api_key:
        raise ValueError("Stooq now requires an API key. Pass --stooq-api-key or use --price-source yahoo.")

    raw_dir.mkdir(parents=True, exist_ok=True)
    frames = []
    for asset in config.assets:
        symbol = f"{asset.lower()}.us"
        raw_path = raw_dir / f"{asset}.csv"
        if config.force or not raw_path.exists():
            params = {
                "s": symbol,
                "i": "d",
                "d1": config.start.replace("-", ""),
                "d2": config.end.replace("-", ""),
                "apikey": config.stooq_api_key,
            }
            url = f"{STOOQ_DAILY_URL}?{urlencode(params)}"
            raw_path.write_bytes(fetch_bytes(url, timeout=12, attempts=1))
            time.sleep(config.price_pause)
        df = pd.read_csv(raw_path)
        if df.empty:
            raise ValueError(f"No Stooq data for {asset}")
        df = df.rename(columns={"Date": "date", "Close": asset})
        frames.append(df[["date", asset]])
    return merge_on_date(frames)


def download_yahoo_prices(config: DownloadConfig, raw_dir: Path) -> pd.DataFrame:
    raw_dir.mkdir(parents=True, exist_ok=True)
    frames = []
    for asset in config.assets:
        raw_path = raw_dir / f"{asset}.json"
        if config.force or not raw_path.exists():
            start_ts = unix_seconds(config.start)
            end_ts = unix_seconds((pd.Timestamp(config.end) + pd.Timedelta(days=1)).date().isoformat())
            params = {
                "period1": str(start_ts),
                "period2": str(end_ts),
                "interval": "1d",
                "events": "history",
                "includeAdjustedClose": "true",
            }
            last_error: Exception | None = None
            for template in YAHOO_CHART_URLS:
                url = f"{template.format(symbol=asset)}?{urlencode(params)}"
                try:
                    raw_path.write_bytes(fetch_bytes(url, accept="application/json", timeout=20, attempts=3))
                    last_error = None
                    break
                except Exception as exc:
                    last_error = exc
            if last_error is not None:
                raise last_error
            time.sleep(config.price_pause)
        payload = json.loads(raw_path.read_text())
        result = payload.get("chart", {}).get("result", [])
        if not result:
            error = payload.get("chart", {}).get("error")
            raise ValueError(f"No Yahoo data for {asset}: {error}")
        item = result[0]
        timestamps = item.get("timestamp", [])
        adjclose = item.get("indicators", {}).get("adjclose", [{}])[0].get("adjclose")
        close = item.get("indicators", {}).get("quote", [{}])[0].get("close")
        values = adjclose if adjclose is not None else close
        if not timestamps or values is None:
            raise ValueError(f"Yahoo payload missing timestamps/prices for {asset}")
        df = pd.DataFrame(
            {
                "date": [datetime.fromtimestamp(ts, tz=timezone.utc).date().isoformat() for ts in timestamps],
                asset: values,
            }
        )
        df[asset] = pd.to_numeric(df[asset], errors="coerce")
        frames.append(df)
    return merge_on_date(frames)


def download_tiingo_prices(config: DownloadConfig, raw_dir: Path) -> pd.DataFrame:
    raw_dir.mkdir(parents=True, exist_ok=True)
    frames = []
    for asset in config.assets:
        raw_path = raw_dir / f"{asset}.csv"
        required_start = max(config.start, KNOWN_ASSET_START_DATES.get(asset, config.start))
        needs_fetch = config.force or not raw_path.exists() or not cached_csv_covers(raw_path, "date", required_start, config.end)
        if needs_fetch:
            if not config.tiingo_api_key:
                raise ValueError("Tiingo requires an API token to fetch missing files. Cached files can be read without it.")
            params = {
                "startDate": config.start,
                "endDate": config.end,
                "format": "csv",
                "token": config.tiingo_api_key,
            }
            url = f"{TIINGO_DAILY_URL.format(symbol=asset.lower())}?{urlencode(params)}"
            raw_path.write_bytes(fetch_bytes(url, timeout=30, attempts=2))
            time.sleep(config.price_pause)
        df = pd.read_csv(raw_path)
        if df.empty:
            raise ValueError(f"No Tiingo data for {asset}")
        price_col = "adjClose" if "adjClose" in df.columns else "close"
        if "date" not in df.columns or price_col not in df.columns:
            raise ValueError(f"Tiingo payload missing date/{price_col} for {asset}")
        out = df[["date", price_col]].copy()
        out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.date.astype("string")
        out[asset] = pd.to_numeric(out[price_col], errors="coerce")
        frames.append(out[["date", asset]])
    return merge_on_date(frames)


def cached_csv_covers(path: Path, date_col: str, start: str, end: str) -> bool:
    try:
        df = pd.read_csv(path, usecols=[date_col])
    except Exception:
        return False
    if df.empty:
        return False
    dates = pd.to_datetime(df[date_col], errors="coerce").dropna()
    if dates.empty:
        return False
    requested_start = pd.Timestamp(start)
    requested_end = pd.Timestamp(end)
    starts_on_time = dates.min() <= requested_start + pd.Timedelta(days=7)
    ends_on_time = dates.max() >= requested_end
    return starts_on_time and ends_on_time


def download_fred(config: DownloadConfig, raw_dir: Path) -> pd.DataFrame:
    raw_dir.mkdir(parents=True, exist_ok=True)
    frames = []
    for series_id, column in DEFAULT_FRED_SERIES.items():
        raw_path = raw_dir / f"{series_id}.{'json' if config.fred_api_key else 'csv'}"
        if config.force or not raw_path.exists():
            if config.fred_api_key:
                params = {
                    "series_id": series_id,
                    "api_key": config.fred_api_key,
                    "file_type": "json",
                    "observation_start": config.start,
                    "observation_end": config.end,
                }
                url = f"{FRED_API_URL}?{urlencode(params)}"
                raw_path.write_bytes(fetch_bytes(url, accept="application/json", timeout=30, attempts=2))
            else:
                url = f"{FRED_CSV_URL}?{urlencode({'id': series_id, 'cosd': config.start, 'coed': config.end})}"
                raw_path.write_bytes(fetch_bytes(url, timeout=35, attempts=2))

        if config.fred_api_key:
            payload = json.loads(raw_path.read_text())
            observations = payload.get("observations", [])
            df = pd.DataFrame(
                {
                    "date": [obs.get("date") for obs in observations],
                    column: [obs.get("value") for obs in observations],
                }
            )
        else:
            df = pd.read_csv(raw_path)
            value_col = series_id if series_id in df.columns else df.columns[-1]
            df = df.rename(columns={"observation_date": "date", "DATE": "date", value_col: column})
        if df.empty:
            raise ValueError(f"No FRED data for {series_id}")
        df[column] = pd.to_numeric(df[column].replace(".", pd.NA), errors="coerce")
        frames.append(df[["date", column]])
    return merge_on_date(frames)


def download_treasury_yields(config: DownloadConfig, raw_dir: Path) -> pd.DataFrame:
    raw_dir.mkdir(parents=True, exist_ok=True)
    frames = []
    for year in range(pd.Timestamp(config.start).year, pd.Timestamp(config.end).year + 1):
        raw_path = raw_dir / f"daily_treasury_yield_curve_{year}.xml"
        if config.force or not raw_path.exists():
            params = {"data": "daily_treasury_yield_curve", "field_tdr_date_value": str(year)}
            url = f"{TREASURY_YIELD_XML_URL}?{urlencode(params)}"
            raw_path.write_bytes(fetch_bytes(url, accept="application/xml", timeout=30, attempts=2))
        frame = parse_treasury_yield_xml(raw_path.read_bytes())
        if not frame.empty:
            frames.append(frame)
    if not frames:
        raise ValueError("No Treasury yield data downloaded")

    out = pd.concat(frames, ignore_index=True)
    out = out.drop_duplicates("date", keep="last").sort_values("date")
    out["yield_curve_10y_2y"] = out["yield_10y"] - out["yield_2y"]
    return out


def parse_treasury_yield_xml(data: bytes) -> pd.DataFrame:
    root = ET.fromstring(data)
    rows = []
    for elem in root.iter():
        if local_name(elem.tag) != "properties":
            continue
        values = {local_name(child.tag): child.text for child in list(elem)}
        raw_date = values.get("NEW_DATE") or values.get("DATE")
        if not raw_date:
            continue
        row = {
            "date": pd.to_datetime(raw_date, errors="coerce").date().isoformat(),
            "yield_3m": values.get("BC_3MONTH"),
            "yield_2y": values.get("BC_2YEAR"),
            "yield_5y": values.get("BC_5YEAR"),
            "yield_10y": values.get("BC_10YEAR"),
            "yield_30y": values.get("BC_30YEAR"),
        }
        rows.append(row)
    out = pd.DataFrame(rows)
    if out.empty:
        return out
    for col in [c for c in out.columns if c != "date"]:
        out[col] = pd.to_numeric(out[col], errors="coerce")
    return out


def local_name(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def load_local_bls_cpi(raw_dir: Path) -> pd.DataFrame:
    path = raw_dir / "cpi.csv"
    if not path.exists():
        raise FileNotFoundError(f"Local BLS CPI file not found: {path}")
    df = pd.read_csv(path)
    required = {"date", "cpi"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"BLS CPI file missing columns: {sorted(missing)}")
    out = df[["date", "cpi"]].copy()
    out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.date.astype("string")
    out["cpi"] = pd.to_numeric(out["cpi"], errors="coerce")
    return out.dropna(subset=["date", "cpi"]).drop_duplicates("date", keep="last")


def download_vix(config: DownloadConfig, raw_dir: Path) -> pd.DataFrame:
    raw_dir.mkdir(parents=True, exist_ok=True)
    raw_path = raw_dir / "VIX_History.csv"
    if config.force or not raw_path.exists():
        raw_path.write_bytes(fetch_bytes(VIX_CSV_URL))
    df = pd.read_csv(raw_path)
    date_col = "DATE" if "DATE" in df.columns else "Date"
    close_col = "CLOSE" if "CLOSE" in df.columns else "Close"
    df = df.rename(columns={date_col: "date", close_col: "VIX"})
    df["VIX"] = pd.to_numeric(df["VIX"], errors="coerce")
    return df[["date", "VIX"]]


def download_fear_greed(config: DownloadConfig, raw_dir: Path) -> pd.DataFrame:
    raw_dir.mkdir(parents=True, exist_ok=True)
    raw_path = raw_dir / "cnn_fear_greed.json"
    if config.force or not raw_path.exists():
        start_dates = [config.start]
        if config.start < "2024-01-01":
            start_dates.append("2024-01-01")
        last_error: Exception | None = None
        for start_date in start_dates:
            url = f"{FEAR_GREED_URL}/{start_date}"
            try:
                raw_path.write_bytes(fetch_bytes(url, accept="application/json"))
                last_error = None
                break
            except Exception as exc:
                last_error = exc
                print(f"warning: Fear & Greed fetch failed for {start_date}: {exc}", file=sys.stderr)
        if last_error is not None:
            raise last_error

    payload = json.loads(raw_path.read_text())
    rows = parse_fear_greed_history(payload)
    if not rows:
        print("warning: could not parse Fear & Greed history; continuing without it", file=sys.stderr)
        return pd.DataFrame(columns=["date", "fear_greed_score"])

    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date.astype("string")
    df["fear_greed_score"] = pd.to_numeric(df["fear_greed_score"], errors="coerce")
    df = df.dropna(subset=["date", "fear_greed_score"]).drop_duplicates("date", keep="last")
    df = df.sort_values("date")
    return df[["date", "fear_greed_score"]]


def parse_fear_greed_history(payload: Any) -> list[dict[str, Any]]:
    candidates = [
        payload.get("fear_and_greed_historical") if isinstance(payload, dict) else None,
        payload.get("fear_and_greed_history") if isinstance(payload, dict) else None,
        payload.get("history") if isinstance(payload, dict) else None,
    ]

    rows: list[dict[str, Any]] = []
    for candidate in candidates:
        rows.extend(parse_history_candidate(candidate))
    if rows:
        return rows

    # Some CNN payloads are nested by indicator. Walk the JSON and accept any list
    # of dicts that looks like daily score history.
    for candidate in walk_lists(payload):
        parsed = parse_history_candidate(candidate)
        if len(parsed) > len(rows):
            rows = parsed
    return rows


def parse_history_candidate(candidate: Any) -> list[dict[str, Any]]:
    if isinstance(candidate, dict) and "data" in candidate:
        candidate = candidate["data"]
    if not isinstance(candidate, list):
        return []

    rows = []
    for item in candidate:
        if not isinstance(item, dict):
            continue
        raw_date = first_present(item, ["date", "x", "timestamp", "asOfDate", "as_of_date"])
        raw_score = first_present(item, ["score", "y", "value", "fearGreed", "fear_greed"])
        parsed_date = parse_any_date(raw_date)
        if parsed_date is None or raw_score is None:
            continue
        rows.append({"date": parsed_date, "fear_greed_score": raw_score})
    return rows


def walk_lists(value: Any) -> list[list[Any]]:
    found = []
    if isinstance(value, list):
        found.append(value)
        for item in value:
            found.extend(walk_lists(item))
    elif isinstance(value, dict):
        for item in value.values():
            found.extend(walk_lists(item))
    return found


def first_present(item: dict[str, Any], keys: list[str]) -> Any:
    for key in keys:
        if key in item and item[key] not in (None, ""):
            return item[key]
    return None


def parse_any_date(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        # CNN commonly uses millisecond Unix timestamps.
        if value > 10_000_000_000:
            value = value / 1000
        return datetime.utcfromtimestamp(value).date().isoformat()
    text = str(value)
    try:
        return pd.to_datetime(text, utc=True).date().isoformat()
    except Exception:
        return None


def merge_daily(frames: list[pd.DataFrame], start: str, end: str) -> pd.DataFrame:
    cleaned = []
    for df in frames:
        if df.empty:
            continue
        copy = df.copy()
        copy["date"] = pd.to_datetime(copy["date"], errors="coerce")
        copy = copy.dropna(subset=["date"]).sort_values("date")
        cleaned.append(copy)
    if not cleaned:
        raise ValueError("No data downloaded")

    calendar = price_calendar(cleaned)
    if calendar is not None:
        out = calendar
        for frame in cleaned:
            cols = [c for c in frame.columns if c != "date"]
            if cols:
                if set(DEFAULT_ASSETS).intersection(frame.columns):
                    out = out.merge(frame, on="date", how="left")
                else:
                    out = pd.merge_asof(
                        out.sort_values("date"),
                        frame.sort_values("date"),
                        on="date",
                        direction="backward",
                    )
    else:
        out = merge_on_date(cleaned)
    out = out[(out["date"] >= pd.Timestamp(start)) & (out["date"] <= pd.Timestamp(end))]
    out = out.sort_values("date").reset_index(drop=True)

    numeric_cols = [c for c in out.columns if c != "date"]
    out[numeric_cols] = out[numeric_cols].ffill()
    out["fear_greed_delta"] = out["fear_greed_score"].diff() if "fear_greed_score" in out.columns else pd.NA
    out["fear_greed_is_extreme_fear"] = (
        (out["fear_greed_score"] <= 25).astype("int64") if "fear_greed_score" in out.columns else 0
    )
    out["fear_greed_is_extreme_greed"] = (
        (out["fear_greed_score"] >= 75).astype("int64") if "fear_greed_score" in out.columns else 0
    )
    out["date"] = out["date"].dt.date.astype("string")
    return out


def price_calendar(frames: list[pd.DataFrame]) -> pd.DataFrame | None:
    asset_set = set(DEFAULT_ASSETS)
    for frame in frames:
        if asset_set.intersection(frame.columns):
            return frame[["date"]].drop_duplicates().sort_values("date").reset_index(drop=True)
    return None


def merge_on_date(frames: list[pd.DataFrame]) -> pd.DataFrame:
    out = frames[0].copy()
    for frame in frames[1:]:
        out = out.merge(frame, on="date", how="outer")
    return out


def fetch_bytes(url: str, accept: str = "text/csv", timeout: int = 30, attempts: int = 2) -> bytes:
    request = Request(
        url,
        headers={
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/121.0.0.0 Safari/537.36"
            ),
            "Accept": accept,
            "Referer": "https://www.cnn.com/markets/fear-and-greed",
            "Content-Type": accept,
        },
    )
    last_error: Exception | None = None
    for attempt in range(attempts):
        try:
            with urlopen(request, timeout=timeout) as response:
                return response.read()
        except HTTPError as exc:
            last_error = exc
            if exc.code in {429, 500, 502, 503, 504} and attempt < attempts - 1:
                time.sleep(2.0 * (attempt + 1))
                continue
            raise RuntimeError(f"HTTP {exc.code} for {url}") from exc
        except (TimeoutError, socket.timeout, URLError, OSError) as exc:
            last_error = exc
            if attempt < attempts - 1:
                time.sleep(1.5 * (attempt + 1))
                continue
    raise RuntimeError(f"Network error for {url}: {last_error}")


def normalize_date(value: str) -> str:
    return pd.to_datetime(value).date().isoformat()


def unix_seconds(value: str) -> int:
    dt = datetime.combine(pd.to_datetime(value).date(), datetime.min.time(), tzinfo=timezone.utc)
    return int(dt.timestamp())


if __name__ == "__main__":
    raise SystemExit(main())
