import requests
import pandas as pd
from datetime import datetime, timedelta
import pytz

BASE_URL = "https://data.cityofnewyork.us/resource/erm2-nwe9.json"
NYC_TIMEZONE = pytz.timezone("America/New_York")
DEFAULT_LIMIT = 50000


def get_time_window(hours: int = 24):
    """Return NYC local time, UTC time, and UTC ISO formatted timestamp for the rolling window."""
    now_nyc = datetime.now(NYC_TIMEZONE)
    start_nyc = now_nyc - timedelta(hours=hours)
    start_utc = start_nyc.astimezone(pytz.utc)
    start_iso = start_utc.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]  # milliseconds precision
    return start_nyc, start_utc, start_iso


def fetch_311_data(since_iso: str, limit: int = DEFAULT_LIMIT) -> pd.DataFrame:
    """Fetch 311 service requests created since given UTC ISO timestamp."""
    params = {
        "$limit": limit,
        "$where": f"created_date >= '{since_iso}'",
        "$order": "created_date DESC"
    }
    response = requests.get(BASE_URL, params=params)
    response.raise_for_status()
    data = response.json()
    return pd.DataFrame(data)


def clean_and_filter(df: pd.DataFrame, start_time_utc: datetime) -> pd.DataFrame:
    """Convert dates to timezone-aware, filter to after start_time_utc, add NYC tz column."""
    if df.empty:
        return df
    df["created_date"] = pd.to_datetime(df["created_date"], errors="coerce", utc=True)
    df = df[df["created_date"] >= start_time_utc]
    df["created_date_nyc"] = df["created_date"].dt.tz_convert(NYC_TIMEZONE)
    return df


def save_to_csv(df: pd.DataFrame, filename: str):
    """Save DataFrame to CSV."""
    df.to_csv(filename, index=False)
    print(f"✅ Data saved to {filename}")


def fetch_and_save_311_rolling(hours: int = 24, filename: str = None) -> pd.DataFrame:
    """
    Fetch 311 complaints for the last `hours` NYC hours.
    Optionally save to `filename`.
    """
    print(f"📡 Fetching 311 data from the last {hours} NYC hours...")
    start_nyc, start_utc, start_iso = get_time_window(hours=hours)
    print(f"🕒 NYC Time Window Start: {start_nyc.strftime('%Y-%m-%d %H:%M:%S')}")

    df_raw = fetch_311_data(since_iso=start_iso)
    df_clean = clean_and_filter(df_raw, start_time_utc=start_utc)

    print(f"✅ Retrieved {len(df_clean)} records.")
    if filename:
        save_to_csv(df_clean, filename)

    return df_clean
