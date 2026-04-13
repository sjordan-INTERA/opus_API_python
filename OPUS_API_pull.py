# -*- coding: utf-8 -*-
"""
API pull of the PID data
"""

import requests
import pandas as pd
from time import sleep
from datetime import datetime

def chunked(seq, size):
    """
    Chunk requests to be friendly :)
    """
    for i in range(0, len(seq), size):
        yield seq[i:i+size]


def fetch_latest_opus(pids, chunk_size=100):
    """
    Pull data via API
    """
    all_rows = []
    
    # Aggregate the PIDs into chunked API requests
    for i, chunk in enumerate(chunked(pids, chunk_size)):
        pid_str = ",".join(chunk)
        # API url
        url = f"https://geodesy.noaa.gov/api/opus/pid?pid={pid_str}"
        print(f"Request {i+1}... ({len(chunk)} PIDs)")
        
        # Send the request
        r = requests.get(url, timeout=60)
        r.raise_for_status()
        
        # Grab data
        data = r.json()
        
        # Append to records
        all_rows.extend(data)

        # be nice to NOAA servers!
        sleep(1)
    
    # Concat to one DataFrame
    df = pd.DataFrame(all_rows)

    # Ensure one row per PID (latest)
    if "observed" in df.columns:
        df["observed"] = pd.to_datetime(df["observed"], utc=True, errors="coerce")
        df = (
            df.sort_values(["pid", "observed"])
              .drop_duplicates("pid", keep="last")
        )

    return df.reset_index(drop=True)


if __name__ == "__main__":
    pid_path = r"Marks_for_NAVD88.xlsx"
    df = pd.read_excel(
        pid_path,
        )
    
    # Drop any NaNs
    df = df[df["PID"].notna()]
    pids = df["PID"].astype(str).tolist()
    print(f"{len(pids)} PIDs loaded")
    
    # Send 'em off
    current_date = datetime.now().strftime("%Y_%m_%d")
    out_fname = f'OPUS_API_pull_{current_date}.csv'
    df_opus = fetch_latest_opus(pids)
    df_opus.to_csv(
        out_fname,
        index=False
        )
