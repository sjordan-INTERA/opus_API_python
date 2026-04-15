# -*- coding: utf-8 -*-
"""
NGS datasheet scraper for NAVD 88 and NGVD 29 elevations.

What this script does
---------------------
- Reads PIDs from an Excel workbook
- Requests NGS datasheets in chunks
- Saves one CSV per chunk so long runs can restart safely
- Extracts:
    * NAVD 88 orthometric height in feet
    * NGVD 29 height in feet

NGVD 29 selection rule
----------------------
1. Prefer an NGVD 29 line whose date contains ??/??/...
2. If no such line exists, fall back to the earliest dated NGVD 29 line

Notes
-----
- Uses modest threading -> default 2 workers
- Writes chunk CSVs immediately so a failure does not lose prior work.
"""

import math
import os
import re
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter, Retry


BASE_URL = "https://www.ngs.noaa.gov/cgi-bin/ds_mark.prl?PidBox={pid}"


def build_session():
    """Build a requests session with retries."""
    session = requests.Session()

    retries = Retry(
        total=5,
        backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=["GET"],
        raise_on_status=False,
    )

    session.mount("https://", HTTPAdapter(max_retries=retries))
    session.headers.update(
        {"User-Agent": "Mozilla/5.0 (compatible; NGS-scraper/1.0)"}
    )
    return session


def fetch_html(pid, session=None, timeout=20):
    """Fetch one datasheet page."""
    session = session or build_session()
    url = BASE_URL.format(pid=pid)
    response = session.get(url, timeout=timeout)
    response.raise_for_status()
    return response.text


def soup_text(html):
    """
    Convert the HTML page to normalized plain text.

    The datasheets are mostly preformatted text, but this keeps parsing
    reasonably stable even if the page structure varies a bit.
    """
    soup = BeautifulSoup(html, "lxml")
    text = soup.get_text("\n")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{2,}", "\n", text).strip()
    return text


def extract_feet_value(line):
    """
    Extract the numeric value immediately before (f) or (feet).

    Supports numbers like:
    - 249.5
    - 614.
    - 227
    """
    match = re.search(
        r"([+-]?\d+(?:\.\d*)?)\s*\((?:f|feet)\)",
        line,
        flags=re.IGNORECASE,
    )
    if match:
        return float(match.group(1))
    return None


def extract_meter_value(line):
    """Extract the numeric value immediately before (m) or (meters)."""
    match = re.search(
        r"([+-]?\d+(?:\.\d*)?)\s*\((?:m|meters)\)",
        line,
        flags=re.IGNORECASE,
    )
    if match:
        return float(match.group(1))
    return None


def parse_navd88(text, out):
    """
    Extract NAVD 88 orthometric height.

    We grab the number immediately before '(feet)' from the NAVD 88 ORTHO HEIGHT
    line. This is intentionally tolerant of inconsistent formatting, including:
    - (+/-2cm)
    - (meters)
    - feet values like 614.
    """
    navd_line_match = re.search(
        r"^.*NAVD\s*88\s+ORTHO\s+HEIGHT.*$",
        text,
        flags=re.IGNORECASE | re.MULTILINE,
    )

    if not navd_line_match:
        return

    line = navd_line_match.group(0)
    out["navd88_match_line"] = line

    navd_ft = extract_feet_value(line)
    # navd_m = extract_meter_value(line)

    if navd_ft is not None:
        out["navd88_ft"] = navd_ft
    # if navd_m is not None:
    #     out["navd88_m"] = navd_m


def parse_ngvd29(text, out):
    """
    Extract NGVD 29 height using the user's selection rule:

    1. Prefer a line with ??/??/... in the date
    2. Otherwise use the earliest dated NGVD 29 line
    3. Otherwise use the first undated NGVD 29 line
    """
    ngvd_lines = re.findall(
        r"^.*NGVD\s*29.*$",
        text,
        flags=re.IGNORECASE | re.MULTILINE,
    )

    # out["ngvd29_line_count"] = len(ngvd_lines)
    # if ngvd_lines:
    #     out["all_ngvd29_lines"] = " | ".join(ngvd_lines)

    if not ngvd_lines:
        return

    selected_line = None
    selected_source = None

    unknown_date_re = re.compile(r"\(\s*\?\?\s*/\s*\?\?\s*/", flags=re.IGNORECASE)
    real_date_re = re.compile(r"\((\d{2}/\d{2}/\d{2})\)")

    # First preference: lines with ??/??/... in the date
    for line in ngvd_lines:
        if unknown_date_re.search(line):
            selected_line = line
            selected_source = False
            break

    # Second preference: earliest real MM/DD/YY date if no ?? line exists
    if selected_line is None:
        dated_lines = []
        for line in ngvd_lines:
            match = real_date_re.search(line)
            if not match:
                continue
            try:
                dt = datetime.strptime(match.group(1), "%m/%d/%y")
                dated_lines.append((dt, line))
            except ValueError:
                continue

        if dated_lines:
            dated_lines.sort(key=lambda x: x[0])
            selected_line = dated_lines[0][1]
            selected_source = True

    # Third preference: first line with no recognizable date at all
    if selected_line is None:
        for line in ngvd_lines:
            if not unknown_date_re.search(line) and not real_date_re.search(line):
                selected_line = line
                selected_source = None
                break

    if selected_line is None:
        return

    # out["ngvd29_match_line"] = selected_line
    out["ngvd29_flag"] = selected_source

    ngvd_ft = extract_feet_value(selected_line)
    ngvd_m = extract_meter_value(selected_line)

    if ngvd_ft is not None:
        out["ngvd29_ft"] = ngvd_ft
    # if ngvd_m is not None:
    #     out["ngvd29_m"] = ngvd_m


def parse_datasheet(text, pid):
    """Parse one datasheet text block into a record dict."""
    out = {"pid": pid}

    parse_navd88(text, out)
    parse_ngvd29(text, out)

    return out


def scrape_pid(pid, session=None, sleep_between=0.1):
    """Fetch and parse one PID."""
    html = fetch_html(pid, session=session)
    text = soup_text(html)
    out = parse_datasheet(text, pid)

    if sleep_between:
        time.sleep(sleep_between)

    return out


def chunk_list(seq, chunk_size):
    """Yield (start_index, chunk) pairs."""
    for i in range(0, len(seq), chunk_size):
        yield i, seq[i : i + chunk_size]


def scrape_chunk(pids, max_workers=2):
    """Scrape one PID chunk and return a DataFrame."""
    session = build_session()
    results = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(scrape_pid, pid, session): pid
            for pid in pids
        }

        for future in as_completed(futures):
            pid = futures[future]
            try:
                record = future.result()
            except Exception as exc:
                record = {
                    "pid": pid,
                    "error": str(exc),
                }
            results.append(record)

    return pd.DataFrame(results)


def combine_chunk_csvs(output_dir, combined_csv_name="ngs_all_chunks_combined.csv"):
    """Combine all chunk CSVs into one final CSV."""
    chunk_files = sorted(
        [
            os.path.join(output_dir, fname)
            for fname in os.listdir(output_dir)
            if fname.lower().endswith(".csv") and fname.startswith("ngs_chunk_")
        ]
    )

    if not chunk_files:
        print("No chunk CSVs found to combine.")
        return

    df = pd.concat([pd.read_csv(path) for path in chunk_files], ignore_index=True)
    out_csv = os.path.join(output_dir, combined_csv_name)
    df.to_csv(out_csv, index=False)
    print(f"Combined CSV written to: {out_csv}")


def main():
    input_xlsx = "Supplementary_list.xlsx"
    output_dir = "ngs_chunks_temp"
    chunk_size = 50
    max_workers = 2
    pause_between_chunks = 2

    os.makedirs(output_dir, exist_ok=True)

    df = pd.read_excel(input_xlsx)
    df = df.head(50)

    if "PID" not in df.columns:
        raise ValueError("Could not find a 'PID' column in the workbook.")

    pids = df["PID"].dropna().astype(str).str.strip().tolist()
    total_chunks = math.ceil(len(pids) / chunk_size)

    for chunk_idx, (start_idx, pid_chunk) in enumerate(
            chunk_list(pids, chunk_size), start=1
        ):
        end_idx = start_idx + len(pid_chunk) - 1

        out_csv = os.path.join(
            output_dir,
            f"ngs_chunk_{chunk_idx:03d}_{start_idx:05d}_{end_idx:05d}.csv",
        )

        if os.path.exists(out_csv):
            print(f"Skipping chunk {chunk_idx}/{total_chunks}, already exists: {out_csv}")
            continue

        print(
            f"Running chunk {chunk_idx}/{total_chunks} "
            f"({len(pid_chunk)} PIDs, rows {start_idx} to {end_idx})"
        )

        t0 = time.time()
        df_chunk = scrape_chunk(pid_chunk, max_workers=max_workers)
        # print(df_chunk)
        df_chunk.to_csv(out_csv, index=False)
        elapsed = time.time() - t0

        print(f"Saved {out_csv} in {elapsed / 60:.2f} minutes")

        n_errors = (
            df_chunk["error"].notna().sum()
            if "error" in df_chunk.columns
            else 0
        )
        print(f"Chunk errors: {n_errors}")

        time.sleep(pause_between_chunks)

    print("All chunks complete.")

    # Auto-combine chunks at the end
    combine_chunk_csvs(output_dir)


if __name__ == "__main__":
    main()
