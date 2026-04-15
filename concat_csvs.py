# -*- coding: utf-8 -*-
"""
Created on Wed Apr 15 09:06:26 2026

@author: shjordan
"""

import os
import pandas as pd


def combine_ngs_chunks(
    input_dir="ngs_chunks",
    output_csv="ngs_all_chunks_combined.csv"
):
    """
    Combine all ngs_chunk_*.csv files in a directory into a single CSV.
    """

    if not os.path.exists(input_dir):
        raise FileNotFoundError(f"Directory not found: {input_dir}")

    # Grab only the chunk CSVs
    csv_files = sorted([
        os.path.join(input_dir, f)
        for f in os.listdir(input_dir)
        if f.lower().endswith(".csv") and f.startswith("ngs_chunk_")
    ])

    if not csv_files:
        raise ValueError(f"No ngs_chunk_*.csv files found in {input_dir}")

    print(f"Found {len(csv_files)} chunk files")

    # Read + concat
    dfs = []
    for f in csv_files:
        print(f"Reading: {f}")
        dfs.append(pd.read_csv(f))

    combined = pd.concat(dfs, ignore_index=True)

    # Write output in parent directory
    combined.to_csv(output_csv, index=False)

    print(f"\nCombined CSV written to: {output_csv}")
    print(f"Total rows: {len(combined)}")


if __name__ == "__main__":
    combine_ngs_chunks()