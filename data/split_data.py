#!/usr/bin/env python3
import argparse
from pathlib import Path

import pandas as pd


def split_csv() -> None:

    # Define input and output paths
    input_path = Path("ResaleflatpricesbasedonregistrationdatefromJan2017onwards.csv").expanduser().resolve()
    output_dir = Path("./split").expanduser()
    output_dir.mkdir(parents=True, exist_ok=True)

    # Define output file paths
    first_path = output_dir / "ResaleflatpricesbasedonregistrationdatefromJan2017onwards_part1_200k.csv"
    second_path = output_dir / "ResaleflatpricesbasedonregistrationdatefromJan2017onwards_part2_5k.csv"
    third_path = output_dir / "ResaleflatpricesbasedonregistrationdatefromJan2017onwards_part3_5k.csv"
    fourth_path = output_dir / "ResaleflatpricesbasedonregistrationdatefromJan2017onwards_part4_5k.csv"
    fifth_path = output_dir / "ResaleflatpricesbasedonregistrationdatefromJan2017onwards_part5_5k.csv"
    sixth_path = output_dir / "ResaleflatpricesbasedonregistrationdatefromJan2017onwards_part6_5k.csv" 
    seventh_path = output_dir / "ResaleflatpricesbasedonregistrationdatefromJan2017onwards_part7_5k.csv"
    eighth_path = output_dir / "ResaleflatpricesbasedonregistrationdatefromJan2017onwards_part8_remaining.csv"

    df = pd.read_csv(input_path)
    if df.empty:
        raise ValueError("Input CSV is empty")

    if "id" in df.columns:
        df = df.set_index("id")
    else:
        df.index = pd.RangeIndex(start=1, stop=len(df) + 1, step=1)
        df.index.name = "id"


    # Creating empty columns maps directly to NULL in the final CSV file
    df["created_at"] = ""
    df["updated_at"] = ""

    # Split the DataFrame into specified parts
    first_df = df.iloc[:200_000]
    second_df = df.iloc[200_000:205_000]
    third_df = df.iloc[205_000:210_000]
    fourth_df = df.iloc[210_000:215_000]
    fifth_df = df.iloc[215_000:220_000]
    sixth_df = df.iloc[220_000:225_000]
    seventh_df = df.iloc[225_000:230_000]
    eighth_df = df.iloc[230_000:]

    first_df.to_csv(first_path, index=True, index_label="id")
    second_df.to_csv(second_path, index=True, index_label="id")
    third_df.to_csv(third_path, index=True, index_label="id")
    fourth_df.to_csv(fourth_path, index=True, index_label="id")
    fifth_df.to_csv(fifth_path, index=True, index_label="id")
    sixth_df.to_csv(sixth_path, index=True, index_label="id")
    seventh_df.to_csv(seventh_path, index=True, index_label="id")
    eighth_df.to_csv(eighth_path, index=True, index_label="id")

    total_rows = len(df)
    print("Split complete:")
    print(f"  1) {first_path} ({len(first_df)} rows)")
    print(f"  2) {second_path} ({len(second_df)} rows)")
    print(f"  3) {third_path} ({len(third_df)} rows)")
    print(f"  4) {fourth_path} ({len(fourth_df)} rows)")
    print(f"  5) {fifth_path} ({len(fifth_df)} rows)")
    print(f"  6) {sixth_path} ({len(sixth_df)} rows)")
    print(f"  7) {seventh_path} ({len(seventh_df)} rows)")
    print(f"  8) {eighth_path} ({len(eighth_df)} rows)")
    print(f"Total rows processed (excluding header): {total_rows}")


if __name__ == "__main__":
    split_csv()
