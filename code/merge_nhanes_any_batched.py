#!/usr/bin/env python3
"""
Memory-safe NHANES merger (CSV/XLSX/XLS) — incremental left-joins by SEQN.

- Scans a folder for "*_clean.(csv|xlsx|xls)"
- Reads one component at a time, optimizes dtypes (downcast numbers, category for small-cardinality strings)
- Immediately merges into an on-disk Parquet accumulator to keep RAM low
- Parquet output only (much smaller & faster); optional final CSV if you pass --csv 1 (not recommended for very wide data)

Usage:
  python merge_nhanes_any_batched.py --dir "D:/Research/Data Set/NHANES Data Files" --out "D:/Research/nhanes_1999_2018" --csv 0
"""

import argparse
from pathlib import Path
import re
import sys
import gc
import pandas as pd

pd.options.display.width = 200

EXCLUDE_PREFIXES = {"dictionary_", "nhanes_inconsistencies_documentation", "example_", "m -", "w -", "~$"}

def is_excluded(name: str) -> bool:
    lname = name.lower()
    return any(lname.startswith(p) for p in EXCLUDE_PREFIXES)

def component_from_filename(stem: str) -> str:
    s = stem.lower()
    s = re.sub(r"_clean$", "", s)
    s = re.sub(r"_unclean$", "", s)
    s = re.sub(r"\s+", "_", s.strip())
    return s

def read_any(path: Path) -> pd.DataFrame:
    ext = path.suffix.lower()
    if ext == ".csv":
        df = pd.read_csv(path, low_memory=False)
    elif ext in (".xlsx", ".xls"):
        engine = "openpyxl" if ext == ".xlsx" else None
        df = pd.read_excel(path, engine=engine)
    else:
        raise ValueError(f"Unsupported file type: {ext}")
    # normalize
    df.columns = [str(c).strip().upper() for c in df.columns]
    if "SEQN" not in df.columns:
        raise ValueError(f"{path.name}: No SEQN column found.")
    # drop empty cols
    drop_cols = [c for c in df.columns if c != "SEQN" and df[c].isna().all()]
    if drop_cols:
        df = df.drop(columns=drop_cols)
    # dedup by SEQN (TODO: This is wrong. For medications, this causes a data loss.)
    if df.duplicated(subset=["SEQN"]).any():
        df = df.sort_values("SEQN").drop_duplicates(subset=["SEQN"], keep="first")
    return df

def optimize_dtypes(df: pd.DataFrame, max_cat=200, cat_ratio=0.5) -> pd.DataFrame:
    # try to downcast numbers
    for c in df.columns:
        if c == "SEQN":
            continue
        col = df[c]
        if pd.api.types.is_float_dtype(col):
            df[c] = pd.to_numeric(col, errors="coerce", downcast="float")
        elif pd.api.types.is_integer_dtype(col):
            df[c] = pd.to_numeric(col, errors="coerce", downcast="integer")
        elif pd.api.types.is_object_dtype(col):
            # convert to category if relatively small cardinality
            nunique = col.nunique(dropna=True)
            if nunique <= max_cat and nunique <= cat_ratio * len(col):
                df[c] = col.astype("category")
    return df

def suffix_non_key(df: pd.DataFrame, component: str) -> pd.DataFrame:
    rename_map = {c: f"{c}__{component}" for c in df.columns if c != "SEQN"}
    return df.rename(columns=rename_map)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dir", required=True, help="Folder with *_clean files")
    ap.add_argument("--out", required=True, help="Output base path (no extension)")
    ap.add_argument("--csv", type=int, default=0, help="Also write a final CSV (0/1). Default 0 = no CSV.")
    args = ap.parse_args()

    base = Path(args.dir)
    if not base.exists():
        sys.exit(f"Folder not found: {base}")

    patterns = ["*_clean.csv", "*_clean.xlsx", "*_clean.xls"]
    files = []
    for patt in patterns:
        files += [p for p in base.glob(patt) if p.is_file() and not is_excluded(p.name)]
    if not files:
        sys.exit("No '*_clean.(csv|xlsx|xls)' files found.")

    # make demographics first if present
    files = sorted(files, key=lambda p: (0 if p.name.lower().startswith("demographics_clean") else 1, p.name.lower()))

    out_parquet = Path(args.out).with_suffix(".parquet")
    tmp_parquet = Path(args.out).with_name(Path(args.out).name + "_tmp.parquet")
    report_rows = []

    accumulator = None

    for i, f in enumerate(files, 1):
        comp = component_from_filename(f.stem)
        print(f"[{i}/{len(files)}] Reading {f.name} as component '{comp}' ...", flush=True)
        try:
            df = read_any(f)
        except Exception as e:
            print(f"[WARN] Skipping {f.name}: {e}")
            continue

        # optimize and suffix
        df = optimize_dtypes(df)
        df = suffix_non_key(df, comp)
        report_rows.append({"component": comp, "file": f.name, "rows": len(df), "cols": df.shape[1]})

        if accumulator is None:
            # start accumulator
            accumulator = df
        else:
            # merge and immediately spill to disk
            print(f"    Merging '{comp}' into accumulator ({accumulator.shape[1]} cols) ...", flush=True)
            accumulator = accumulator.merge(df, on="SEQN", how="left")
        # spill to disk every step to keep memory low
        accumulator.to_parquet(tmp_parquet, index=False)
        # reload to ensure memory is compacted
        del accumulator, df
        gc.collect()
        accumulator = pd.read_parquet(tmp_parquet)

        print(f"    Accumulator now: {accumulator.shape[0]} rows x {accumulator.shape[1]} cols", flush=True)

    # final write
    accumulator.to_parquet(out_parquet, index=False)
    # optional CSV (can be very large)
    if args.csv == 1:
        accumulator.to_csv(Path(args.out).with_suffix(".csv"), index=False)

    pd.DataFrame(report_rows).to_csv(Path(args.out).with_name(Path(args.out).name + "_merge_report.csv"), index=False)
    print(f"\nDone. Final shape: {accumulator.shape[0]} rows x {accumulator.shape[1]} cols")
    print(f"Wrote: {out_parquet}")
    if args.csv == 1:
        print(f"Wrote: {Path(args.out).with_suffix('.csv')}")
    print(f"Wrote: {Path(args.out).with_name(Path(args.out).name + '_merge_report.csv')}")

if __name__ == "__main__":
    main()
