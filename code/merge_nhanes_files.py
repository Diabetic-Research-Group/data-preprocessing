#!/usr/bin/env python3
"""
Memory-safe NHANES merger (CSV/XLSX/XLS)
With Special Handling for Medications (One-to-Many Aggregation)
"""

import argparse
from pathlib import Path
import re
import sys
import gc
import os
import pandas as pd
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

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
    
    # Normalize columns
    df.columns = [str(c).strip().upper() for c in df.columns]
    if "SEQN" not in df.columns:
        raise ValueError(f"{path.name}: No SEQN column found.")
    
    # Drop empty columns
    drop_cols = [c for c in df.columns if c != "SEQN" and df[c].isna().all()]
    if drop_cols:
        df = df.drop(columns=drop_cols)

    # --- NEW LOGIC: Special Handling for Medications ---
    # We check if the filename suggests it contains medication data.
    # Standard NHANES names usually contain "RXQ_RX" or "medications"
    fname = path.name.lower()
    is_meds_file = "medication" in fname or "rxq" in fname or "rx_" in fname

    if is_meds_file and "RXDDRUG" in df.columns:
        print(f"   -> Detected Medication file: aggregating 'RXDDRUG' by SEQN...")
        
        # 1. Select only SEQN and the Drug Name (ignore dosage/freq to avoid mess)
        df = df[["SEQN", "RXDDRUG"]].copy()
        
        # 2. Remove rows where Drug Name is empty
        df = df.dropna(subset=["RXDDRUG"])
        
        # 3. Convert to string to ensure .join works
        df["RXDDRUG"] = df["RXDDRUG"].astype(str)
        
        # 4. Group by SEQN and join drugs with a comma
        # Example: "Aspirin", "Insulin" -> "Aspirin, Insulin"
        df = df.groupby("SEQN")["RXDDRUG"].agg(lambda x: ', '.join(x)).reset_index()
        
        # Return immediately (df is now unique by SEQN)
        return df
    # ---------------------------------------------------

    # Standard deduplication for all other files (keep first row only)
    if df.duplicated(subset=["SEQN"]).any():
        df = df.sort_values("SEQN").drop_duplicates(subset=["SEQN"], keep="first")
    
    return df

def optimize_dtypes(df: pd.DataFrame, max_cat=200, cat_ratio=0.5) -> pd.DataFrame:
    for c in df.columns:
        if c == "SEQN":
            continue
        col = df[c]
        if pd.api.types.is_float_dtype(col):
            df[c] = pd.to_numeric(col, errors="coerce", downcast="float")
        elif pd.api.types.is_integer_dtype(col):
            df[c] = pd.to_numeric(col, errors="coerce", downcast="integer")
        elif pd.api.types.is_object_dtype(col):
            nunique = col.nunique(dropna=True)
            if nunique <= max_cat and nunique <= cat_ratio * len(col):
                df[c] = col.astype("category")
    return df

def suffix_non_key(df: pd.DataFrame, component: str) -> pd.DataFrame:
    rename_map = {c: f"{c}__{component}" for c in df.columns if c != "SEQN"}
    return df.rename(columns=rename_map)

def main():
    # --- LOAD FROM ENV ---
    input_dir = os.getenv("NHANES_INPUT_DIR")
    output_dir = os.getenv("NHANES_OUTPUT_DIR")
    csv_opt = os.getenv("NHANES_MAKE_CSV", "0") 

    # Validations
    if not input_dir or not output_dir:
        print("ERROR: Could not find paths in .env file.")
        sys.exit(1)

    base = Path(input_dir)
    if not base.exists():
        sys.exit(f"Input Folder not found: {base}")

    patterns = ["*_clean.csv", "*_clean.xlsx", "*_clean.xls"]
    files = []
    for patt in patterns:
        files += [p for p in base.glob(patt) if p.is_file() and not is_excluded(p.name)]
    if not files:
        sys.exit(f"No '*_clean.(csv|xlsx|xls)' files found in {base}")

    files = sorted(files, key=lambda p: (0 if p.name.lower().startswith("demographics_clean") else 1, p.name.lower()))

    # Ensure output directory exists
    out_path_obj = Path(output_dir)
    if not out_path_obj.parent.exists():
        out_path_obj.parent.mkdir(parents=True, exist_ok=True)

    out_parquet = out_path_obj.with_suffix(".parquet")
    tmp_parquet = out_path_obj.with_name(out_path_obj.name + "_tmp.parquet")
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

        df = optimize_dtypes(df)
        df = suffix_non_key(df, comp)
        report_rows.append({"component": comp, "file": f.name, "rows": len(df), "cols": df.shape[1]})

        if accumulator is None:
            accumulator = df
        else:
            print(f"    Merging '{comp}' into accumulator ({accumulator.shape[1]} cols) ...", flush=True)
            accumulator = accumulator.merge(df, on="SEQN", how="left")
        
        accumulator.to_parquet(tmp_parquet, index=False)
        del accumulator, df
        gc.collect()
        accumulator = pd.read_parquet(tmp_parquet)
        print(f"    Accumulator now: {accumulator.shape[0]} rows x {accumulator.shape[1]} cols", flush=True)

    if accumulator is None:
        sys.exit("No data merged. Exiting.")

    accumulator.to_parquet(out_parquet, index=False)
    
    if csv_opt == "1":
        accumulator.to_csv(Path(output_dir).with_suffix(".csv"), index=False)

    pd.DataFrame(report_rows).to_csv(Path(output_dir).with_name(Path(output_dir).name + "_merge_report.csv"), index=False)
    
    if tmp_parquet.exists():
        try:
            tmp_parquet.unlink()
        except:
            pass

    print(f"\nDone. Final shape: {accumulator.shape[0]} rows x {accumulator.shape[1]} cols")
    print(f"Wrote: {out_parquet}")

if __name__ == "__main__":
    main()