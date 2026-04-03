# data-preprocessing

Preprocessing codes for diabetes dataset

## Dataset links

### Common

1. [Raw dataset files](https://www.kaggle.com/datasets/nguyenvy/nhanes-19882018)

2. [Merged dataset](https://www.huggingface.co/datasets/AnnDinoushka/nhanes-training-merged-new)

3. [Diabetes classified dataset - v3](https://huggingface.co/datasets/rtweera/nhanes-dataset-diabetes-classified-v3)

4. [Selected raw attributes v3 - 375 attributes](https://huggingface.co/datasets/rtweera/nhanes-dataset-selected-raw-attributes-v3)

5. [Feature Engineered dataset - 273 attributes](https://huggingface.co/datasets/rtweera/nhanes-dataset-feature-engineered)

6. [Datatype corrected and reduced dataset - 74 attributes](https://huggingface.co/datasets/rtweera/nhanes-data-converted)

### Diabetes prediction

1. [Diabetes prediction leaked removed - selection stage 1 - 240 attributes](https://huggingface.co/datasets/rtweera/nhanes-dataset-prediction-selection-stage-1)

2. [Diabetes prediction leaked removed - selection from reduced dataset - To Be Uploaded](https://example.com)

### Severity classification

1. [Example dataset goes here](https://example.com)

### Treatement recommendation

1. [Example dataset goes here](https://example.com)

## How to use the huggingface datasets above

Say the dataset URL is `https://huggingface.co/datasets/user_name/dataset_name`

Use the `user_name/dataset_name`

``` py
import pandas as pd
from datasets import load_dataset

dataset = load_dataset("user_name/dataset_name", split="train")  # default split is train, if the dataset has other splits, use them as necessary

# Use dataset as is for a Arrow dataset or conver to pandas if needed
df = dataset.to_pandas()
```

## Preprocessing scripts

1. `merge_nhanes_files.py` : Merges multiple NHANES files into a single dataset.
2. `parquet_to_csv.py` : Converts Parquet files to CSV format for easier data handling.

## Environment Variables

The project uses environment variables to manage input and output directories as well as options for generating CSV files. You can set these variables in a `.env` file based on the provided `.env.example`.

## Prerequisites

- [Python 3.13](https://www.python.org/downloads/) or higher
- Ensure you have [poetry](https://python-poetry.org/) installed for dependency management.

## Setup

1. Clone the repository to your local machine.
2. Navigate to the project directory.
3. Install the required dependencies using poetry:

   ```bash
   poetry install
   ```

4. Create a `.env` file in the project root directory and set the necessary environment variables as shown in the `.env.example` file.
5. Run the preprocessing scripts as needed.

## Usage Example

To merge NHANES files, run the following command from the project root directory:

   ```bash
   poetry run python code/merge_nhanes_files.py
   ```

## Archived Links

### V1

1. [Initial Merged NHANES Dataset](https://huggingface.co/datasets/rtweera/nhanes-training-merged)
2. [Diabetes classified training dataset](https://huggingface.co/datasets/rtweera/nhanes-diabetes-classified-training-dataset)
3. [Selected attributes - 386 attributes](https://huggingface.co/datasets/rtweera/nhanes-diabetes-selected-attributes-386)
4. [Selected raw attributes - 375 attributes](https://huggingface.co/datasets/rtweera/nhanes-dataset-selected-raw-attributes)

## Project Overview

This repository contains data preprocessing utilities and notebooks for preparing NHANES-based datasets used for diabetes-related research tasks.

At a high level, the workflow includes:

1. Collecting cleaned NHANES component files.
2. Merging files into a single SEQN-keyed dataset.
3. Optionally exporting merged data to CSV.
4. Running notebook-based transformation and feature engineering workflows.
5. Publishing curated datasets to Hugging Face.

## Repository Structure

```text
data-preprocessing/
├── code/
│   ├── merge_nhanes_files.py
│   ├── parquet_to_csv.py
│   ├── latest/
│   └── archive/
├── data/
├── .env.example
├── pyproject.toml
└── README.md
```

### `code/` directory

- `merge_nhanes_files.py`: Main preprocessing script for merging NHANES component files by `SEQN`.
- `parquet_to_csv.py`: Utility script to convert merged Parquet output to CSV.
- `latest/`: Current iteration notebooks for feature engineering and selection workflows.
- `archive/`: Older notebook workflows kept for reference.

### `data/` directory

Contains supporting artifacts such as dictionary and column metadata files used during preprocessing and analysis.

## Detailed Script Behavior

### `merge_nhanes_files.py`

This script:

- Reads input files from `NHANES_INPUT_DIR`.
- Loads files that match `*_clean.csv`, `*_clean.xlsx`, or `*_clean.xls`.
- Excludes helper files (for example dictionary/documentation files) based on filename prefixes.
- Standardizes columns to uppercase and validates `SEQN`.
- Drops fully empty columns.
- Handles medication files specially by aggregating `RXDDRUG` values by `SEQN`.
- Suffixes non-key columns with a component identifier to avoid name collisions.
- Merges each component into an accumulator using a left join on `SEQN`.
- Writes merged output as `<NHANES_OUTPUT_DIR>.parquet`.
- Optionally writes `<NHANES_OUTPUT_DIR>.csv` when `NHANES_MAKE_CSV=1`.
- Writes a merge report as `<NHANES_OUTPUT_DIR>_merge_report.csv`.

### `parquet_to_csv.py`

This utility:

- Reads `<NHANES_OUTPUT_DIR>.parquet`.
- Exports a CSV version to `<NHANES_OUTPUT_DIR>.csv`.

Use this when you already have a Parquet output and only need a CSV export.

## Environment Configuration Reference

Create a `.env` file in the project root using `.env.example` as a guide:

```env
NHANES_INPUT_DIR=/absolute/path/to/input/folder
NHANES_OUTPUT_DIR=/absolute/path/to/output/merged_dataset
NHANES_MAKE_CSV=0
```

### Notes

- `NHANES_INPUT_DIR` should point to a folder containing cleaned NHANES component files.
- `NHANES_OUTPUT_DIR` is a base output path (without extension).
- `NHANES_MAKE_CSV=1` enables automatic CSV generation during merge.

## End-to-End Local Workflow

1. Install dependencies:
   ```bash
   poetry install
   ```
2. Create and configure `.env`.
3. Run merge:
   ```bash
   poetry run python code/merge_nhanes_files.py
   ```
4. (Optional) Convert Parquet to CSV:
   ```bash
   poetry run python code/parquet_to_csv.py
   ```
5. Inspect outputs:
   - `<output>.parquet`
   - `<output>.csv` (optional or from conversion script)
   - `<output>_merge_report.csv`

## Input File Expectations

To ensure smooth merging:

- Keep one participant key column named `SEQN` in each input file.
- Use cleaned filenames ending with `_clean.csv`, `_clean.xlsx`, or `_clean.xls`.
- Ensure files are structurally valid and readable by pandas.
- Keep medication-related files clearly named (including terms like `medication`, `rxq`, or `rx_`) if medication aggregation is expected.

## Troubleshooting

- **`No '*_clean.(csv|xlsx|xls)' files found`**
  - Verify `NHANES_INPUT_DIR` and filename pattern.
- **`No SEQN column found`**
  - Ensure each input file includes `SEQN`.
- **Parquet read/write errors**
  - Confirm dependencies are installed correctly via Poetry and retry.
- **Unexpected missing values after merge**
  - Check source files and verify `SEQN` consistency across components.

## Versioning of Published Datasets

The dataset links above are organized by task type and version stage. For reproducibility:

- Prefer the newest version in each section (`Common`, `Diabetes prediction`, etc.).
- Use archived links only when reproducing older experiments.
- Record exact dataset URLs and versions in downstream training/evaluation logs.
