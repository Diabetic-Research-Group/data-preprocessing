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
