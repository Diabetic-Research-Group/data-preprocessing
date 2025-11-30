# data-preprocessing
Preprocessing codes for diabetes dataset

## Dataset links

1. [Raw training dataset](https://www.kaggle.com/datasets/nguyenvy/nhanes-19882018?select=medications_clean.csv)

2. [Merged training dataset](https://huggingface.co/datasets/rtweera/nhanes-training-merged)

3. [Diabetes classified training dataset](https://huggingface.co/datasets/rtweera/nhanes-diabetes-classified-training-dataset)

4. [Selected attributes - 386 attributes](https://huggingface.co/datasets/rtweera/nhanes-diabetes-selected-attributes-386)

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
