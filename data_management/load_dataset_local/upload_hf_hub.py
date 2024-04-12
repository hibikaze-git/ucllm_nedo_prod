"""
jsonlをHFにアップロード

huggingface-cli login
python upload_hf_hub.py ./data/0313wiki.jsonl hibikaze/upload_test
"""

import argparse
import shutil

from datasets import load_dataset


def rm_cache():
    shutil.rmtree("./dataset_cache")


parser = argparse.ArgumentParser(
    description="upload dataset to huggingface hub"
)

parser.add_argument("input_file", type=str, help="Input JSONL file containing text data.")
parser.add_argument("repo_name", type=str, help="hugging face repository name")

args = parser.parse_args()

dataset = load_dataset("json", data_files=args.input_file, split="train", cache_dir="./dataset_cache")

dataset.push_to_hub(args.repo_name)

rm_cache()
