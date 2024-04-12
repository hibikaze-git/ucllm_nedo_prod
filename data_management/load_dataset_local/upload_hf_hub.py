"""
jsonlをHFにアップロード

huggingface-cli login
python upload_hf_hub.py ./data/0313wiki.jsonl hibikaze/upload_test
"""

import argparse
import os
import shutil

from datasets import load_dataset


def rm_cache():
    cache_path = "./dataset_cache"
    permissions = 0o777

    # 権限を再帰的に変更
    for root, dirs, files in os.walk(cache_path):
        for name in dirs:
            dir_path = os.path.join(root, name)
            os.chmod(dir_path, permissions)
        for name in files:
            file_path = os.path.join(root, name)
            os.chmod(file_path, permissions)

    shutil.rmtree(cache_path, ignore_errors=True)


parser = argparse.ArgumentParser(
    description="upload dataset to huggingface hub"
)

parser.add_argument("input_file", type=str, help="Input JSONL file containing text data.")
parser.add_argument("repo_name", type=str, help="hugging face repository name")

args = parser.parse_args()

dataset = load_dataset("json", data_files=args.input_file, split="train", cache_dir="./dataset_cache")

dataset.push_to_hub(args.repo_name)

rm_cache()
