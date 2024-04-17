"""
HFからload_datasetしてjsonlに保存
python load_dataset_jsonl.py hibikaze/test_llama_ratio ./data/test_llama_ratio.jsonl
python load_dataset_jsonl.py hibikaze/test_mixing_laws_ratio ./data/test_mixing_laws_ratio.jsonl
"""

import argparse
import json

from datasets import load_dataset
from tqdm import tqdm


def to_jsonl(repo_name, output_file):
    with open(output_file, "w", encoding="utf-8") as f:
        dataset = load_dataset(repo_name, cache_dir="./dataset_cache")

        for data in tqdm(dataset["train"]["text"]):
            f.write(json.dumps({"text": data}, ensure_ascii=False) + "\n")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("repo_name", type=str)
    parser.add_argument("output_file", type=str, help="Output JSONL file.")

    args = parser.parse_args()

    to_jsonl(args.repo_name, args.output_file)


if __name__ == "__main__":
    main()
