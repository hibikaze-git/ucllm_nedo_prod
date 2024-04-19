"""
parquet→jsonl化

.parquetファイルの名称に重複がないか確認が必要

python parquet_to_jsonl.py input_dir output_dir
"""

import argparse
import glob
import json
import os
import shutil

from datasets import load_dataset
from tqdm import tqdm


def make_dir(path):
    if not os.path.exists(path):
        os.mkdir(path)


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


def main(args):
    pattern = os.path.join(args.input_dir, "**/*.parquet")
    file_paths = glob.glob(pattern, recursive=True)
    print("num_file_path:", len(file_paths))
    print(file_paths[:5])

    target_file_paths = file_paths

    processed_file_paths = []
    processed_file_paths_filename = "processed_file_paths.json"
    processed_file_paths_path = os.path.join(args.output_dir, processed_file_paths_filename)

    if os.path.exists(processed_file_paths_path):
        with open(processed_file_paths_path, "r") as f:
            processed_file_paths = json.load(f)

        print(processed_file_paths[:5])

        target_file_paths = [item for item in file_paths if item not in processed_file_paths]

    print(target_file_paths[:5])

    for file_path in tqdm(target_file_paths):
        print(file_path)
        filename = os.path.basename(file_path).replace(".parquet", "")

        dataset = load_dataset("parquet", data_files=file_path, split="train", cache_dir="./dataset_cache")

        output_filename = filename + ".jsonl"
        output_path = os.path.join(args.output_dir, output_filename)

        with open(output_path, "w", encoding="utf-8") as f:
            for i, data in enumerate(dataset):
                f.write(json.dumps(data, ensure_ascii=False) + "\n")

        processed_file_paths.append(file_path)

        with open(processed_file_paths_path, "w") as f:
            f.write(json.dumps(processed_file_paths, ensure_ascii=False))

        rm_cache()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    # 必須の引数
    parser.add_argument("input_dir", type=str, help="Path to the input directory")
    parser.add_argument("output_dir", type=str, help="Path to the output directory")

    args = parser.parse_args()

    # 出力ディレクトリの作成
    make_dir(args.output_dir)

    # メイン関数の実行
    main(args)
