"""
コーパスの名前、電話番号、メールアドレスをマスキングする
ただし名前は、後ろに敬称（さん、くん、君、様）が付くものに限る

前提
pip install -U hojichar==0.9.0
pip install regex
pip install faker
pip install datasets transformers

使い方の例
(初回)
python mask_personal_info.py ./hf_dataset/CulturaX/ja ./output parquet parquet
python mask_personal_info.py /persistentshare/storage/team_haijima/dataset_pre/CulturaX/ja ./output parquet parquet

(継続)
python mask_personal_info.py /persistentshare/storage/team_haijima/dataset_pre/CulturaX/ja ./output parquet parquet -processed_dir ./output-0415
"""

import argparse
import glob
import json
import os
import shutil

import hojichar
import regex as re

from datasets import load_dataset
from faker import Faker
from multiprocessing import Pool, cpu_count
from tqdm import tqdm

fake = Faker("ja_JP")


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


def replace_entities(text):
    # 名前を置換
    text = re.sub(
        r"(?P<name>\p{Script=Han}+)(?P<honorific>さん|くん|様|君)",
        lambda m: fake.last_name() + m.group("honorific"),
        text,
    )

    # 電話番号を置換
    text = re.sub(r"\d{2,4}-?\d{2,4}-?\d{2,4}", lambda x: fake.phone_number(), text)

    # メールアドレスを置換
    text = re.sub(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}", fake.email(), text)

    return text


def process_text(item):
    replaced_text = replace_entities(item["text"])
    # HojiCharの処理
    cleaner = hojichar.Compose(
        [
            hojichar.document_filters.DocumentNormalizer(),
            hojichar.document_filters.JSONDumper(),
        ]
    )
    document = hojichar.Document(replaced_text)
    #document = hojichar.Document(json.dumps({"text": replaced_text, "origin_text": item["text"]}, ensure_ascii=False)) #確認用
    cleaned_text = cleaner.apply(document).text
    return cleaned_text


def main(args):
    pattern = os.path.join(args.input_dir, f"**/*.{args.extension}")
    file_paths = glob.glob(pattern, recursive=True)
    print("num_file_path:", len(file_paths))
    print(file_paths[:5])

    target_file_paths = file_paths

    if args.processed_dir is not None:
        processed_file_suffix = "-mask_personal_info.jsonl"
        processed_pattern = os.path.join(args.processed_dir, f"**/*.{args.extension}" + processed_file_suffix)
        processed_file_paths = glob.glob(processed_pattern, recursive=True)
        print(processed_file_paths[:5])

        processed_file_name_list = [os.path.basename(processed_file_path) for processed_file_path in processed_file_paths]

        target_file_paths = [file_path for file_path in file_paths if os.path.basename(file_path) + processed_file_suffix not in processed_file_name_list]

    print(target_file_paths[:5])

    for file_path in tqdm(target_file_paths):
        print(file_path)
        dataset = load_dataset(args.file_type, data_files=file_path, split="train", cache_dir="./dataset_cache")

        dataset_list = list(dataset)
        #dataset_list = list(dataset)[:1000] #確認用

        num_cores = cpu_count()
        print("num cpu:", num_cores)

        with Pool(num_cores) as pool:
            processed_texts = pool.map(process_text, dataset_list)

        output_filename = os.path.basename(file_path) + processed_file_suffix
        output_path = os.path.join(args.output_dir, output_filename)

        with open(output_path, "w") as f:
            for processed_text in processed_texts:
                f.write(processed_text + "\n")

        # 結果の表示
        num_display = 10
        with open(output_path, "r") as f:
            for i, line in enumerate(f):
                if i >= num_display:
                    break
                data = json.loads(line)
                actual_text = data["text"]
                print(actual_text)

        rm_cache()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    # 必須の引数
    parser.add_argument("input_dir", type=str, help="Path to the input directory")
    parser.add_argument("output_dir", type=str, help="Path to the output directory")
    parser.add_argument("extension", type=str, help="File extension to process")
    parser.add_argument("file_type", type=str, help="Type of file to process")

    # 任意の引数
    parser.add_argument("--processed_dir", type=str, default=None, help="Path to the directory for processed files")

    args = parser.parse_args()

    # 出力ディレクトリの作成
    make_dir(args.output_dir)

    # メイン関数の実行
    main(args)
