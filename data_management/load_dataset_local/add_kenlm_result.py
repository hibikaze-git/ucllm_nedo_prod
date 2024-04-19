"""
KenLMによる品質スコアリング

python add_kenlm_result.py ./hf_dataset/CultureX/ja ./output

kenlmの環境構築が必要
https://github.com/lighttransport/japanese-llama-experiment
https://huggingface.co/lighttransport/japanese-scoring-model
"""

import argparse
import glob
import json
import os
import shutil
#import unicodedata

import kenlm

from datasets import load_dataset
from fugashi import Tagger
from multiprocessing import Pool


CACHE_PATH = "./dataset_cache"
#MODEL_BIN='./data/lm_sp/kenlm_model-wiki-nfkc-char.bin'
MODEL_BIN='./data/lm_sp/kenlm_model-wiki-nfkc-wakachi.bin'


def make_dir(path):
    if not os.path.exists(path):
        os.mkdir(path)


def rm_cache(cache_path):
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


def process_file(file_path, output_dir, processed_file_paths_path):
    model = kenlm.LanguageModel(MODEL_BIN)
    tagger = Tagger('-Owakati')

    print(file_path)
    filename = os.path.basename(file_path).replace(".parquet", "")

    pid = os.getpid()
    cache_dir = os.path.join(CACHE_PATH, f"dataset_cache_{pid}")

    dataset = load_dataset("parquet", data_files=file_path, split="train", cache_dir=cache_dir)

    output_filename = filename + "_kenlm.jsonl"
    output_path = os.path.join(output_dir, output_filename)

    with open(output_path, "w") as f:
        for i, data in enumerate(dataset):
            #text = unicodedata.normalize('NFKC', data["text"].strip())
            #sentence = " ".join(text)
            sentence = tagger.parse(data["text"].strip())
            prob = model.score(sentence, bos=True, eos=True)
            perplexity = model.perplexity(sentence)

            data.update(ppl=perplexity)

            #f.write(json.dumps(data, ensure_ascii=False) + "\n")
            f.write(str(perplexity) + "\t" + json.dumps(data, ensure_ascii=False) + "\n") # 確認用

            if i == 100:
                break

    #with open(processed_file_paths_path, "a") as f:
    #    f.write(file_path + "\n")

    rm_cache(cache_dir)


def main(args):
    pattern = os.path.join(args.input_dir, "**/*.parquet")
    file_paths = glob.glob(pattern, recursive=True)
    print("num_file_path:", len(file_paths))
    print(file_paths[:5])

    target_file_paths = file_paths

    processed_file_paths_filename = "processed_file_paths.txt"
    processed_file_paths_path = os.path.join(args.output_dir, processed_file_paths_filename)

    if os.path.exists(processed_file_paths_path):
        processed_file_paths = []

        with open(processed_file_paths_path, "r") as f:
            for line in f:
                # 各行から改行文字を削除
                stripped_line = line.strip()
                # 改行文字が削除された行をリストに追加
                processed_file_paths.append(stripped_line)

        print(processed_file_paths[:5])

        target_file_paths = [item for item in file_paths if item not in processed_file_paths]
    else:
        with open(processed_file_paths_path, "w") as f:
            f.write("")

    print(target_file_paths[:5])

    with Pool(processes=os.cpu_count()) as pool:
        results = pool.starmap(process_file, [(path, args.output_dir, processed_file_paths_path) for path in target_file_paths])


if __name__ == "__main__":
    if not os.path.exists(MODEL_BIN):
        raise Exception("model file not found: {}".format(MODEL_BIN))

    parser = argparse.ArgumentParser()

    # 必須の引数
    parser.add_argument("input_dir", type=str, help="Path to the input directory")
    parser.add_argument("output_dir", type=str, help="Path to the output directory")

    args = parser.parse_args()

    # 出力ディレクトリの作成
    make_dir(args.output_dir)

    # メイン関数の実行
    main(args)
