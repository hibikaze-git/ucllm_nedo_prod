"""
トークナイザーとファイルまたはディレクトリを指定してトークン数をカウント

hf
python count_tokens.py hf hibikaze/gpt_0.084B_wiki-en-ja-2b_python-0.5b_65k_step11626 hf_dataset/slimPajama-627B/train/chunk/example_train_0.jsonl.zst jsonl.zst json

sp
python count_tokens.py sp ./tokenizer.model ./data/0313wiki.jsonl jsonl json
"""
import argparse
import glob
import os
import shutil

from datasets import load_dataset
from tqdm import tqdm


def rm_cache():
    shutil.rmtree("./dataset_cache")


class HFTokenizer():
    def __init__(self, tokeinzer) -> None:
        self.tokenizer = tokeinzer

    def tokenize(self, text):
        result = self.tokenizer.encode(text, add_special_tokens=False)

        return result


class SPTokenizer():
    def __init__(self, tokeinzer) -> None:
        self.tokenizer = tokeinzer

    def tokenize(self, text):
        result = self.tokenizer.encode(text, out_type=str)

        return result


parser = argparse.ArgumentParser(
    description="count token"
)

parser.add_argument("tokenizer_type", type=str, help="hf or sp")
parser.add_argument("repo_or_path", type=str, help="hf: repo, sp: path")
parser.add_argument("file_or_dir", type=str, help="text for count token")
parser.add_argument("extension", type=str, help="file extension")
parser.add_argument("file_type", type=str, help="file extension for load_datast")

args = parser.parse_args()

tokenizer = None

if args.tokenizer_type == "sp":
    import sentencepiece as spm

    tokenizer = SPTokenizer(spm.SentencePieceProcessor(model_file=args.repo_or_path))
elif args.tokenizer_type == "hf":
    from transformers import AutoTokenizer

    tokenizer = HFTokenizer(AutoTokenizer.from_pretrained(args.repo_or_path))
else:
    raise

total_tokens = 0

if os.path.isfile(args.file_or_dir):
    dataset = load_dataset(args.file_type, data_files=args.file_or_dir, split="train", cache_dir="./dataset_cache")

    for data in dataset:
        n_tokens = len(tokenizer.tokenize(data["text"]))
        total_tokens += n_tokens

    rm_cache()

elif os.path.isdir(args.file_or_dir):
    pattern = os.path.join(args.file_or_dir, f"**/*.{args.extension}")
    file_paths = glob.glob(pattern, recursive=True)
    print("num_file_path:", len(file_paths))
    print(file_paths[:5])

    for file_path in tqdm(file_paths):
        print(file_path)
        dataset = load_dataset(args.file_type, data_files=file_path, split="train", cache_dir="./dataset_cache")

        for data in dataset:
            n_tokens = len(tokenizer.tokenize(data["text"]))
            total_tokens += n_tokens

        rm_cache()

        print("check_count:", total_tokens)

else:
    print(f"{args.file_or_dir} is not a valid file or directory.")
    raise

# billion
print("tokens in billion")
print(total_tokens / 10**9)
print("tokens")
print(total_tokens)
