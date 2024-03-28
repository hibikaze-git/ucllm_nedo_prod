"""
既存のjsonlから日本語データを抽出してjsonlに保存
python extract_data_ja.py ./data/0313wiki.jsonl ./data/0313wiki_ja.jsonl
"""

import json
import argparse


def extract_japanese_text(input_file, output_file):
    with open(output_file, "w", encoding="utf-8") as outfile:
        with open(input_file, "r", encoding="utf-8") as infile:
            for line in infile:
                data = json.loads(line.strip())
                if data.get("text") and is_japanese_percentage(data["text"], 0.2):
                    outfile.write(json.dumps(data, ensure_ascii=False) + "\n")


def is_japanese_percentage(text, threshold):
    total_chars = len(text)
    japanese_chars = sum(1 for c in text if "\u3040" <= c <= "\u30FF")
    return (japanese_chars / total_chars) >= threshold if total_chars > 0 else False


def main():
    parser = argparse.ArgumentParser(
        description="Extract Japanese text from a JSONL file with at least 20% Japanese content."
    )
    parser.add_argument("input_file", type=str, help="Input JSONL file containing text data.")
    parser.add_argument("output_file", type=str, help="Output JSONL file for Japanese text.")

    args = parser.parse_args()

    extract_japanese_text(args.input_file, args.output_file)


if __name__ == "__main__":
    main()
