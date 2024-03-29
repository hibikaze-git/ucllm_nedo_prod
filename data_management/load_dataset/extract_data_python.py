"""
python extract_data_python.py ./data/github ./data/github/output
"""

import json
import argparse
import os
from pathlib import Path


def extract_python_text(input_dir, output_dir):
    # Ensure output directory exists
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    # List all jsonl files in the input directory
    for file_name in os.listdir(input_dir):
        if file_name.endswith(".jsonl"):
            input_file_path = os.path.join(input_dir, file_name)
            output_file_path = os.path.join(output_dir, file_name)

            with open(input_file_path, "r", encoding="utf-8") as infile, open(
                output_file_path, "w", encoding="utf-8"
            ) as outfile:
                for line in infile:
                    data = json.loads(line.strip())
                    if data.get("meta") and is_python(data["meta"]):
                        outfile.write(json.dumps({"text": data.get("text")}, ensure_ascii=False) + "\n")


def is_python(meta):
    if len(meta.get("language")) == 1 and meta.get("language")[0].get("name") == "Python":
        return True
    else:
        return False


def main():
    parser = argparse.ArgumentParser(
        description="Extract Japanese text from JSONL files in a directory with at least 20% Japanese content."
    )
    parser.add_argument("input_dir", type=str, help="Input directory containing text data.")
    parser.add_argument("output_dir", type=str, help="Output directory for the processed files.")

    args = parser.parse_args()

    extract_python_text(args.input_dir, args.output_dir)


if __name__ == "__main__":
    main()
