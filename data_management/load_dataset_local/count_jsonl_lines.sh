#!/bin/bash

# 引数が指定されているか確認
# bash count_jsonl_lines.sh ./extract
if [ $# -eq 0 ]; then
  echo "Usage: $0 <directory>"
  exit 1
fi

# 指定されたディレクトリが存在するか確認
if [ ! -d "$1" ]; then
  echo "Directory not found: $1"
  exit 1
fi

# 指定ディレクトリ内の全.jsonlファイルの行数を合計
total_lines=0
for file in "$1"/*.jsonl
do
  if [ -f "$file" ]; then
    lines=$(wc -l < "$file")
    total_lines=$((total_lines + lines))
  fi
done

echo "Total lines in all .jsonl files: $total_lines"
