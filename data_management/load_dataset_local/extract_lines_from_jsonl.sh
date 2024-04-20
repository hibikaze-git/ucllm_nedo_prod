#!/bin/bash

# 使用法の表示
# bash extract_multiple_lines.sh ./config_extract_lines_from_jsonl.txt
usage() {
  echo "Usage: $0 <config_file>"
  exit 1
}

# 引数のチェック
if [ "$#" -ne 1 ]; then
  usage
fi

# 設定ファイルの読み込み
CONFIG_FILE=$1

# ファイル存在確認
if [ ! -f "$CONFIG_FILE" ]; then
  echo "Error: Config file $CONFIG_FILE does not exist."
  exit 1
fi

# トータル行数を設定ファイルから読み込む
TOTAL_LINES=$(head -n 1 $CONFIG_FILE)

# ループで各ファイルペアを処理
tail -n +2 $CONFIG_FILE | while read INPUT_FILE OUTPUT_FILE PERCENTAGE
do
  if [ ! -f "$INPUT_FILE" ]; then
    echo "Error: Input file $INPUT_FILE does not exist."
    continue
  fi

  # 抽出する行数を計算
  NUM_LINES=$(awk -v total="$TOTAL_LINES" -v percent="$PERCENTAGE" 'BEGIN {print int(total * percent + 0.5)}')

  # 指定行数のデータを新しいファイルに抽出
  head -n $NUM_LINES $INPUT_FILE > $OUTPUT_FILE

  echo "Extracted $NUM_LINES lines from $INPUT_FILE to $OUTPUT_FILE"
done
