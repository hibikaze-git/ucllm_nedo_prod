#!/bin/sh

#bash aggregate_files.sh output/filtering output/filtered result.filtering.jsonl
#bash aggregate_files.sh output/dedup output/deduped result.dedup.jsonl

if [ "$#" -ne 3 ]; then
    echo "Usage: $0 <input_directory> <output_directory> <jsonl_file>"
    exit 1
fi

input_directory="$1"
output_directory="$2"
jsonl_file="$3"
counter=1

# input_directory内のすべてのサブディレクトリに対して処理を行う
find "$input_directory"/* -type f -name "$jsonl_file" | while read filename; do
    # ファイルを移動して名前を変更
    cp "$filename" "$output_directory/$counter.jsonl"
    # カウンターをインクリメント
    ((counter++))
done
