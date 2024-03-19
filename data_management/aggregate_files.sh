#!/bin/sh

counter=1

# outputディレクトリ内のすべてのサブディレクトリに対して処理を行う
find output/filtering/* -type f | while read filename; do
    # ファイルを移動して名前を変更
    cp "$filename" "output/filtered/$counter.jsonl"
    # カウンターをインクリメント
    ((counter++))
done
