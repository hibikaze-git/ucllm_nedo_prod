#!/bin/bash

# Command line options go here
SBATCH --partition=g2
SBATCH --time=01:00:00
SBATCH --nodes=1
SBATCH --job-name=train_tokneizer
#SBATCH --output=example.out
SBATCH --gpus-per-node=0

# Command(s) goes here
source ~/miniconda3/etc/profile.d/conda.sh
conda activate train

cd ~/ucllm_nedo_prod/train/scripts/step1_train_tokenizer

python ./train_sentencepiece_tokenizer.py \
    --input /persistentshare/storage/team_nakamura/yamaguchi/data/text/0313wiki.jsonl \
    --model_prefix tokenizer \
    --vocab_size 65000 \
    --output_dir ./wiki_65k_vocab
