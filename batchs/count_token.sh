#!/bin/bash

# Command line options go here
#SBATCH --partition=g2
#SBATCH --time=06:00:00
#SBATCH --nodes=1
#SBATCH --job-name=count_token
#SBATCH --output=count_token.out
#SBATCH --gpus-per-node=0
#SBATCH --cpus-per-task=24

# Command(s) goes here
source ~/miniconda3/etc/profile.d/conda.sh
conda activate train

cd /persistentshare/storage/team_nakamura/member/yamaguchi/ext_hrk_ymgch_gmail_com/ucllm_nedo_prod/data_management/load_dataset_local

python count_tokens.py hf hibikaze/gpt_0.084B_wiki-en-ja-2b_python-0.5b_65k_step11626 /persistentshare/storage/team_haijima/dataset_pre/CulturaX/ja parquet parquet
