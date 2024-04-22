#!/bin/bash

# Command line options go here
#SBATCH --partition=g2
#SBATCH --time=06:00:00
#SBATCH --nodes=1
#SBATCH --job-name=data_dl
#SBATCH --output=data_dl.out
#SBATCH --gpus-per-node=0
#SBATCH --cpus-per-task=12

# Command(s) goes here
source ~/miniconda3/etc/profile.d/conda.sh
conda activate data-clone

cd /persistentshare/storage/team_nakamura/member/yamaguchi/ext_hrk_ymgch_gmail_com/ucllm_nedo_prod/data_management

python -m preprocessing.download_dataset --dataset=redpajama --split=github --output_base=/persistentshare/storage/team_nakamura/common/datasets/Redpajama-1T
