#!/bin/bash

# Command line options go here
#SBATCH --partition=g2
#SBATCH --time=06:00:00
#SBATCH --nodes=1
#SBATCH --job-name=separate_domain_slimpajama
#SBATCH --output=separate_domain_slimpajama.out
#SBATCH --gpus-per-node=0
#SBATCH --cpus-per-task=24

# Command(s) goes here
source ~/miniconda3/etc/profile.d/conda.sh
conda activate data-clone

cd /persistentshare/storage/team_nakamura/member/yamaguchi/ext_hrk_ymgch_gmail_com/ucllm_nedo_prod/data_management/load_dataset_local

python separate_domain_slimpajama_multiprocess.py /persistentshare/storage/team_haijima/dataset_pre/SlimPajama-627B/train ./domain_sep
