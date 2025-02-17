#!/bin/bash

# Command line options go here
#SBATCH --partition=g2
#SBATCH --time=06:00:00
#SBATCH --nodes=1
#SBATCH --job-name=train_model
#SBATCH --output=train_model.out
#SBATCH --gpus-per-node=1
#SBATCH --cpus-per-task=24

# Command(s) goes here
source ~/miniconda3/etc/profile.d/conda.sh
conda activate train

cd /persistentshare/storage/team_nakamura/member/yamaguchi/train

bash /persistentshare/storage/team_nakamura/member/yamaguchi/ext_hrk_ymgch_gmail_com/ucllm_nedo_prod/train/scripts/step2_pretrain_model/gcp_node-1_gpu/with-python/zero-0_dp-1_pp-1_tp-1_flashattn2-on.sh \
    --input_tokenizer_file /persistentshare/storage/team_nakamura/member/yamaguchi/ext_hrk_ymgch_gmail_com/ucllm_nedo_prod/train/scripts/step1_train_tokenizer/output_model/wiki_65k_vocab_1000000_with_python/tokenizer.model \
    --output_model_dir /persistentshare/storage/team_nakamura/member/yamaguchi/train/output/step2_pretrain_model/wiki_65k_1.9b_0.084b_with_python \
    --save_interval 200
