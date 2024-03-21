#!/bin/bash

cd ./train/
git clone https://github.com/hibikaze-git/Megatron-DeepSpeed.git
cd ./Megatron-DeepSpeed && git fetch origin && git checkout refs/tags/ucllm_nedo_dev_v20240205.1.0

cd ../
git clone https://github.com/NVIDIA/apex
cd ./apex/ && git fetch origin && git checkout refs/tags/23.08

cd ../
git clone https://github.com/hibikaze-git/llm-jp-sft.git
cd ./llm-jp-sft/ && git fetch origin && git checkout refs/tags/ucllm_nedo_dev_v20240208.1.0

cd ../

cd ../eval
git clone https://github.com/matsuolab/llm-leaderboard.git