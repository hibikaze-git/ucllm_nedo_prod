from src.loaders import *
from src.RecordDistributor import RecordDistributor
import os
import yaml
import os


def make_dir(path):
    if not os.path.exists(path):
        os.mkdir(path)


make_dir("../../data")
make_dir("../../data/text")

with open("config.yaml", "r") as file:
    conf = yaml.safe_load(file)
output_path = conf["output_path"]
max_records = conf["max_records"]
print(conf)

"""
dataset_dict = {
    "wiki(ja)": {
        "class": WikiJa,
        "n_records": max_records,
        "stage_ratio": [1,],
        "data_dir": "./hf_dataset/wikipedia-20240101/data",
        "extension": "parquet"
    },

    "culture_x(ja)": {
        "class": CultureX,
        "n_records": max_records,
        "stage_ratio": [1,],
        "data_dir": "./hf_dataset/CulturaX/ja",
        "extension": "parquet"
    },
}
"""

dataset_dict = {
    "slimPajama": {
        "class": SlimPajama,
        "n_records": max_records,
        "stage_ratio": [1,],
        "data_dir": "./hf_dataset/slimPajama-627B/train",
        "extension": "jsonl.zst"
    },
}

"""
dataset_dict = {
    "wiki(ja)": {
        "class": WikiJa,
        "n_records": max_records,
        "stage_ratio": [
            1,
        ],
        "data_dir": "./hf_dataset/wikipedia-20240101/data",
        "extension": "parquet"
    },
}
"""


distributor = RecordDistributor(dataset_dict)

distributor.write_jsonl(output_path, overwrite=conf["overwrite"])
