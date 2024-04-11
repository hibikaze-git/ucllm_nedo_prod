import glob
import os
import numpy as np
import json


class RecordDistributor:
    def __init__(self, dataset_dict) -> None:
        self.dataset_dict = dataset_dict

        self.update_records_per_stage()
        self.update_file_paths()

    def update_records_per_stage(self):
        sum_ratio = 0

        for dataset_info in self.dataset_dict.values():
            sum_ratio += dataset_info["stage_ratio"][0]

        for dataset_info in self.dataset_dict.values():
            stage_records = []
            dataset_info["stage_ratio"] = np.array(dataset_info["stage_ratio"])
            dataset_info["stage_ratio"] = dataset_info["stage_ratio"] / sum_ratio

            for ratio in dataset_info["stage_ratio"]:
                records = int(ratio * dataset_info["n_records"])
                stage_records.append(records)

            dataset_info["records_per_stage"] = stage_records
            print(dataset_info)

    def update_file_paths(self):
        for name, dataset_info in self.dataset_dict.items():
            pattern = os.path.join(dataset_info["data_dir"], f"**/*.{dataset_info['extension']}")
            dataset_info["file_paths"] = glob.glob(pattern, recursive=True)
            print(dataset_info["file_paths"][:5])

    def write_jsonl(self, output_path, output_dir, continue_process, overwrite=True):
        if overwrite:
            with open(output_path, "w") as f:
                f.write("")
        else:
            if os.path.exists(output_path):
                print("file already exists")
                raise FileExistsError

        # write files
        text_list = []

        for name, dataset_info in self.dataset_dict.items():
            print(name)
            text_list = dataset_info["class"].add_text_list(text_list, dataset_info, output_dir, continue_process)

        with open(output_path, "a") as f:
            for text in text_list:
                out_text = json.dumps({"text": text}, ensure_ascii=False)
                f.write(out_text + "\n")
