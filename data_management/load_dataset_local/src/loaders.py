import json

from datetime import datetime
from datasets import load_dataset
from tqdm import tqdm


class WikiJa:
    @classmethod
    def add_text_list(cls, text_list, dataset_info, output_dir, continue_process):
        stop_extract = False

        for file_path in dataset_info["file_paths"]:
            if stop_extract:
                break

            dataset_iterator = load_dataset(dataset_info["extension"], data_files=file_path, split="train")

            for i, data in tqdm(enumerate(dataset_iterator)):
                if i == dataset_info["records_per_stage"][0]:
                    stop_extract = True
                    break

                text_list.append(data["text"])

        return text_list


class WikiEn:
    @classmethod
    def add_text_list(cls, text_list, dataset_info, output_dir, continue_process):
        stop_extract = False

        for file_path in dataset_info["file_paths"]:
            if stop_extract:
                break

            dataset_iterator = load_dataset(dataset_info["extension"], data_files=file_path, split="train")

            for i, data in tqdm(enumerate(dataset_iterator)):
                if i == dataset_info["records_per_stage"][0]:
                    stop_extract = True
                    break

                text_list.append(data["text"])

        return text_list


class CultureX:
    @classmethod
    def add_text_list(cls, text_list, dataset_info, output_dir, continue_process):
        stop_extract = False

        for file_path in dataset_info["file_paths"]:
            if stop_extract:
                break

            dataset_iterator = load_dataset(dataset_info["extension"], data_files=file_path, split="train")

            for i, data in tqdm(enumerate(dataset_iterator)):
                if i == dataset_info["records_per_stage"][0]:
                    stop_extract = True
                    break

                text_list.append(data["text"])

        return text_list


class SlimPajama:
    @classmethod
    def add_text_list(cls, text_list, dataset_info, output_dir, continue_process):
        ratio = {
            "RedPajamaCommonCrawl": 0.67,
            "RedPajamaC4": 0.15,
            "RedPajamaGithub": 0.045,
            "RedPajamaWikipedia": 0.045,
            "RedPajamaBook": 0.045,
            "RedPajamaArXiv": 0.025,
            "RedPajamaStackExchange": 0.02,
        }

        """
        ratio = {
            "RedPajamaCommonCrawl": 0.3,
            "RedPajamaC4": 0.2,
            "RedPajamaGithub": 0.1,
            "RedPajamaWikipedia": 0.1,
            "RedPajamaBook": 0.1,
            "RedPajamaArXiv": 0.1,
            "RedPajamaStackExchange": 0.1,
        }
        """

        num_records = {}

        texts = {}
        check_complete = []
        processed_file_paths = []
        processed_num_records = {}

        for key, value in ratio.items():
            num_records[key] = int(value * dataset_info["n_records"])
            texts[key] = []

        if continue_process:
            with open(output_dir + "check_complete.json", "r") as f:
                check_complete = json.load(f)

            with open(output_dir + "processed_file_paths.json", "r") as f:
                processed_file_paths = json.load(f)

            with open(output_dir + "processed_num_records.json", "r") as f:
                processed_num_records = json.load(f)
        else:
            for key, value in ratio.items():
                processed_num_records[key] = 0

        print(check_complete)
        print(processed_file_paths)
        print(processed_num_records)

        stop_extract = False

        file_paths = [item for item in dataset_info["file_paths"] if item not in processed_file_paths]

        print(file_paths)

        for i, file_path in enumerate(file_paths):
            if stop_extract:
                break

            dataset_iterator = load_dataset("json", data_files=file_path, split="train")
            print(file_path)

            for data in tqdm(dataset_iterator):
                set_name = data["meta"]["redpajama_set_name"]

                if processed_num_records[set_name] < num_records[set_name]:
                    texts[set_name].append(data["text"])
                    processed_num_records[set_name] += 1
                else:
                    if set_name not in check_complete:
                        check_complete.append(set_name)

                if set(check_complete) == set(ratio.keys()):
                    stop_extract = True
                    print("all complete")
                    break

            processed_file_paths.append(file_path)

            if i % 100 == 0:
                tmp_text_list = []
                for text in texts.values():
                    tmp_text_list += text

                datetime_str = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")

                with open(output_dir + f"tmp/texts_{datetime_str}.jsonl", "w") as f:
                    for text in tmp_text_list:
                        out_text = json.dumps({"text": text}, ensure_ascii=False)
                        f.write(out_text + "\n")

                with open(output_dir + "check_complete.json", "w") as f:
                    f.write(json.dumps(check_complete, ensure_ascii=False))

                with open(output_dir + "processed_file_paths.json", "w") as f:
                    f.write(json.dumps(processed_file_paths, ensure_ascii=False))

                with open(output_dir + "processed_num_records.json", "w") as f:
                    f.write(json.dumps(processed_num_records, ensure_ascii=False))

                for key, value in ratio.items():
                    texts[key] = []

        print(check_complete)

        for text in texts.values():
            text_list += text

        return text_list
