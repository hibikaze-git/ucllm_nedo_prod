from datasets import load_dataset
from tqdm import tqdm

streaming = True


class WikiJa:
    @classmethod
    def add_text_list(cls, text_list, dataset_info):
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
    def add_text_list(cls, text_list, dataset_info):
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
    def add_text_list(cls, text_list, dataset_info):
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
    def add_text_list(cls, text_list, dataset_info):
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

        for key, value in ratio.items():
            num_records[key] = int(value * dataset_info["n_records"])
            texts[key] = []

        stop_extract = False

        for file_path in dataset_info["file_paths"]:
            if stop_extract:
                break

            dataset_iterator = load_dataset("json", data_files=file_path, split="train")

            for i, data in tqdm(enumerate(dataset_iterator)):
                set_name = data["meta"]["redpajama_set_name"]

                if len(texts[set_name]) < num_records[set_name]:
                    texts[set_name].append(data["text"])
                else:
                    if set_name not in check_complete:
                        check_complete.append(set_name)

                if set(check_complete) == set(ratio.keys()):
                    stop_extract = True
                    break

        for text in texts.values():
            text_list += text

        return text_list
