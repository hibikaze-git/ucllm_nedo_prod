from datasets import load_dataset
from tqdm import tqdm

streaming = True


class WikiJa:
    @classmethod
    def loader(cls):
        return load_dataset("hpprc/wikipedia-20240101", split="train", streaming=streaming)

    @classmethod
    def get_data(cls, item):
        text = item["text"]

        return text

    @classmethod
    def add_text_list(cls, text_list, dataset_info):
        for i, data in tqdm(enumerate(dataset_info["dataset_iterator"])):
            if i == dataset_info["records_per_stage"][0]:
                break

            text_list.append(dataset_info["class"].get_data(data))

        return text_list


class WikiEn:
    @classmethod
    def loader(cls):
        return load_dataset(
            "wikipedia",
            "20220301.en",
            split="train",
            streaming=streaming,
        )

    @classmethod
    def get_data(cls, item):
        text = item["text"]

        return text

    @classmethod
    def add_text_list(cls, text_list, dataset_info):
        for i, data in tqdm(enumerate(dataset_info["dataset_iterator"])):
            if i == dataset_info["records_per_stage"][0]:
                break

            text_list.append(dataset_info["class"].get_data(data))

        return text_list


class CultureX:
    @classmethod
    def loader(cls):
        return load_dataset(
            "uonlp/CulturaX",
            "ja",
            split="train",
            use_auth_token=True,
            streaming=streaming,
        )

    @classmethod
    def get_data(cls, item):
        text = item["text"]

        return text

    @classmethod
    def add_text_list(cls, text_list, dataset_info):
        for i, data in tqdm(enumerate(dataset_info["dataset_iterator"])):
            if i == dataset_info["records_per_stage"][0]:
                break

            text_list.append(dataset_info["class"].get_data(data))

        return text_list


class SlimPajama:
    @classmethod
    def loader(cls):
        return load_dataset(
            "cerebras/SlimPajama-627B",
            split="train",
            streaming=streaming,
        )

    @classmethod
    def get_data(cls, item):
        text = item["text"]

        return text

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

        num_records = {}
        texts = {}
        check_complete = []

        for key, value in ratio.items():
            num_records[key] = int(value * dataset_info["n_records"])
            texts[key] = []

        for i, data in tqdm(enumerate(dataset_info["dataset_iterator"])):
            set_name = data["meta"]["redpajama_set_name"]

            if len(texts[set_name]) < num_records[set_name]:
                texts[set_name].append(dataset_info["class"].get_data(data))
            else:
                if set_name not in check_complete:
                    check_complete.append(set_name)

            if set(check_complete) == set(ratio.keys()):
                break

        for text in texts.values():
            text_list += text

        return text_list
