from dataframeconverter import DataFrameConverter
from ngramconverter import NGramConverter

EVENT_TYPE = "PushEvent"
INPUT_FILE_PATH = "10K.github.jsonl"
NGRAM_NUM = 3


def main():
    ngram_converter = NGramConverter(NGRAM_NUM, INPUT_FILE_PATH)
    ngrams = ngram_converter.get_ngrams(EVENT_TYPE)

    data_frame_converter = DataFrameConverter(ngrams)
    data_frame_converter.save_csv()


if __name__ == "__main__":
    main()
