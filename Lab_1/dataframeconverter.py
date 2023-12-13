from time import strftime


class DataFrameConverter:
    def __init__(self, data_frame):
        self.data_frame = data_frame

    def save_csv(self):
        result_file_name = "ngram-{timestamp}".format(timestamp=strftime("%Y%m%d-%H%M%S"))
        self.data_frame.write.option("header", True).option("delimiter", "|").csv(result_file_name)

    def show_scheme(self):
        print("<---> ngram-{timestamp}".format(timestamp=strftime("%Y%m%d-%H%M%S")) + "<--->")
        self.data_frame.show()
        print("<---> End <--->")
