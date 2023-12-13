from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    lower,
    explode,
    regexp_replace,
    collect_list,
    split,
    flatten,
    size,
    concat_ws,
    expr,
)
from pyspark.ml.feature import NGram


class NGramConverter:
    def __init__(self, ngram_num, file_path):
        self.spark = (
            SparkSession(SparkContext.getOrCreate())
            .builder.master("local[*]")
            .appName("Laboratory Work #1")
            .getOrCreate()
        )
        self.ngram = NGram(n=ngram_num, inputCol="words", outputCol='ngram_column')
        self.json = self.spark.read.json(file_path)

    def get_ngrams(self, event_type):
        data_frame = (self.json.filter(f"type = '{event_type}'")
                      .select(explode("payload.commits").alias("commit"))
                      .select(lower("commit.author.name").alias("author"), lower("commit.message").alias("message"))
                      .withColumn("message", regexp_replace("message", "[^a-zA-Z0-9\\s]", ""))
                      .withColumn("message", (split("message", "\\s+")))
                      .withColumn("message", expr("filter(message, element -> element != '')"))
                      .groupBy("author")
                      .agg(flatten(collect_list("message")).alias("words")))

        return (
            self.ngram.transform(data_frame)
            .select("author", 'ngram_column')
            .filter(size('ngram_column') > 0)
            .withColumn('ngram_column', concat_ws(", ", 'ngram_column'))
        )

