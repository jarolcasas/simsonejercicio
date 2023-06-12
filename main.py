import constan.constants as c
from transformation.tranformation import Transformation
from pyspark.sql import SparkSession


def main():

    spark = SparkSession.builder.appName(c.APP_NAME).master(c.MODE).getOrCreate()
    simpson_df = spark.read.option(c.HEADER, c.TRUE_STRING).option('delimiter', c.DELIMITER).csv(c.INPUT_PATH)
    simpson_df.show()
    t = Transformation

    select_columns_df = t.select_columns(simpson_df)
    #select_columns_df.show()
    clean_df = t.clean_df(select_columns_df)
    #clean_df.show()
    best_df = t.best_temp(clean_df)
    best_df.show()
    score_df = t.score(clean_df)
    score_df.show()
    best_year_df = t.best_year(clean_df)
    best_year_df.show()
    best_chapter = t.best_chapter(clean_df)
    best_chapter.show()
    top = t.top_chapters(score_df)
    top.show()
    top.write.mode(c.MODE_OVER).parquet(c.OUTPUT)
    #top.write.mode(c.MODE_OVER).parquet(c.OUTPUT)


if __name__ == "__main__":
    main()
