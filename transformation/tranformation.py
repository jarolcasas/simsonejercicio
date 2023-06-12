from pyspark.sql import DataFrame, Window
import constan.constants as c
import pyspark.sql.functions as f
from pyspark.sql.types import DoubleType, DateType, FloatType , IntegerType
import utils.utils as u


class Transformation:
    def __int__(self):
        pass

    @staticmethod
    def select_columns(df: DataFrame) -> DataFrame:
        return df.select(
            c.ID,
            f.col(c.RATING).cast(DoubleType()),
            f.col(c.VOTES),
            f.col(c.NUMBER_IN_SEASON).cast(IntegerType()),
            f.col(c.NUMBER_IN_SERIES),
            f.col(c.ORIGINAL_AIR_DATE).cast(DateType()),
            f.col(c.ORIGINAL_AIR_YEAR),
            f.col(c.PRODUCTION_CODE),
            f.col(c.SEASON).cast(IntegerType()),
            f.col(c.TITLE),
            f.col(c.VIEWERS_IN_MILLIONS).cast(DoubleType())
        )

    @staticmethod
    def clean_df(df: DataFrame) -> DataFrame:
        rating_sin_nulos = f.when(f.col(c.RATING).isNull(), f.lit(c.NONE))\
            .otherwise(f.col(c.RATING))
        votes_sin_nulos = f.when(f.col(c.VOTES).isNull(), f.lit(c.NONE))\
            .otherwise(f.col(c.VOTES))
        viewers_in_millions_sin_nulos = f.when(f.col(c.VIEWERS_IN_MILLIONS).isNull(), f.lit(c.NONE))\
            .otherwise(f.col(c.VIEWERS_IN_MILLIONS))
        return df.select(*u.diff(df.columns, [c.RATING, c.VOTES, c.VIEWERS_IN_MILLIONS]),
                         rating_sin_nulos.cast(FloatType()).alias(c.RATING),
                         votes_sin_nulos.alias(c.VOTES),
                         viewers_in_millions_sin_nulos.cast(FloatType()).alias(c.VIEWERS_IN_MILLIONS))

    @staticmethod
    def best_temp(df: DataFrame) -> DataFrame:
        return df.groupBy(f.col(c.SEASON)).agg(f.sum(c.RATING).alias(c.TOTRA))

    @staticmethod
    def best_chapter(df: DataFrame) -> DataFrame:
        return df.select(f.col(c.TITLE), (f.round(f.col(c.RATING) * f.col(c.VIEWERS_IN_MILLIONS), 2)).alias(c.SCORE)) \
            .orderBy(f.col(c.SCORE).desc())

    @staticmethod
    def best_year(df: DataFrame) -> DataFrame:
        best_year = df.groupBy(f.col(c.ORIGINAL_AIR_YEAR)).agg(
            f.sum(c.VIEWERS_IN_MILLIONS).alias(c.VIEWERS_IN_MILLIONS)
        )
        return best_year.orderBy(f.col(c.VIEWERS_IN_MILLIONS).desc())

    @staticmethod
    def score(df: DataFrame) -> DataFrame:
        return df.select(*df.columns, (f.col(c.RATING) * f.col(c.VIEWERS_IN_MILLIONS)).alias(c.SCORE))\
        .orderBy(f.col(c.SCORE).desc())

    @staticmethod
    def top_chapters(df: DataFrame) -> DataFrame:
        window = Window.partitionBy(f.col(c.SEASON)).orderBy(f.col(c.SCORE).desc())
        return df.select(*df.columns, (f.row_number().over(window).alias("Top")).cast(IntegerType())) \
            .filter(f.col(c.TOP) <= 3)
