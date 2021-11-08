from pyspark.sql import SparkSession, Row, DataFrame
import pyspark.sql.functions as F
import pyspark.sql.types as T


def add_missing_columns(df1, df2):
  """
  """
  additional_cols = [F.lit(None).cast(field.dataType).alias(field.name) 
                     for field in df2.schema.fields if field.name not in df1.columns]
  return df1.select("*", *additional_cols)

def except_columns(df, ex = []):
  return [F.col(cl) for cl in df.columns if cl not in ex]
