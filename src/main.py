from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, DoubleType
import pyspark.sql.functions as F

from delta.pip_utils import configure_spark_with_delta_pip

spark = (
    SparkSession
    .builder.master("spark://spark:7077")
    .appName("DeltaLakeFundamentals")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)

spark = configure_spark_with_delta_pip(spark).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
SCHEMA = StructType(
    [
        StructField('id', StringType(), True),          # ACCIDENT ID
        StructField('data_inversa', StringType(), True),# DATE
        StructField('dia_semana', StringType(), True),  # DAY OF WEEK
        StructField('horario', StringType(), True),     # HOUR
        StructField('uf', StringType(), True),          # BRAZILIAN STATE
        StructField('br', StringType(), True),          # HIGHWAY
        # AND OTHER FIELDS OMITTED TO MAKE THIS CODE BLOCK SMALL
    ]
)


# df_acidentes = (
#     spark
#     .read.format("csv")
#     .option("delimiter", ";")
#     .option("header", "true")
#     .option("encoding", "ISO-8859-1")
#     .schema(SCHEMA)
#     .load("/data/datatran2023.csv")
# )

# df_acidentes.show(5)


# df_acidentes\
#     .write.format("delta")\
#     .mode("overwrite")\
#     .save("/data/delta/acidentes/")

df_acidentes_delta = (
    spark
    .read.format("delta")
    .load("/data/delta/acidentes/")
)
df_acidentes_delta.select(["id", "data_inversa", "dia_semana", "horario", "uf"]).show(5)

print(df_acidentes_delta.count())




  # spark-submit --packages io.delta:delta-core_2.12:2.1.0 --master spark://spark:7077 main.py