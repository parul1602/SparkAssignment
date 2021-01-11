from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import Window
import pyspark.sql.functions as f

from pyspark.sql.types import IntegerType

from pyspark.sql.functions import udf
sc = SparkContext(master="local", appName="spark demo")
spark = SparkSession \
    .builder \
    .appName("how to read csv file") \
    .getOrCreate()
df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("spark_emp_rdd_df_ds_data")
df1 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("spark_dept_rdd_df_ds_data")
df.printSchema()

w = Window.partitionBy()
df.withColumn('maxB', f.max('Sal').over(w)).where(f.col('Sal') == f.col('maxB'))\
    .drop('maxB')\
    .drop('DeptNo')\
    .show()

df2 = df.join(df1, how="outer", on="DeptNo")
df2.show()
df2.groupBy("DeptName").agg(f.sum('Sal')).sort('sum(Sal)', ascending=False).show()