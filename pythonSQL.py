from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

sc = SparkContext(master="local", appName="spark demo")
spark = SparkSession \
    .builder \
    .appName("how to read csv file") \
    .getOrCreate()
df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("spark_emp_rdd_df_ds_data")
df1 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("spark_dept_rdd_df_ds_data")

df.registerTempTable("df_table")
spark.sql("Select Fname,Lname,Sal from df_table where Sal=(select max(Sal) from df_table)").show()
df3 = df.join(df1, how="outer", on="DeptNo")
df3.registerTempTable("table")
spark.sql("select DeptName, sum(Sal) from table group by DeptName order by 2 desc").show()

