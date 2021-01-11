import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, desc, lower, max}

object scalaDF {
  def main(args: Array[String]): Unit = {

   // case class Employee(FName:String, Lname:String, Sal:Int, DeptNo:Int)
   val spark_conf = new SparkConf().setAppName("Read File").setMaster("local")

    val sc = new SparkContext(spark_conf)
    var empcsv = "src/spark_emp_rdd_df_ds_data"
    var deptcsv = "src/spark_dept_rdd_df_ds_data"


    val spark_session = SparkSession.builder()
      .config(spark_conf)
      .getOrCreate()

    import spark_session.implicits._

    val empdf = spark_session.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "True")
      .load(empcsv)

   val deptdf = spark_session.read.format("csv")
     .option("header", "true")
     .option("inferSchema", "True")
     .load(deptcsv)


    empdf.printSchema();

    empdf.withColumn("max_p", max("Sal").over(Window.partitionBy()))
      .where($"Sal" === $"max_p").drop("DeptNo")
      .drop("max_p").show()


   val emp_dept= empdf.join(deptdf).where(empdf("DeptNo") === deptdf("DeptNo")).drop(empdf("DeptNo"))
   val dept_max = emp_dept.groupBy("DeptNo","DeptName")
     .sum("Sal").orderBy(desc("sum(Sal)"))


   dept_max.show()





    }
  }

