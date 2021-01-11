import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, desc, lower, max}

object scalaSQL {
  def main(args: Array[String]): Unit = {

    // case class Employee(FName:String, Lname:String, Sal:Int, DeptNo:Int)
    val spark_conf = new SparkConf().setAppName("Read File").setMaster("local")


    val sc = new SparkContext(spark_conf)
    var empcsv = "src/spark_emp_rdd_df_ds_data"
    var deptcsv = "src/spark_dept_rdd_df_ds_data"

    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._

    import spark.implicits._
    val empdf = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "True")
      .load(empcsv)

    val deptdf = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "True")
      .load(deptcsv)

    empdf.createGlobalTempView("emptable")
    deptdf.createGlobalTempView("deptable")


    println("Show Fname, LName of highest paid person")
    spark.sql("Select Fname,Lname,Sal from global_temp.emptable where Sal=(select max(Sal) from global_temp.emptable)").show()

    val df = empdf.join(deptdf, empdf("DeptNo") === deptdf("DeptNo"))
    df.createGlobalTempView("final")

    println("Show DeptName and Total salary for department costing most money")
    spark.sql("select DeptName, sum(Sal) from global_temp.final group by DeptName order by 2 desc").show()


  }
}
