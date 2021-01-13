import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkSessionTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("sparkSession")
      .master("local[*]")
      .getOrCreate()
    import spark.implicits._
    val userRdd: RDD[(String, Int)] = spark.sparkContext.makeRDD(List(("dyh", 29)))
    val userDF = userRdd.toDF("name", "age")
//    userDF.show(10)
//    userDF.createOrReplaceGlobalTempView("user")
//    userDF.createTempView("user")
    userDF.createOrReplaceTempView("user")
//    userDF.createGlobalTempView("user")
    val df: DataFrame = spark.sql("select * from user")
    df.select("name").show(10)
    spark.close()
  }

}
