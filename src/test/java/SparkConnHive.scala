import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkConnHive {
  def main(args: Array[String]): Unit = {
    val hiveURL = "hdfs://101.200.235.134:9000/user/hive/warehouse"
    val spark = SparkSession.builder()
      .appName("spark-hive")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", hiveURL)
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._
    val tablesDF: DataFrame = spark.sql(
      """
        |select count(1) from douyonghou.bm
        |""".stripMargin)
    tablesDF.show(10)
    spark.close()
  }
}


