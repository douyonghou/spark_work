import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

object SparkHDFS {
  def main(args: Array[String]): Unit = {
    val hiveURL = "hdfs://101.200.235.134:9000/user/hive/warehouse"
    val spark = SparkSession.builder()
      .appName("spark-hdfs")
      .master("local[*]")
      .getOrCreate()
    val hadoopConf = spark.sparkContext.hadoopConfiguration
    val hdfs = FileSystem.get(hadoopConf)
    if (hdfs.exists(new Path(hiveURL))){
      println(hdfs.getScheme)
    }
  }
}


