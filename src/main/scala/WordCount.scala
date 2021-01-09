import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "E:\\package\\hadoop-2.7.7");
    val sparkConf = new SparkConf().setMaster("local").setAppName("test_spark")
    val sc = new SparkContext(sparkConf)
//  设置输出日志级别
    sc.setLogLevel("WARN")
    val line_rdd = sc.textFile("src/main/scala/a.txt")
    val flat_rdd:RDD[(String)] = line_rdd.flatMap(line => line.split(" "))
    val map_rdd:RDD[(String,Int)] = flat_rdd.map(x => (x,1))
    val reduceByKey_rdd:RDD[(String,Int)] = map_rdd.reduceByKey((x,y) => x + y)
//    flat_rdd.collect().foreach(u => println(u))

//    map_rdd.collect().foreach(u => println(u._1, u._2))
//    val reduce_rdd = map_rdd.reduce((x,y) => ("共计单词数:",x._2+y._2))
//    println(reduce_rdd._1,reduce_rdd._2)

//    val reduce_group = map_rdd.groupByKey().reduce((x,y) => x+y)
  }

}
