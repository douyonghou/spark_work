
import java.util

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.parsing.json.{JSON, JSONObject}

object CleanVal {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "E:\\package\\hadoop-2.7.7");
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("test_spark1")
    val sc = new SparkContext(sparkConf)
    //  设置输出日志级别
    sc.setLogLevel("WARN")
    val line_rdd = sc.textFile("src/main/scala/event.202003240005-12")
//    val map_rdd = line_rdd.map(x => x.split("\t")(70))
    val map_rdd = line_rdd.map(x => (x.split("\t")(70),x))


//    map_rdd.filter(x => !x.isEmpty)saveAsTextFile("json.json")
    val join_rdd= map_rdd.filter(x => !x._1.isEmpty).map(str => (regJson(JSON.parseFull(str._1)),str._2))   //.foreach(str => regJson(JSON.parseFull(str)))
    join_rdd.map(x => {
//      x._2.split("\t")(70)=x._1.toString

      println(x._1.toString)

      println(x._2)
      x._2.updated(70,x._1.toString)


    }).saveAsTextFile("kkkk")
  }



  def regJson(json:Option[Any]) = json match {
    case Some(list: List[Map[String,Any]]) => {
      var arrayList = new util.ArrayList[Any]()
      list.foreach(myMap => {
        arrayList.add(JSONObject.apply(myMap.-("creditMessage")))
      })

      arrayList
    }
  }

}
