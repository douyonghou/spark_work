package com.study.kafka
import com.alibaba.fastjson.{JSON, JSONException}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
object SparkConumerKafka {

  //  private val logger = LoggerFactory.getLogger(PVUV.getClass)

  def main(args: Array[String]): Unit = {
//    System.setProperty("hadoop.home.dir", "E:\\package\\hadoop-2.7.7");
    //    System.setProperty("HADOOP_USER_NAME", "hadoop")
    val conf = new SparkConf().setAppName("SparkConumerKafka").setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val streamContext = new StreamingContext(spark.sparkContext, Seconds(5))
    //直连方式相当于跟kafka的Topic至直接连接
    //"auto.offset.reset:earliest(每次重启重新开始消费)，latest(重启时会从最新的offset开始读取)
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "101.200.235.134:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "group01",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val topics = Array("yundun")
    val kafkaDStream = KafkaUtils.createDirectStream[String, String](
      streamContext,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
    )
    //如果使用SparkStream和Kafka直连方式整合，生成的kafkaDStream必须调用foreachRDD
    kafkaDStream.foreachRDD(kafkaRDD => {
      if (!kafkaRDD.isEmpty()) {
        //获取当前批次的RDD的偏移量
        val offsetRanges = kafkaRDD.asInstanceOf[HasOffsetRanges].offsetRanges

        //拿出kafka中的数据
        val lines: RDD[String] = kafkaRDD.map(_.value())

        lines.foreach(println(_))
        /*        //将lines字符串转换成json对象
                val logBeanRDD = lines.map(line => {
                  var logBean: LogBean = null
                  try {
                    logBean = JSON.parseObject(line, classOf[LogBean])
                  } catch {
                    case e: JSONException => {
                      //logger记录
        //              logger.error("json解析错误！line:" + line, e)
                    }
                  }
                  logBean
                })*/

        /*  //过滤
          val filteredRDD = logBeanRDD.filter(_ != null)

          //将RDD转化成DataFrame,因为RDD中装的是case class
          import spark.implicits._

          val df = filteredRDD.toDF()

          df.show()
          //将数据写到hdfs中:hdfs://hd1:9000/360
          df.repartition(1).write.mode(SaveMode.Append).parquet(args(0))
  */
        //提交当前批次的偏移量，偏移量最后写入kafka
        kafkaDStream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
      }
    })

    //启动
    streamContext.start()
    streamContext.awaitTermination()
    streamContext.stop()

  }

}
//
//case class LogBean(time:String,
//                   longitude:Double,
//                   latitude:Double,
//                   openid:String,
//                   page:String,
//                   evnet_type:Int)
/*

export PSARK_HOME=/opt/spark-2.1.1-bin-hadoop2.7/bin/
$PSARK_HOME/spark-submit --class com.study.kafka.SparkConumerKafka \
--master yarn \
--deploy-mode cluster \
--driver-memory 4g \
--executor-memory 2g \
--executor-cores 1 \
lib/spark-examples*.jar \
10


export PSARK_HOME=/opt/spark-2.4.1-bin-hadoop2.7/bin
$PSARK_HOME/spark-submit --class com.study.kafka.SparkConumerKafka \
--master yarn \
--name SparkConumerKafka \
--deploy-mode cluster \
spark_work-1.0-SNAPSHOT-jar-with-dependencies.jar
yarn logs -applicationId application_1612295319417_0003  -out ./info

sh kafka-server-start.sh ../config/server.properties
sh zookeeper-server-start.sh ../config/zookeeper.properties
*/
