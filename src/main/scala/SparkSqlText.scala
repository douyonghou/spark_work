import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, SparkSession}


object SparkSqlText {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("sparkSQL").setMaster("local[*]")
    val spark: SparkSession= SparkSession.builder().config(conf).getOrCreate()
    spark.sql("select 'aaa'").show(10)
    import spark.implicits._
    val rdd = spark.sparkContext.makeRDD(List(("001", "核事故", "重大事件"), ("002", "生化危机", "重大事件"), ("003", "自杀事件", "较为严重")))
    // rdd转df
    val df = rdd.toDF("orgId", "sglx", "sgdj")

    df.show(10)
    // df 转ds
    val ds: Dataset[Sgzb] = df.as[Sgzb]
    ds.mapPartitions{
      x => x.map{
        x => x.orgId
      }
    }
    val frame = ds.toDF("orgId", "sglx", "sgdj")
    ds.show(10)
    // df转rdd
    val rdd1 = df.rdd
    // ds转rdd
    val rdd2 = ds.rdd


    spark.stop()
  }
}
case class Sgzb(orgId: String, sglx: String, sgdj: String)
