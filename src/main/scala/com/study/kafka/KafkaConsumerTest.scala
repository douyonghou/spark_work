package com.study.kafka
import java.util.{Collections, Properties}
import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}
object KafkaConsumerTest {
  def main(args: Array[String]): Unit = {
    val prop = new Properties
    prop.put("bootstrap.servers", "101.200.235.134:9092")
    prop.put("group.id", "group01")
    prop.put("auto.offset.reset", "earliest")
    prop.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    prop.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    prop.put("enable.auto.commit", "true")
    prop.put("session.timeout.ms", "30000")
    val kafkaConsumer = new KafkaConsumer[String, String](prop)
    kafkaConsumer.subscribe(Collections.singletonList("yundun"))
    while (true) {
      val msgs: ConsumerRecords[String, String] = kafkaConsumer.poll(2000)
      val it = msgs.iterator()
      while (it.hasNext) {
        val msg = it.next()
        println(s"partition: ${msg.partition()}, offset: ${msg.offset()}, key: ${msg.key()}, value: ${msg.value()}")
      }
    }

  }
}
