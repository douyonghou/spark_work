import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}

object KafkaProductTest {
  def main(args: Array[String]): Unit = {
    val properties = new Properties
    properties.put("bootstrap.servers", "101.200.235.134:9092")
    properties.put("acks", "all")
    properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.put("request.timeout.ms", "600000")
    val producer = new KafkaProducer[String, String](properties)
    val metadata: RecordMetadata = producer.send(new ProducerRecord[String, String]("yundun","hello")).get

  }

}
