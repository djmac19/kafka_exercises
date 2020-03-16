import java.util.{Properties, Arrays}
import org.apache.kafka.clients.consumer.KafkaConsumer
import scala.jdk.CollectionConverters._

object Consumer extends App{
  //  def main(args: Array[String]): Unit = {
  //    consumeFromKafka("quick-start")
  //  }
  // def consumeFromKafka(topic: String) = {
  val props = new Properties()
  props.put("bootstrap.servers", "localhost:9092")
  props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("auto.offset.reset", "latest")
  props.put("group.id", "consumer-group")
  val consumer: KafkaConsumer[String, String] = new KafkaConsumer[String, String](props)
  consumer.subscribe(Arrays.asList("quick-start"))
  try {
    while (true) {
      val record = consumer.poll(1000).asScala
      for (data <- record.iterator)
        println(data.value())
    }
  }catch{
    case e:Exception => e.printStackTrace()
  }finally {
    consumer.close()
  }
  // }
}