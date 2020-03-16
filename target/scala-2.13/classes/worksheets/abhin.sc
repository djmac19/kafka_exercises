import java.util.Properties
import org.apache.kafka.clients.consumer.KafkaConsumer
import java.util
import scala.jdk.CollectionConverters._
import java.time.Duration


object Consumer extends App{

   def consumeFromKafka(topic: String): Unit = {
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("auto.offset.reset", "latest")
    props.put("group.id", "consumer-group")
    val consumer: KafkaConsumer[String, String] = new KafkaConsumer[String, String](props)
    consumer.subscribe(util.Arrays.asList(topic))
    val duration = Duration.ofMillis(1000)
    try {
      while (true) {
        val record = consumer.poll(duration).asScala
        for (data <- record.iterator)
          println(data.value())
      }
    }catch{
      case e:Exception => e.printStackTrace()
    }finally {
      consumer.close()
    }
  }

}

Consumer.consumeFromKafka("testNew")