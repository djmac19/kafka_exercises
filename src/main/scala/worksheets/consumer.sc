import java.util.{Properties, Arrays}
import org.apache.kafka.clients.consumer.{KafkaConsumer, ConsumerRecord, ConsumerConfig}
import java.time.Duration
import scala.jdk.CollectionConverters._
import org.apache.avro.Schema
import org.apache.kafka.common.serialization.{StringDeserializer, ByteArrayDeserializer}
import org.apache.avro.specific.SpecificDatumReader
import org.apache.avro.generic.GenericRecord
import java.io.ByteArrayInputStream
import org.apache.avro.io.{BinaryDecoder, DecoderFactory}


object MyConsumer {

  def consumeFromKafka(topic: String): Unit = {
    val props: Properties = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("auto.offset.reset", "earliest")
    props.put("group.id", "consumer-group")

    val consumer = new KafkaConsumer[String, String](props)
    consumer.subscribe(Arrays.asList(topic))

    val duration: Duration = Duration.ofMillis(1000)

    try {
//      while (true) {
        val record: Iterable[ConsumerRecord[String, String]] = consumer.poll(duration).asScala
        for (data <- record.iterator)
          println(data.value())
//      }
    } catch {
      case e:Exception => e.printStackTrace()
    } finally {
      consumer.close()
    }
  }

  case class Vehicle(registration: String, make: String, model: String, doors: Int)

  def consumeFromKafka(topic: String, schema: Schema): Unit = {
    val props: Properties = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("group.id", "test-consumer")
    props.put("zookeeper.connect", "localhost:9092")
    props.put("auto.offset.reset", "earliest")
    props.put("consumer.timeout.ms", "120000")
    props.put("auto.commit.interval.ms", "10000")

    val configs: Properties = ConsumerConfig.addDeserializerToConfig(props, new StringDeserializer(), new ByteArrayDeserializer())
    val consumer = new KafkaConsumer[String, Array[Byte]](configs)
    consumer.subscribe(Arrays.asList(topic))

    val duration: Duration = Duration.ofMillis(10000)
    val record: Iterable[ConsumerRecord[String, Array[Byte]]] = consumer.poll(duration).asScala

    for (data <- record.iterator) {
      val reader = new SpecificDatumReader[GenericRecord](schema)
      val message: ByteArrayInputStream = new ByteArrayInputStream(data.value)
      val decoder: BinaryDecoder = DecoderFactory.get().binaryDecoder(message, null)
      val vehicleData: GenericRecord = reader.read(null, decoder)
      val vehicle = Vehicle(vehicleData.get("registration").toString, vehicleData.get("make").toString, vehicleData.get("model").toString, vehicleData.get("doors").toString.toInt)
      println(vehicle)
    }
  }

}


//MyConsumer.consumeFromKafka("test")


val SCHEMA_STRING: String = """{
  "namespace": "test",
  "type": "record",
  "name": "vehicle",
  "fields": [
    {"name": "registration", "type": "string"},
    {"name": "make", "type": "string"},
    {"name": "model", "type": "string"},
    {"name": "doors", "type": "string"}
  ]
}"""

val schema: Schema = new Schema.Parser().parse(SCHEMA_STRING)

MyConsumer.consumeFromKafka("test", schema)