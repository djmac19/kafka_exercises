import java.io.ByteArrayInputStream
import java.util
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, KafkaConsumer}
import java.util.Properties
import scala.jdk.CollectionConverters._
import java.time.Duration
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.{BinaryDecoder, DatumReader, DecoderFactory}
import org.apache.avro.specific.SpecificDatumReader
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, StringDeserializer}

object MyConsumer {

  def consumeFromKafka(topic: String): Unit = {
    val props: Properties = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("auto.offset.reset", "earliest")
    props.put("group.id", "consumer-group")
    val consumer = new KafkaConsumer[String, String](props)
    consumer.subscribe(util.Arrays.asList(topic))
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

  def consumeFromKafka(topic: String, schema: Schema): Unit = {
    val props: Properties = new Properties()
    val groupId: String = "test-consumer"
    props.put("bootstrap.servers", "localhost:9092")
    props.put("group.id", groupId)
    props.put("zookeeper.connect", "localhost:9092")
    props.put("auto.offset.reset", "earliest")
    props.put("consumer.timeout.ms", "120000")
    props.put("auto.commit.interval.ms", "10000")

    val config: Properties = ConsumerConfig.addDeserializerToConfig(props, new StringDeserializer(), new ByteArrayDeserializer())
    val consumer = new KafkaConsumer[String, Array[Byte]](config)
    consumer.subscribe(util.Arrays.asList(topic))
    val duration: Duration = Duration.ofMillis(10000)
    val record: Iterable[ConsumerRecord[String, Array[Byte]]] = consumer.poll(duration).asScala
//    println(record)
    for (data <- record.iterator) {
//      println(data)
//      println("hello")
      val reader: DatumReader[GenericRecord] = new SpecificDatumReader[GenericRecord](schema)
      val message = new ByteArrayInputStream(data.value)
      val decoder: BinaryDecoder = DecoderFactory.get().binaryDecoder(message, null)
      val vehicleData: GenericRecord = reader.read(null, decoder)
      case class Vehicle(registration: String, make: String, model: String, doors: Int)
      val vehicle = Vehicle(vehicleData.get("registration").toString, vehicleData.get("make").toString, vehicleData.get("model").toString, vehicleData.get("doors").toString.toInt)
      println(vehicle)
    }

//    def read() =
//      try {
//        if (hasNext) {
//          println("Getting message from queue.............")
//          val message: Array[Byte] = iterator.next().message()
//          getUser(message)
//        } else {
//          None
//        }
//      } catch {
//        case ex: Exception => ex.printStackTrace()
//          None
//      }
//    private def hasNext: Boolean =
//    try
//      iterator.hasNext()
//    catch {
//      case timeOutEx: ConsumerTimeoutException =>
//        false
//      case ex: Exception => ex.printStackTrace()
//        println("Got error when reading message ")
//        false
//    }
//    private def getUser(message: Array[Byte]) = {
//      // Deserialize and create generic record
//
//
//
//      // Make user object
//      , try {
//        Some(userData.get("email").toString)
//      } catch {
//        case _ => None
//      })
//      Some(user)
//    }

  }

}

//Consumer.consumeFromKafka("test")


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