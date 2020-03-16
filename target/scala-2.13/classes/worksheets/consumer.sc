import java.util

import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import java.util.Properties

import scala.jdk.CollectionConverters._
import java.time.Duration

import kafka.serializer.DefaultDecoder
import kafka.utils.Whitelist
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.{DatumReader, DecoderFactory}
import org.apache.avro.specific.SpecificDatumReader

object Consumer {

  def consumeFromKafka(topic: String): Unit = {
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("auto.offset.reset", "earliest")
    props.put("group.id", "consumer-group")
    val consumer = new KafkaConsumer[String, String](props)
    consumer.subscribe(util.Arrays.asList(topic))
    val duration = Duration.ofMillis(1000)
    try {
//      while (true) {
        val record = consumer.poll(duration).asScala
        for (data <- record.iterator)
          println(data.value())
//      }
    } catch {
      case e:Exception => e.printStackTrace()
    } finally {
      consumer.close()
    }
  }

  def consumeObjectFromKafka(topic: String, schema: Schema): Unit = {
    val props = new Properties()
    val groupId = s"${topic}-consumer"

    props.put("group.id", groupId)
    props.put("zookeeper.connect", "localhost:9092")
    props.put("auto.offset.reset", "smallest")
    props.put("consumer.timeout.ms", "120000")
    props.put("auto.commit.interval.ms", "10000")

    val consumerConfig = new ConsumerConfig(props)
    val consumerConnector = new KafkaConsumer[String, Array[Byte]](consumerConfig)
    val filterSpec = new Whitelist(topic)
    val streams = consumerConnector.createMessageStreamsByFilter(filterSpec, 1, new DefaultDecoder(), new DefaultDecoder())(0)

    val iterator = streams.iterator()

    def read() =
      try {
        if (hasNext) {
          println("Getting message from queue.............")
          val message: Array[Byte] = iterator.next().message()
          getUser(message)
        } else {
          None
        }
      } catch {
        case ex: Exception => ex.printStackTrace()
          None
      }
    private def hasNext: Boolean =
    try
      iterator.hasNext()
    catch {
      case timeOutEx: ConsumerTimeoutException =>
        false
      case ex: Exception => ex.printStackTrace()
        println("Got error when reading message ")
        false
    }
    private def getUser(message: Array[Byte]) = {
      // Deserialize and create generic record
      val reader: DatumReader[GenericRecord] = new SpecificDatumReader[GenericRecord](schema)
      val decoder: Decoder = DecoderFactory.get().binaryDecoder(message, null)
      val userData: GenericRecord = reader.read(null, decoder)
      // Make user object
      val user = User(userData.get("id").toString.toInt, userData.get("name").toString, try {
        Some(userData.get("email").toString)
      } catch {
        case _ => None
      })
      Some(user)
    }

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