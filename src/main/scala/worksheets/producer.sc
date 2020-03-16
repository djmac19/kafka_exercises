import java.io.ByteArrayOutputStream
import java.util.{Properties, UUID}

import org.apache.kafka.clients.producer._
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.io.{BinaryEncoder, EncoderFactory}
import org.apache.avro.specific.SpecificDatumWriter
import org.apache.kafka.common.serialization.{ByteArraySerializer, StringSerializer}

object MyProducer {

  def writeToKafka(topic: String, key: String, value: String): Unit = {
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String, String](props)
    val record = new ProducerRecord[String, String](topic, key, value)
    producer.send(record)
    producer.close()
  }

  def writeToKafka(topic: String, records: Map[String, String]): Unit = {
    for ((key, value) <- records) {
      writeToKafka(topic, key, value)
    }
  }

  def writeToKafka(topic: String, key: String, schema: Schema, value: GenericRecord): Unit = {
    val props: Properties = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("metadata.broker.list", "localhost:9092")
    props.put("message.send.max.retries", "5")
    props.put("request.required.acks", "-1")
    props.put("serializer.class", "kafka.serializer.DefaultEncoder")
    props.put("client.id", UUID.randomUUID().toString())
//    val configs = new ProducerConfig(props)
    val configs = ProducerConfig.addSerializerToConfig(props, new StringSerializer(), new ByteArraySerializer())
    val producer = new KafkaProducer[String, Array[Byte]](configs)

    val writer = new SpecificDatumWriter[GenericRecord](schema)
    val out = new ByteArrayOutputStream()
    val encoder: BinaryEncoder = EncoderFactory.get().binaryEncoder(out, null)
    writer.write(value, encoder)
    encoder.flush()
    out.close()
    val serializedBytes: Array[Byte] = out.toByteArray()
    val queueMessage = new ProducerRecord[String, Array[Byte]](topic, key, serializedBytes)
    producer.send(queueMessage)
  }

  def writeToKafka(topic: String, schema: Schema, records: Map[String, GenericRecord]): Unit = {
    for ((key, value) <- records) {
      writeToKafka(topic, key, schema, value)
    }
  }

}


//Producer.writeToKafka("test", "key", "value")


//val testRecords: Map[String, String] = Map("key4" -> "value4", "key5" -> "value5", "key6" -> "value6")
//Producer.writeToKafka("test", testRecords)


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

val vehicle1: GenericRecord = new GenericData.Record(schema)
vehicle1.put("registration", "SE55FTW")
vehicle1.put("make", "Toyota")
vehicle1.put("model", "Yaris")
vehicle1.put("doors", "3")

val vehicle2: GenericRecord = new GenericData.Record(schema)
vehicle2.put("registration", "DA66BLM")
vehicle2.put("make", "Vauxhall")
vehicle2.put("model", "Astra")
vehicle2.put("doors", "5")

val testRecords: Map[String, GenericRecord] = Map("vehicle1" -> vehicle1, "vehicle2" -> vehicle2)

MyProducer.writeToKafka("test", schema, testRecords)


