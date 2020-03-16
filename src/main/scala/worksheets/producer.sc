import java.util.{Properties, UUID}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, ProducerConfig}
import org.apache.avro.Schema
import org.apache.kafka.common.serialization.{StringSerializer, ByteArraySerializer}
import org.apache.avro.generic.{GenericRecord, GenericData}
import org.apache.avro.specific.SpecificDatumWriter
import java.io.ByteArrayOutputStream
import org.apache.avro.io.{BinaryEncoder, EncoderFactory}


object MyProducer {

  def writeToKafka(topic: String, key: String, value: String): Unit = {
    val props: Properties = new Properties()
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

  case class Vehicle(registration: String, make: String, model: String, doors: Int)

  def writeToKafka(topic: String, key: String, schema: Schema, value: Vehicle): Unit = {
    val props: Properties = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("metadata.broker.list", "localhost:9092")
    props.put("message.send.max.retries", "5")
    props.put("request.required.acks", "-1")
    props.put("serializer.class", "kafka.serializer.DefaultEncoder")
    props.put("client.id", UUID.randomUUID().toString())

    val configs: Properties = ProducerConfig.addSerializerToConfig(props, new StringSerializer(), new ByteArraySerializer())
    val producer = new KafkaProducer[String, Array[Byte]](configs)

    val genericRecord: GenericRecord = new GenericData.Record(schema)
    genericRecord.put("registration", value.registration)
    genericRecord.put("make", value.make)
    genericRecord.put("model", value.model)
    genericRecord.put("doors", value.doors.toString)

    val writer = new SpecificDatumWriter[GenericRecord](schema)
    val out: ByteArrayOutputStream = new ByteArrayOutputStream()
    val encoder: BinaryEncoder = EncoderFactory.get().binaryEncoder(out, null)
    writer.write(genericRecord, encoder)
    encoder.flush()
    out.close()

    val serializedBytes: Array[Byte] = out.toByteArray()
    val queueMessage = new ProducerRecord[String, Array[Byte]](topic, key, serializedBytes)
    producer.send(queueMessage)
  }

  def writeToKafka(topic: String, schema: Schema, records: Map[String, Vehicle]): Unit = {
    for ((key, value) <- records) {
      writeToKafka(topic, key, schema, value)
    }
  }

}


//MyProducer.writeToKafka("test", "key", "value")


//val testRecords: Map[String, String] = Map("key4" -> "value4", "key5" -> "value5", "key6" -> "value6")
//MyProducer.writeToKafka("test", testRecords)


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

import MyProducer.Vehicle

val vehicle1 = Vehicle("SE55FTW", "Toyota", "Yaris", 3)
val vehicle2 = Vehicle("DA66BLM", "Vauxhall", "Astra", 5)
val testRecords: Map[String, Vehicle] = Map("vehicle1" -> vehicle1, "vehicle2" -> vehicle2)

MyProducer.writeToKafka("test", schema, testRecords)


