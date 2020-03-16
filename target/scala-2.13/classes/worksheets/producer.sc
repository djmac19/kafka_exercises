import java.io.ByteArrayOutputStream
import java.util.{Properties, UUID}

import org.apache.kafka.clients.producer._
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.io.{BinaryEncoder, EncoderFactory}
import org.apache.avro.specific.SpecificDatumWriter
import org.apache.kafka.common.serialization.{ByteArraySerializer, StringSerializer}

object Producer {

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

  def writeToKafka(topic: String, schema: Schema, genericRecord: GenericRecord): Unit = {
    val props = new Properties()
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
    writer.write(genericRecord, encoder)
    encoder.flush()
    out.close()
    val serializedBytes: Array[Byte] = out.toByteArray()
    val queueMessage = new ProducerRecord[String, Array[Byte]](topic, serializedBytes)
    producer.send(queueMessage)
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

val genericRecord: GenericRecord = new GenericData.Record(schema)
genericRecord.put("registration", "SE55FTW")
genericRecord.put("make", "Toyota")
genericRecord.put("model", "Yaris")
genericRecord.put("doors", "3")

Producer.writeToKafka("test", schema, genericRecord)


