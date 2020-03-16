import java.util.{Properties, UUID}

import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.{ByteArraySerializer, StringSerializer}

val props = new Properties()
props.put("metadata.broker.list", "localhost:9092")
props.put("message.send.max.retries", "5")
props.put("request.required.acks", "-1")
props.put("serializer.class", "kafka.serializer.DefaultEncoder")
props.put("client.id", UUID.randomUUID().toString())

val config = ProducerConfig.addSerializerToConfig(props, new StringSerializer, new ByteArraySerializer())

println(config)