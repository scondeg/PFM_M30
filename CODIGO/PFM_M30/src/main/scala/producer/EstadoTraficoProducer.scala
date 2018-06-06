package main.scala.producer

import java.util
import java.util.Properties

import main.java.FileUtilities.EstadosTraficoXMLParser
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.collection.JavaConverters._

object EstadoTraficoProducer extends App{

  var TOPIC = "EstadoTrafico"
  val props = new Properties()
  props.put("bootstrap.servers","localhost:9092")

  props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer")

  val producer = new KafkaProducer[String, String](props)

  while(true){
    val jsonMap = EstadosTraficoXMLParser.getEstadosTrafico
    if (jsonMap != null) {
      val jsonCollection = jsonMap.values()
      for (json <- jsonCollection.asScala) {
        val jsonKey = json.hashCode().toString
        println("Sending message with: " + jsonKey)
        val record = new ProducerRecord(TOPIC, jsonKey, json)
        producer.send(record)
      }
    }
    Thread.sleep(10000)
  }
  val record = new ProducerRecord(TOPIC, "key", "the end " + new java.util.Date)
  producer.send(record)
  producer.close()
}
