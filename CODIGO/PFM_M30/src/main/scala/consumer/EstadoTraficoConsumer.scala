package main.scala.consumer

import java.util
import java.util.Properties
import com.google.gson.Gson
import com.mongodb.{DBObject,Mongo}
import com.mongodb.casbah.{Imports, MongoClient, MongoConnection}
import com.mongodb.util.JSON
import kafka.serializer.StringDecoder
import org.apache.spark.sql.{DataFrame, Encoders, SQLContext, SaveMode}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import main.scala.pojo.EstadoTrafico

object EstadoTraficoConsumer extends App {

  val sConf = new SparkConf().setMaster("local[*]").setAppName("EstadoTraficoConsumer")
  val sc = new SparkContext(sConf)
  val ssc = new StreamingContext(sc, Seconds(10))

  val TOPIC="EstadoTrafico"
  val brokers = "127.0.0.1:9092"

  val kafkaParams = Map[String, String](
    "metadata.broker.list" -> brokers,
    "enable.auto.commit" -> String.valueOf(true)
  )

  val Database = "pfm"
  val Collection = "estadoTraficoCoord"
  val MongoHost = "127.0.0.1"
  val MongoPort = 27017

  val estadoTraficoStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, Set(TOPIC)).map(_._2)
  estadoTraficoStream.foreachRDD(
    rddEstado => {
      val sqlContext = SQLContext.getOrCreate(rddEstado.sparkContext)
      import sqlContext.implicits._

      val estadoDF = sqlContext.read.json(rddEstado)

      if (estadoDF.count() > 0) {
        println("estadoDF SCHEMA: ")
        estadoDF.printSchema()
        println("END estadoDF SCHEMA")
        println("estadoDF FIRST:" + estadoDF.first())

        estadoDF.select("Fecha").show(1)

        estadoDF.registerTempTable("tableAux")
        val splitted = sqlContext.sql("SELECT split(`Fecha`, ' ')[0] AS `fecha` FROM tableAux limit 1")
        val partitionPath = splitted.first().mkString
        val pathToSave = "hdfs://localhost:9000/EstadosTraficoCoord/" + partitionPath
        estadoDF.write.mode(SaveMode.Append).save(pathToSave)
        println("Guardando " + estadoDF + " en " + pathToSave)

        val allRows = sqlContext.sql("SELECT CONCAT(`Latitud`,`Longitud`,`Fecha`) AS `id`,`Latitud`,`Longitud`,`Color`,`Fecha` FROM tableAux")

        if (allRows != null && allRows.count() > 0) {
          println("Imprimo")
          allRows.show
          allRows.schema

          allRows.foreachPartition { row =>
            val mongoClient = MongoClient(MongoHost, MongoPort)
            val gson = new Gson()
            row.toSeq.foreach { col =>
              println("ID: " + col.get(0))
              println("LATITUD: " + col.get(1))
              println("LONGITUD: " + col.get(2))
              println("COLOR: " + col.get(3))
              println("FECHA: " + col.get(4))
              var estadoTrafico: EstadoTrafico = null
              val id = col.get(0).toString
              val latitud = col.get(1).toString
              val longitud = col.get(2).toString
              val color = col.get(3).toString
              val fecha = col.get(4).toString

              estadoTrafico = new EstadoTrafico(id, latitud, longitud, color, fecha)
              println("ESTADO TRAFICO: ")
              estadoTrafico.printEstadoTrafico
              val jsonEstado = gson.toJson(estadoTrafico)
              println("CONECTO CON MONGO")
              val mongoConn = mongoClient(Database)(Collection)
              println("CREO DBOBJECT PARA MONGO")
              val dBObject : DBObject = JSON.parse(jsonEstado).asInstanceOf[DBObject]
              println("INSERTO EN MONGO")
              mongoConn.insert(dBObject)
              println("INSERTADO EN MONGO")
            }
            mongoClient.close()
          }
        }
      } else {
        println("Empty DF")
      }
    })

  ssc.start()
  ssc.awaitTermination()
}
