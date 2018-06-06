package main.scala.consumer

import com.google.gson.Gson
import com.mongodb.DBObject
import com.mongodb.casbah.MongoClient
import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.util.JSON
import kafka.serializer.StringDecoder
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import main.scala.pojo.IncidenciaTR

object IncidenciasTRConsumer extends App {

  val sConf = new SparkConf().setMaster("local[*]").setAppName("IncidenciasConsumer")
  val sc = new SparkContext(sConf)
  val ssc = new StreamingContext(sc, Seconds(10))

  val TOPIC="IncidenciasTR"
  val brokers = "127.0.0.1:9092"

  val kafkaParams = Map[String, String](
    "metadata.broker.list" -> brokers,
    "enable.auto.commit" -> String.valueOf(true)
  )

  val incidenciaAbierta = "SINCERRAR"

  val Database = "pfm"
  val Collection = "incidenciasCoord"
  val MongoHost = "127.0.0.1"
  val MongoPort = 27017

  val incidenciasStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, Set(TOPIC)).map(_._2)
  incidenciasStream.foreachRDD(
    rddIncidencia => {
      val sqlContext = SQLContext.getOrCreate(rddIncidencia.sparkContext)

      import sqlContext.implicits._

      val incidenciaDF = sqlContext.read.json(rddIncidencia)

      if (incidenciaDF.count() > 0) {
        println("incidenciaDF SCHEMA: ")
        incidenciaDF.printSchema()
        println("END incidenciaDF SCHEMA")
        println("incidenciaDF FIRST:" + incidenciaDF.first())

        incidenciaDF.select("Fecha").show(1)

        incidenciaDF.registerTempTable("tableAux")
        val splitted = sqlContext.sql("SELECT split(`Fecha`, ' ')[0] AS `fecha` FROM tableAux limit 1")
        val partitionPath = splitted.first().mkString
        val pathToSave = "hdfs://localhost:9000/Incidencias/" + partitionPath
        incidenciaDF.write.mode(SaveMode.Append).save(pathToSave)
        println("Guardando " + incidenciaDF + " en " + pathToSave)

        val allRows = sqlContext.sql("SELECT `Identificador` AS `id`,`Latitud`,`Longitud`,`Codigo`,`Texto`,`FechaCierre`,`Fecha` FROM tableAux")

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
              println("CODIGO: " + col.get(3))
              println("TEXTO: " + col.get(4))
              println("FECHA CIERRE: " + col.get(5))
              println("FECHA INCIDENCIA: " + col.get(6))
              var incidenciaTrafico: IncidenciaTR = null
              val id = col.get(0).toString
              val latitud = col.get(1).toString
              val longitud = col.get(2).toString
              val codigo = col.get(3).toString
              val texto = col.get(4).toString
              val fechaCierre = col.get(5).toString
              var fecha = col.get(6).toString

              println("CONECTO CON MONGO Y CONSULTO SI TENGO QUE ACTUALIZAR")
              val mongoConn = mongoClient(Database)(Collection)

              val query = MongoDBObject("idIncidencia" -> id)
              val result = mongoConn.find(query)
              if (result.size == 0) {
                  incidenciaTrafico = new IncidenciaTR(id, latitud, longitud, codigo, texto, fecha, fechaCierre)
                  println("INCIDENCIA: ")
                  incidenciaTrafico.printIncidencia
                  val jsonEstado = gson.toJson(incidenciaTrafico)
                  println("CREO DBOBJECT PARA MONGO")
                  val dBObject: DBObject = JSON.parse(jsonEstado).asInstanceOf[DBObject]
                  println("INSERTO EN MONGO")
                  mongoConn.insert(dBObject)
                  println("INSERTADO EN MONGO")
              } else if (fechaCierre.compareTo(incidenciaAbierta) != 0) {
                println("Incidencia " + id + "ya existe, nos llega el cierre")
                mongoConn.findOne(query) match {
                  case Some(result) => fecha = result.get("fechaIncidencia").asInstanceOf[String]
                }
                incidenciaTrafico = new IncidenciaTR(id, latitud, longitud, codigo, texto, fecha, fechaCierre)
                val jsonIncidencia = gson.toJson(incidenciaTrafico)
                val dBObject : DBObject = JSON.parse(jsonIncidencia).asInstanceOf[DBObject]
                println("ACTUALIZO EN MONGO")
                mongoConn.update(query, dBObject)
                println("ACTUALIZADO")
              } else {
                println("Incidencia " + id + " ya existe, nos llega de nuevo con fecha de cierre " + fechaCierre)
              }
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