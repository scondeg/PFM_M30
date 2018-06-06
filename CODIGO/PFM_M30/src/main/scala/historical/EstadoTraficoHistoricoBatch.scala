package main.scala.historical

import java.text.SimpleDateFormat
import java.util.Date

import com.google.gson.Gson
import com.mongodb.DBObject
import com.mongodb.casbah.MongoClient
import com.mongodb.util.JSON
import org.apache.spark.sql.{Row, SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}
import main.scala.pojo.EstadoTraficoTotalPO
import com.databricks.spark.xml
import com.mongodb.casbah.commons.MongoDBObject
import main.java.FileUtilities.HistoricosEntryPoint

object EstadoTraficoHistoricoBatch {
  def main(args: Array[String]): Unit ={

    HistoricosEntryPoint.downloadAndSaveToHdfsFile

    val Database = "pfm"
    val Collection = "estadoTraficoTotales"
    val MongoHost = "127.0.0.1"
    val MongoPort = 27017
    val hdfsUrl = "hdfs://localhost:9000/"
    val hdfsEstadosTotalesPath = "HistoricoEstadoTotales/"
    val hdfsRawPath = "RAW/"
    val hdfsToSave = "dataStored/"
    val hdfsFile = "historicousuarios.xml"

    val sConf = new SparkConf().setMaster("local[*]").setAppName("EstadoTraficoHistorico")
    val sc = new SparkContext(sConf)
    val sqlContext = SQLContext.getOrCreate(sc)

    val estadoTotalesDF = sqlContext.read.format("com.databricks.spark.xml").option("rowTag", "Historico").load(hdfsUrl + hdfsEstadosTotalesPath + hdfsRawPath + hdfsFile)
    val estadoTotalesFechaActualización = sqlContext.read.format("com.databricks.spark.xml").option("rowTag", "campo").load(hdfsUrl + hdfsEstadosTotalesPath + hdfsRawPath + hdfsFile)

    println("estadoTotalesDF SCHEMA: ")
    estadoTotalesDF.printSchema()
    println("END estadoTotalesDF SCHEMA")
    println("estadoDF FIRST:" + estadoTotalesDF.first())
    estadoTotalesDF.show()

    println("COUNT: " + estadoTotalesDF.count())
    println("COUNT2: " + estadoTotalesFechaActualización.count())

    if (estadoTotalesDF.count() > 0) {
      estadoTotalesDF.registerTempTable("tableAux")
      val pathToSave = hdfsUrl + hdfsEstadosTotalesPath + hdfsToSave
      estadoTotalesDF.write.mode(SaveMode.Overwrite).save(pathToSave)
      println("Guardando en HDFS " + estadoTotalesDF + " en " + pathToSave)

      val mongoClient = MongoClient(MongoHost, MongoPort)
      val mongoConn = mongoClient(Database)(Collection)
      val ultimoEstadoAlmacenado = mongoConn.find().sort(MongoDBObject("fecha" -> -1)).one()

      var ultimaFechaAlmacenada = "1999-01-01"
      if (ultimoEstadoAlmacenado != null) ultimaFechaAlmacenada = ultimoEstadoAlmacenado.get("fecha").toString
      println("ULTIMA FECHA ALMACENADA: " + ultimaFechaAlmacenada)
      mongoClient.close()

      //TODO mirar se ya no hace falta meter la tabla Aux que ya esta más arriba
      estadoTotalesDF.registerTempTable("tableAux")
      val allRows = sqlContext.sql("SELECT " +
        "TO_DATE(CAST(UNIX_TIMESTAMP(`Fecha`, 'dd/MM/yyyy') AS Timestamp)), " +
        "split(`UsuariosCalle30`, ' ')[0] AS UsuariosCalle30, " +
        "IF(`distanciaMediaRecorrida` = 'Indeterminate metros', 0, split(`distanciaMediaRecorrida`, ' ')[0]) AS distanciaMediaRecorrida, " +
        "IF((split(`tiempoMediodeRecorrido`, ' ')[0]) = 'Indeterminate', 0, (split(tiempoMediodeRecorrido, ' ')[0]*60 + split(tiempoMediodeRecorrido, ' ')[2])) AS tiempoMediodeRecorrido, " +
        "split(`vehxKmRamales`, ' ')[0] AS vehxKmRamales, " +
        "split(`vehxKmTotales`, ' ')[0] AS vehxKmTotales, " +
        "split(`velocidadMedia`, ' ')[0] AS velocidadMedia " +
        "FROM tableAux " +
        "WHERE TO_DATE(CAST(UNIX_TIMESTAMP(`Fecha`, 'dd/MM/yyyy') AS Timestamp)) > '" + ultimaFechaAlmacenada + "'")

      if (allRows != null && allRows.count() > 0) {
        println("Imprimo")
        allRows.show()
        allRows.schema

        allRows.foreachPartition{ row =>

          val mongoClient = MongoClient(MongoHost, MongoPort)
          val gson = new Gson()
          row.toSeq.foreach { col =>

            var estadoTraficoTotalPO: EstadoTraficoTotalPO = null

            val fecha = col.get(0).toString
            val usuariosCalle = col.get(1).toString
             val distanciaMediaRecorrida = col.get(2).toString
             val tiempoMedioRecorrido = col.get(3).toString
             val vehxKmRamales = col.get(4).toString
             val vehXKmTotales = col.get(5).toString
             val velocidadMedia  = col.get(6).toString

            estadoTraficoTotalPO = new EstadoTraficoTotalPO(fecha, usuariosCalle, distanciaMediaRecorrida, tiempoMedioRecorrido, vehxKmRamales, vehXKmTotales, velocidadMedia)
            println("ESTADO TRAFICO TOTAL: " + estadoTraficoTotalPO.printEstadoTraficoTotal)

            val jsonEstadoTotal = gson.toJson(estadoTraficoTotalPO)
            val mongoConn = mongoClient(Database)(Collection)
            val dBObject: DBObject = JSON.parse(jsonEstadoTotal).asInstanceOf[DBObject]
            println("INSERTO EN MONGO")
            mongoConn.insert(dBObject)
            println("INSERTADO EN MONGO")
          }
          mongoClient.close()
        }
      } else {
        println("\n¡¡¡¡¡¡¡¡¡ No hay nada que procesar !!!!!!!!!")
      }

    }
  }
}
