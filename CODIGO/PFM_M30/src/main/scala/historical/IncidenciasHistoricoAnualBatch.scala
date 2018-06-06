package main.scala.historical

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import main.java.FileUtilities.HistoricoIncidenciasAnualHelper
import com.google.gson.Gson
import com.mongodb.DBObject
import com.mongodb.casbah.MongoClient
import com.mongodb.util.JSON
import org.apache.spark.sql.{Row, SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}
import com.databricks.spark.csv
import com.mongodb.casbah.commons.MongoDBObject
import main.scala.pojo.IncidenciaHistoricoAnual
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import sun.misc.VM

object IncidenciasHistoricoAnualBatch {
  def main(args: Array[String]): Unit ={


    HistoricoIncidenciasAnualHelper.storeRawToHdfs()

    val Database = "pfm"
    val Collection = "historicoAnualIncidencias"
    val MongoHost = "127.0.0.1"
    val MongoPort = 27017
    val hdfsUrl = "hdfs://localhost:9000/"
    val hdfsIncidenciasHistoricoPath = "IncidenciasHistoricoAnual/"
    val hdfsRawPath = "RAW/*"
    val hdfsToSave = "dataStored/"
    val hdfsFile = "Listado_accidentes_*.csv"

    val sConf = new SparkConf().setMaster("local[*]").setAppName("IncidenciasHistoricoAnualBatch")
    val sc = new SparkContext(sConf)
    val sqlContext = SQLContext.getOrCreate(sc)

    val rawPath = hdfsUrl + hdfsIncidenciasHistoricoPath + hdfsRawPath
    val processedPath = "/" + hdfsIncidenciasHistoricoPath + hdfsRawPath

    val data = sc.wholeTextFiles(rawPath)
    val files = data.map { case (filename, content) => filename}


    def processFile(file: String) = {
      println (file);
      val pathFolders = file.split('/');
      val year =  pathFolders(5)
      val partition = year + "/";

      var schema = StructType(Array(
        StructField("Fecha", StringType,true),
        StructField("Hora", StringType,true),
        StructField("Dia", StringType,true),
        StructField("Incidencia", StringType,true),
        StructField("sitrem", StringType,true),
        StructField("Carretera", StringType,true),
        StructField("MC30", StringType,true),
        StructField("Calzada", StringType,true),
        StructField("Localizacion", StringType,true),
        StructField("Enlace", StringType,true),
        StructField("TipoAccidente", StringType,true),
        StructField("TipoColision", StringType,true),
        StructField("NVehiculos", StringType,true),
        StructField("NTurismos", StringType,true),
        StructField("NMotocicletas", StringType,true),
        StructField("NVehiculosPesados", StringType,true),
        StructField("HL", StringType,true),
        StructField("HG", StringType,true),
        StructField("VM", StringType,true),
        StructField("DaniosMobiliario", StringType,true),
        StructField("Circulacion", StringType,true),
        StructField("EstadoFirme", StringType,true),
        StructField("PresenciaRRExt", StringType,true),
        StructField("CorteCarril", StringType,true),
        StructField("Vertido", StringType,true),
        StructField("GruaUsuario", StringType,true),
        StructField("GruaPropiaEMESA", StringType,true),
        StructField("GruaExterna", StringType,true),
        StructField("GruaMovilidad", StringType,true),
        StructField("FactoresClima", StringType,true),
        StructField("VisibilidadSenializacion", StringType,true),
        StructField("SenializacionPeligro", StringType,true),
        StructField("VisibilidadRestringida", StringType,true),
        StructField("Aceras", StringType,true),
        StructField("Arboles", StringType,true),
        StructField("VehiculosDenominacion", StringType,true),
        StructField("DescripcionAccidente", StringType,true),
        StructField("DaniosViario", StringType,true),
        StructField("Materiales", StringType,true),
        StructField("Observaciones", StringType,true)))

      if(year.equals("2016")) {
        schema = StructType(Array(
          StructField("Fecha", StringType,true),
          StructField("Hora", StringType,true),
          StructField("Dia", StringType,true),
          StructField("Incidencia", StringType,true),
          StructField("sitrem", StringType,true),
          StructField("Carretera", StringType,true),
          StructField("MC30", StringType,true),
          StructField("Calzada", StringType,true),
          StructField("Localizacion", StringType,true),
          StructField("Enlace", StringType,true),
          StructField("TipoAccidente", StringType,true),
          StructField("TipoColision", StringType,true),
          StructField("NVehiculos", StringType,true),
          StructField("NTurismos", StringType,true),
          StructField("NMotocicletas", StringType,true),
          StructField("NVehiculosPesados", StringType,true),
          StructField("HLbis", StringType,true),
          StructField("HL", StringType,true),
          StructField("HG", StringType,true),
          StructField("VM", StringType,true),
          StructField("DaniosMobiliario", StringType,true),
          StructField("Circulacion", StringType,true),
          StructField("EstadoFirme", StringType,true),
          StructField("PresenciaRRExt", StringType,true),
          StructField("CorteCarril", StringType,true),
          StructField("Vertido", StringType,true),
          StructField("GruaUsuario", StringType,true),
          StructField("GruaPropiaEMESA", StringType,true),
          StructField("GruaMovilidad", StringType,true),
          StructField("FactoresClima", StringType,true),
          StructField("VisibilidadSenializacion", StringType,true),
          StructField("SenializacionPeligro", StringType,true),
          StructField("VisibilidadRestringida", StringType,true),
          StructField("Aceras", StringType,true),
          StructField("Arboles", StringType,true),
          StructField("VehiculosDenominacion", StringType,true),
          StructField("DescripcionAccidente", StringType,true),
          StructField("DaniosViario", StringType,true),
          StructField("Materiales", StringType,true),
          StructField("Observaciones", StringType,true)))
      }

      val incidenciaDF = sqlContext.read.format("com.databricks.spark.csv").
        option("header", "true").
        option("delimiter",";").
        schema(schema).
        load(file)

      println("incidenciaDF SCHEMA: ")
      incidenciaDF.printSchema()
      println("END incidenciaDF SCHEMA")
      println("incidenciaDF FIRST:" + incidenciaDF.first())
      incidenciaDF.show()

      println("COUNT: " + incidenciaDF.count())

      if (incidenciaDF.count() > 0) {
        val pathToSave = hdfsUrl + hdfsIncidenciasHistoricoPath + hdfsToSave + partition + "/"
        incidenciaDF.write.mode(SaveMode.Overwrite).save(pathToSave)
        println("Guardando en HDFS " + incidenciaDF + " en " + pathToSave)

        incidenciaDF.registerTempTable("tableAux")
        val allRows = sqlContext.sql("SELECT " +
          "`Incidencia` AS `Incidencia`, " +
          "`Fecha` AS `Fecha`, " +
          "`Hora` AS `Hora`, " +
          "`Calzada` AS `Calzada`, " +
          "`Enlace` AS `Enlace`, " +
          "`TipoAccidente` AS `Tipo`, " +
          "IF(`TipoColision` = '', 'NR', `TipoColision`) AS Colision, " +
          "IF(`NVehiculos` = '', 0, `NVehiculos`) AS NVehiculos, " +
          "IF(`NTurismos` = '', 0, `NTurismos`) AS NTurismos, " +
          "IF(`NMotocicletas` = '', 0, `NMotocicletas`) AS NMotocicletas, " +
          "IF(`NVehiculosPesados` = '', 0, `NVehiculosPesados`) AS NVehiculosPesados, " +
          "IF(`HL` = '', 0, `HL`) AS HL, " +
          "IF(`HG` = '', 0, `HG`) AS HG, " +
          "IF(`VM` = '', 0, `VM`) AS VM, " +
          "IF(`DaniosMobiliario` = '', 'NR', `DaniosMobiliario`) AS DaniosMobiliario, " +
          "`Circulacion` AS `Circulacion`, " +
          "`EstadoFirme` AS `EstadoFirme`, " +
          "IF(`CorteCarril` = '', 'NR', `CorteCarril`) AS CorteCarril, " +
          "`DescripcionAccidente` AS `Descripcion` " +
          "FROM tableAux")


        if (allRows != null && allRows.count() > 0) {
          println("Imprimo")
          allRows.show()
          //SI YA HAY INCINDENCIAS CON ESTA FECHA ES QUE YA SE HABIA PROCESADO, SE BORRA TODA LA INFORMACION
          // EXISTENTE DE ESTE AÑO Y SE VUELVE A CARGAR POR SI FUESE UNA ACTUALIZACION
          val year = partition.replace("/","")
          val yearrexp = ".*" + year + ".*"
          println("YEAR TO QUERY : " + year)
          val query = MongoDBObject("fechaVI" ->  yearrexp.r)
          val mongoClient = MongoClient(MongoHost, MongoPort)
          val mongoConn = mongoClient(Database)(Collection)
          val result = mongoConn.find(query)
          if (result.size != 0) {
            println("ELIMINO TODOS LOS REGISTOS CON FECHA " + year)
            mongoConn.remove(query)
            println("ELIMINADOS")
          }

            allRows.foreachPartition{ row =>

              val mongoClient = MongoClient(MongoHost, MongoPort)
              val gson = new Gson()
              row.toSeq.foreach { col =>

                var incidenciaPO: IncidenciaHistoricoAnual = null

                val incidencia = col.get(0).toString
                val fecha = col.get(1).toString
                val hora = col.get(2).toString
                val calzada = col.get(3).toString
                val enlace = col.get(4).toString
                val tipo = col.get(5).toString
                val colision = col.get(6).toString
                val vehiculosImplicados = col.get(7).toString
                val turismos = col.get(8).toString
                val motocicletas = col.get(9).toString
                val vehiculosPesados = col.get(10).toString
                val hl = col.get(11).toString
                val hg = col.get(12).toString
                val vm = col.get(13).toString
                val mobiliario = col.get(14).toString
                val circulacion = col.get(15).toString
                val firme = col.get(16).toString
                val corte = col.get(17).toString
                val descripcion = col.get(18).toString

                incidenciaPO = new IncidenciaHistoricoAnual(incidencia,fecha,hora,calzada,enlace,tipo,colision,vehiculosImplicados,turismos,motocicletas,vehiculosPesados,hl,hg,vm,mobiliario,circulacion,firme,corte,descripcion);
                println("INCIDENCIA: ")
                incidenciaPO.printIncidencia

                val jsonIncidencia = gson.toJson(incidenciaPO)
                val mongoConn = mongoClient(Database)(Collection)
                val dBObject: DBObject = JSON.parse(jsonIncidencia).asInstanceOf[DBObject]
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
    files.collect.foreach(filename => {
      processFile(filename)
    })
  }
}
