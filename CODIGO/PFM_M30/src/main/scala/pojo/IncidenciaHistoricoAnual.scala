package main.scala.pojo

/**
  *
  * @param incidencia
  * @param fecha
  * @param hora
  * @param calzada
  * @param enlace
  * @param tipo
  * @param colisión
  * @param numVeh
  * @param numTurismos
  * @param numMotos
  * @param vehPesados
  * @param hl
  * @param hg
  * @param vm
  * @param danioMobiliario
  * @param estadoCirculación
  * @param firme
  * @param corteCarril
  * @param desc
  */
class IncidenciaHistoricoAnual(incidencia : String,
                               fecha : String,
                               hora : String,
                               calzada : String,
                               enlace	: String,
                               tipo : String,
                               colisión	: String,
                               numVeh : String,
                               numTurismos : String,
                               numMotos: String,
                               vehPesados : String,
                               hl : String,
                               hg : String,
                               vm	: String,
                               danioMobiliario : String,
                               estadoCirculación : String,
                               firme : String,
                               corteCarril : String,
                               desc : String
          ) extends Serializable{

  var incidenciaVI = incidencia
  var fechaVI = fecha
  var horaVI = hora
  var calzadaVI = calzada
  var enlaceVI = enlace
  var tipoVI = tipo
  var colisiónVI = colisión
  var numVehVI = numVeh
  var numTurismosVI = numTurismos
  var numMotosVI = numMotos
  var vehPesadosVI = vehPesados
  var hlVI = hl
  var hgVI = hg
  var vmVI = vm
  var danioMobiliarioVI = danioMobiliario
  var estadoCirculaciónVI = estadoCirculación
  var firmeVI = firme
  var corteCarrilVI = corteCarril
  var descVI = desc

  def printIncidencia = println("id: " + incidenciaVI + " - fecha: " + fechaVI + " " + horaVI)
}