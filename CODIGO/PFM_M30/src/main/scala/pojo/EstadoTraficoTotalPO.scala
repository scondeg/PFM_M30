package main.scala.pojo

/**
  *
  * @param fechaEstado
  * @param usuarioCalle
  * @param distanciaMediaRecorrida
  * @param tiempoMediodeRecorrido
  * @param vehxKmRamales
  * @param vehXKmTotales
  * @param velocidadMedia
  */
class EstadoTraficoTotalPO(fechaEstado : String,
                           usuarioCalle : String,
                           distanciaMediaRecorrida : String,
                           tiempoMediodeRecorrido : String,
                           vehxKmRamales : String,
                           vehXKmTotales : String,
                           velocidadMedia : String
          ) extends Serializable{
  var fecha = fechaEstado
  var usuarios = usuarioCalle
  var distanciaMedia = distanciaMediaRecorrida
  var tiempoMedio = tiempoMediodeRecorrido
  var vehKmRamales = vehxKmRamales
  var vehKmTotales = vehXKmTotales
  var velMedia = velocidadMedia

  def printEstadoTraficoTotal = println("\nfecha : " + fecha + "\nusuarios :" + usuarios + "\ndistanciaMedia : " + distanciaMedia
                                      + "\ntiempoMedio : " + tiempoMedio + "\nvehKmRamales : " + vehKmRamales +
                                      "\nvehKmTotales : " + vehKmTotales + "\nvelMedia : " + velMedia)
}