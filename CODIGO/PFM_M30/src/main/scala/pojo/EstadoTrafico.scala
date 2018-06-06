package main.scala.pojo

/**
  *
  * @param id
  * @param latitudPos
  * @param longitudPos
  * @param color
  * @param fecha
  */
class EstadoTrafico (id : String,
                     latitudPos : String,
                     longitudPos : String,
                     color : String,
                     fecha : String
                    ) extends Serializable{
  var idEstadoTrafico = id
  var latitud = latitudPos
  var longitud = longitudPos
  var colorEstado = color
  var fechaEstado = fecha

  def printEstadoTrafico = println("id: " + idEstadoTrafico + " - color: " + colorEstado)
}